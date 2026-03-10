// netlify/functions/daily-cache.js
// Scheduled function — runs every day at 07:00 BRT (10:00 UTC)
// Pre-fetches CVM data for "yesterday" and stores in Netlify Blobs
// so the main /api/fundos endpoint can serve instantly from cache.
//
// Schedule: "0 10 * * *"  (cron UTC)

const { schedule } = require('@netlify/functions');
const { getStore }  = require('@netlify/blobs');
const {
  fetchCadastro, fetchCotaMonth,
  classifyFund, toYYYYMM,
  buildCotaIndex, computeReturns,
} = require('./lib/cvm');

const MONTHS_BACK = 25;
const BATCH = 6;

function getMonthsRange(baseDateStr, monthsBack) {
  const result = [];
  const base = new Date(baseDateStr + 'T12:00:00Z');
  for (let i = 0; i <= monthsBack; i++) {
    const d = new Date(base.getFullYear(), base.getMonth() - i, 1);
    result.push(toYYYYMM(d));
  }
  return result;
}

async function buildFunds(baseDate) {
  const cadRows = await fetchCadastro();
  const fundMap = {};

  for (const f of cadRows) {
    const sit  = (f.SIT || f.SITUACAO || '').toUpperCase();
    const nome = (f.DENOM_SOCIAL || f.NOME_FUNDO || '').trim();
    const tipo = (f.TP_FUNDO || f.CLASSE || '').trim();
    const cnpj = (f.CNPJ_FUNDO || f.CNPJ || '').trim();
    if (!cnpj) continue;
    if (!sit.includes('EM FUNCIONAMENTO') && !sit.includes('ATIVO')) continue;
    const nomeUp = nome.toUpperCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'');
    if (nomeUp.includes('MASTER') || nomeUp.includes('FI MASTER')) continue;
    const cat = classifyFund(tipo, nome);
    if (!cat) continue;
    fundMap[cnpj] = { nome, tipo, cat };
  }

  const activeCNPJs = new Set(Object.keys(fundMap));
  const months = getMonthsRange(baseDate, MONTHS_BACK);
  const allRows = [];

  for (let i = 0; i < months.length; i += BATCH) {
    const batch = months.slice(i, i + BATCH);
    const results = await Promise.allSettled(batch.map(ym => fetchCotaMonth(ym)));
    for (const r of results) {
      if (r.status === 'fulfilled') allRows.push(...r.value);
    }
  }

  const filtered = allRows.filter(r => activeCNPJs.has(r.CNPJ_FUNDO || r.CNPJ || ''));
  const cotaIdx  = buildCotaIndex(filtered);

  const funds = [];
  for (const [cnpj, meta] of Object.entries(fundMap)) {
    const entries = cotaIdx[cnpj];
    if (!entries || entries.length < 5) continue;
    const rets = computeReturns(entries, baseDate);
    if (!rets || !rets.pl || rets.pl < 1_000_000) continue;
    funds.push({ cnpj, nome: meta.nome, cat: meta.cat, tipo: meta.tipo, ...rets });
  }

  funds.sort((a, b) => (b.pl || 0) - (a.pl || 0));
  return funds;
}

const handler = async () => {
  try {
    // Yesterday in Brazil (UTC-3)
    const now = new Date();
    now.setHours(now.getHours() - 3);
    now.setDate(now.getDate() - 1);
    const baseDate = now.toISOString().slice(0, 10);

    console.log(`[daily-cache] Building cache for baseDate=${baseDate}`);
    const funds = await buildFunds(baseDate);

    const payload = JSON.stringify({
      baseDate,
      generatedAt: new Date().toISOString(),
      count: funds.length,
      funds,
    });

    // Save to Netlify Blobs
    const store = getStore('fundos-cache');
    await store.set('latest', payload, { metadata: { baseDate } });
    await store.set(`snapshot-${baseDate}`, payload, { metadata: { baseDate } });

    console.log(`[daily-cache] Done. ${funds.length} funds saved for ${baseDate}`);
    return { statusCode: 200 };
  } catch (err) {
    console.error('[daily-cache] Error:', err);
    return { statusCode: 500 };
  }
};

// Netlify scheduled function: 10:00 UTC = 07:00 BRT
exports.handler = schedule('0 10 * * *', handler);
