// netlify/functions/fundos.js
// GET /api/fundos?baseDate=YYYY-MM-DD

const {
  fetchCadastro, fetchCotaMonth,
  classifyFund, toYYYYMM,
  buildCotaIndex, computeReturns,
} = require('./lib/cvm');

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Content-Type': 'application/json; charset=utf-8',
};

const MONTHS_BACK = 25;
const BATCH = 5;

function getMonthsRange(baseDateStr, monthsBack) {
  const result = [];
  const base = new Date(baseDateStr + 'T12:00:00Z');
  for (let i = 0; i <= monthsBack; i++) {
    const d = new Date(base.getFullYear(), base.getMonth() - i, 1);
    result.push(toYYYYMM(d));
  }
  return result;
}

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: CORS, body: '' };

  let baseDate = (event.queryStringParameters || {}).baseDate || '';
  if (!baseDate || !/^\d{4}-\d{2}-\d{2}$/.test(baseDate)) {
    const d = new Date();
    d.setDate(d.getDate() - 1);
    baseDate = d.toISOString().slice(0, 10);
  }

  try {
    // 1. Cadastro
    const cadRows = await fetchCadastro();

    // Build active fund map: cnpj -> {nome, tipo, cat}
    const fundMap = {};
    for (const f of cadRows) {
      const cnpj = (f.CNPJ_FUNDO || '').trim().replace(/[.\-\/]/g, '');
      const nome = (f.DENOM_SOCIAL || '').trim();
      const tipo = (f.TP_FUNDO || '').trim();
      const sit  = (f.SIT || '').toUpperCase();

      if (!cnpj || cnpj.length < 11) continue;
      if (!sit.includes('EM FUNCIONAMENTO') && !sit.includes('ATIVO')) continue;

      const nomeUp = nome.toUpperCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'');
      if (nomeUp.includes('MASTER') || nomeUp.includes('FI MASTER')) continue;

      const cat = classifyFund(tipo, nome);
      if (!cat) continue;

      fundMap[cnpj] = { nome, tipo, cat };
    }

    console.log(`Active funds in map: ${Object.keys(fundMap).length}`);

    const activeCNPJs = new Set(Object.keys(fundMap));

    // 2. Fetch cota months
    const months = getMonthsRange(baseDate, MONTHS_BACK);
    const allRows = [];

    for (let i = 0; i < months.length; i += BATCH) {
      const batch = months.slice(i, i + BATCH);
      const results = await Promise.allSettled(batch.map(ym => fetchCotaMonth(ym)));
      for (const r of results) {
        if (r.status === 'fulfilled') allRows.push(...r.value);
        else console.warn('Month fetch failed:', r.reason?.message);
      }
    }

    console.log(`Total cota rows: ${allRows.length}`);

    // 3. Build index — CNPJ in diario files is raw (no formatting)
    // Try to match both formatted and raw CNPJs
    const filtered = allRows.filter(r => {
      const c = (r.CNPJ_FUNDO || r.CNPJ || '').trim().replace(/[.\-\/]/g, '');
      return activeCNPJs.has(c);
    });

    // Rebuild index with normalised CNPJs
    const normalised = filtered.map(r => ({
      ...r,
      CNPJ_FUNDO: (r.CNPJ_FUNDO || r.CNPJ || '').trim().replace(/[.\-\/]/g, ''),
    }));

    const cotaIdx = buildCotaIndex(normalised);
    console.log(`Funds with cotas: ${Object.keys(cotaIdx).length}`);

    // 4. Compute returns
    const funds = [];
    for (const [cnpj, meta] of Object.entries(fundMap)) {
      const entries = cotaIdx[cnpj];
      if (!entries || entries.length < 5) continue;

      const rets = computeReturns(entries, baseDate);
      if (!rets || !rets.pl || rets.pl < 1_000_000) continue;

      funds.push({ cnpj, nome: meta.nome, cat: meta.cat, tipo: meta.tipo, ...rets });
    }

    funds.sort((a, b) => (b.pl || 0) - (a.pl || 0));

    return {
      statusCode: 200,
      headers: { ...CORS, 'Cache-Control': 'public, max-age=3600, stale-while-revalidate=86400' },
      body: JSON.stringify({ baseDate, generatedAt: new Date().toISOString(), count: funds.length, funds }),
    };

  } catch (err) {
    console.error('fundos error:', err);
    return { statusCode: 502, headers: CORS, body: JSON.stringify({ error: err.message }) };
  }
};
