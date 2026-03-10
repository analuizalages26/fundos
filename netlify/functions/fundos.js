// netlify/functions/fundos.js
// GET /api/fundos?baseDate=YYYY-MM-DD
// Returns processed fund list with returns relative to baseDate.
// Fetches enough months of daily cotas to cover 24m lookback.

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

// How many months back we need to cover 24m of history
// We fetch from (baseDate - 25 months) to (baseDate month) = ~26 month windows
// But that's huge. Strategy: fetch baseDate month + 12 prior months for 12m/YTD/MTD.
// For 24m we fetch 25 prior months. We batch in parallel.
const MONTHS_BACK = 25;

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

  // Determine base date
  let baseDate = (event.queryStringParameters || {}).baseDate || '';
  if (!baseDate || !/^\d{4}-\d{2}-\d{2}$/.test(baseDate)) {
    // default: yesterday (CVM lags 1-2 days)
    const d = new Date();
    d.setDate(d.getDate() - 1);
    baseDate = d.toISOString().slice(0, 10);
  }

  try {
    // 1. Cadastro
    const cadRows = await fetchCadastro();

    // Build active fund map: cnpj → {nome, tipo, cat}
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

    // 2. Fetch monthly cota files in parallel (chunked to avoid timeout)
    const months = getMonthsRange(baseDate, MONTHS_BACK);

    // Fetch in batches of 6 concurrent
    const BATCH = 6;
    const allRows = [];
    for (let i = 0; i < months.length; i += BATCH) {
      const batch = months.slice(i, i + BATCH);
      const results = await Promise.allSettled(batch.map(ym => fetchCotaMonth(ym)));
      for (const r of results) {
        if (r.status === 'fulfilled') allRows.push(...r.value);
      }
    }

    // 3. Build index only for active funds
    const filtered = allRows.filter(r => activeCNPJs.has(r.CNPJ_FUNDO || r.CNPJ || ''));
    const cotaIdx = buildCotaIndex(filtered);

    // 4. Compute returns per fund
    const funds = [];
    for (const [cnpj, meta] of Object.entries(fundMap)) {
      const entries = cotaIdx[cnpj];
      if (!entries || entries.length < 5) continue;

      const rets = computeReturns(entries, baseDate);
      if (!rets) continue;
      if (!rets.pl || rets.pl < 1_000_000) continue; // skip micro funds

      funds.push({
        cnpj,
        nome: meta.nome,
        cat:  meta.cat,
        tipo: meta.tipo,
        ...rets,
      });
    }

    // Sort by PL desc
    funds.sort((a, b) => (b.pl || 0) - (a.pl || 0));

    return {
      statusCode: 200,
      headers: {
        ...CORS,
        'Cache-Control': 'public, max-age=3600, stale-while-revalidate=86400',
      },
      body: JSON.stringify({
        baseDate,
        generatedAt: new Date().toISOString(),
        count: funds.length,
        funds,
      }),
    };

  } catch (err) {
    console.error('fundos error:', err);
    return {
      statusCode: 502,
      headers: CORS,
      body: JSON.stringify({ error: err.message }),
    };
  }
};
