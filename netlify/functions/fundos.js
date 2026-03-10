// netlify/functions/fundos.js
const { fetchCadastro, fetchCotaMonth, classifyFund, toYYYYMM, buildCotaIndex, computeReturns } = require('./lib/cvm');

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Content-Type': 'application/json; charset=utf-8',
};

function monthsRange(baseDateStr, back) {
  const r = [];
  const base = new Date(baseDateStr + 'T12:00:00Z');
  for (let i = 0; i <= back; i++) {
    r.push(toYYYYMM(new Date(base.getFullYear(), base.getMonth() - i, 1)));
  }
  return r;
}

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: CORS, body: '' };

  let baseDate = (event.queryStringParameters || {}).baseDate || '';
  if (!/^\d{4}-\d{2}-\d{2}$/.test(baseDate)) {
    const d = new Date(); d.setDate(d.getDate() - 1);
    baseDate = d.toISOString().slice(0, 10);
  }

  try {
    // 1. Cadastro
    const cadRows = await fetchCadastro();
    const fundMap = {};
    for (const f of cadRows) {
      const cnpj = f.CNPJ_FUNDO;
      if (!cnpj || cnpj.length < 11) continue;

      const sit = (f.SIT || '').toUpperCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'');
      if (!sit.includes('FUNCIONAMENTO') && !sit.includes('ATIVO')) continue;

      const nome = (f.DENOM_SOCIAL || '').trim();
      const nUp  = nome.toUpperCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'');
      if (nUp.includes('MASTER') || nUp.includes('FI MASTER')) continue;

      const cat = classifyFund(f.TP_FUNDO, nome);
      if (!cat) continue;

      fundMap[cnpj] = { nome, tipo: f.TP_FUNDO, cat };
    }

    console.log(`Fundos ativos mapeados: ${Object.keys(fundMap).length}`);
    const activeCNPJs = new Set(Object.keys(fundMap));

    // 2. Cotas
    const months = monthsRange(baseDate, 25);
    const allRows = [];
    for (let i = 0; i < months.length; i += 5) {
      const batch = months.slice(i, i + 5);
      const res = await Promise.allSettled(batch.map(ym => fetchCotaMonth(ym)));
      for (const r of res) if (r.status === 'fulfilled') allRows.push(...r.value);
    }
    console.log(`Total linhas de cotas: ${allRows.length}`);

    // 3. Normaliza CNPJ — diário usa CNPJ_FUNDO_CLASSE
    const normCNPJ = r => (r.CNPJ_FUNDO_CLASSE || r.CNPJ_FUNDO || r.CNPJ || '').replace(/[.\-\/]/g,'').trim();

    const norm = allRows
      .map(r => ({ ...r, _cnpj: normCNPJ(r) }))
      .filter(r => activeCNPJs.has(r._cnpj));

    // Rebuild index com campo normalizado
    const normRows = norm.map(r => ({ ...r, CNPJ_FUNDO: r._cnpj }));
    const cotaIdx = buildCotaIndex(normRows);
    console.log(`Fundos com cotas: ${Object.keys(cotaIdx).length}`);

    // 4. Calcula retornos
    const funds = [];
    for (const [cnpj, meta] of Object.entries(fundMap)) {
      const entries = cotaIdx[cnpj];
      if (!entries || entries.length < 5) continue;
      const rets = computeReturns(entries, baseDate);
      if (!rets || !rets.pl || rets.pl < 1_000_000) continue;
      funds.push({ cnpj, nome: meta.nome, cat: meta.cat, tipo: meta.tipo, ...rets });
    }

    funds.sort((a, b) => (b.pl || 0) - (a.pl || 0));
    console.log(`Fundos no resultado: ${funds.length}`);

    return {
      statusCode: 200,
      headers: { ...CORS, 'Cache-Control': 'public, max-age=3600' },
      body: JSON.stringify({ baseDate, generatedAt: new Date().toISOString(), count: funds.length, funds }),
    };
  } catch (err) {
    console.error(err);
    return { statusCode: 502, headers: CORS, body: JSON.stringify({ error: err.message }) };
  }
};
