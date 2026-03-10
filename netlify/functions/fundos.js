const { fetchCadastro, fetchCotaMonth, classifyFund, normCNPJ,
        toYYYYMM, buildCotaIndex, computeReturns } = require('./lib/cvm');

const CORS = { 'Access-Control-Allow-Origin':'*', 'Content-Type':'application/json; charset=utf-8' };

function monthsRange(base, back) {
  const r=[], d=new Date(base+'T12:00:00Z');
  for (let i=0;i<=back;i++) r.push(toYYYYMM(new Date(d.getFullYear(),d.getMonth()-i,1)));
  return r;
}

exports.handler = async (event) => {
  if (event.httpMethod==='OPTIONS') return {statusCode:200,headers:CORS,body:''};

  let baseDate = (event.queryStringParameters||{}).baseDate||'';
  if (!/^\d{4}-\d{2}-\d{2}$/.test(baseDate)) {
    const d=new Date(); d.setDate(d.getDate()-1); baseDate=d.toISOString().slice(0,10);
  }

  try {
    // 1. Cadastro → fundMap keyed by normalised 14-digit CNPJ
    const cadRows = await fetchCadastro();
    const fundMap = {};
    for (const f of cadRows) {
      const cnpj = f.CNPJ_FUNDO;   // already normalised by fetchCadastro
      if (!cnpj || cnpj==='00000000000000') continue;

      const sit = (f.SIT||'').toUpperCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'');
      if (!sit.includes('FUNCIONAMENTO') && !sit.includes('ATIVO')) continue;

      const nome = (f.DENOM_SOCIAL||'').trim();
      if (!nome) continue;
      const nUp = nome.toUpperCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'');
      if (nUp.includes('MASTER')||nUp.includes('FI MASTER')) continue;

      const cat = classifyFund(f.TP_FUNDO, nome);
      if (!cat) continue;

      fundMap[cnpj] = { nome, tipo:f.TP_FUNDO, cat };
    }
    console.log(`fundMap: ${Object.keys(fundMap).length}`);

    // 2. Cotas — last 25 months in batches of 5
    const months = monthsRange(baseDate, 25);
    const allRows = [];
    for (let i=0;i<months.length;i+=5) {
      const res = await Promise.allSettled(months.slice(i,i+5).map(ym=>fetchCotaMonth(ym)));
      for (const r of res) if (r.status==='fulfilled') allRows.push(...r.value);
    }
    console.log(`Cota rows: ${allRows.length}`);

    // 3. Build index — normalise CNPJ_FUNDO_CLASSE from diario
    const cotaIdx = buildCotaIndex(allRows);
    console.log(`Funds with cotas: ${Object.keys(cotaIdx).length}`);

    // 4. Match & compute
    const funds=[];
    for (const [cnpj, meta] of Object.entries(fundMap)) {
      const entries = cotaIdx[cnpj];
      if (!entries||entries.length<5) continue;
      const rets = computeReturns(entries, baseDate);
      if (!rets||!rets.pl||rets.pl<1_000_000) continue;
      funds.push({cnpj, nome:meta.nome, cat:meta.cat, tipo:meta.tipo, ...rets});
    }
    funds.sort((a,b)=>(b.pl||0)-(a.pl||0));
    console.log(`Result: ${funds.length} funds`);

    return {
      statusCode:200,
      headers:{...CORS,'Cache-Control':'public, max-age=3600'},
      body:JSON.stringify({baseDate, generatedAt:new Date().toISOString(), count:funds.length, funds}),
    };
  } catch(err) {
    console.error(err);
    return {statusCode:502, headers:CORS, body:JSON.stringify({error:err.message})};
  }
};
