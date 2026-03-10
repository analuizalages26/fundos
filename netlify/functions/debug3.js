// /api/debug3 — cruza CNPJs do diário com cadastro
const { fetchCadastro, fetchCotaMonth, toYYYYMM, classifyFund } = require('./lib/cvm');
const CORS = { 'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json; charset=utf-8' };

const norm = s => (s||'').replace(/[.\-\/]/g,'').trim().padStart(14,'0');

exports.handler = async () => {
  const out = {};

  // Pega CNPJs do diário (amostra de 500)
  const now = new Date();
  const ym = toYYYYMM(now);
  const diarioRows = await fetchCotaMonth(ym);
  const diarioCNPJs = new Set(diarioRows.slice(0,500).map(r =>
    norm(r.CNPJ_FUNDO_CLASSE || r.CNPJ_FUNDO || r.CNPJ)
  ));
  out.diario_cnpjs_sample = [...diarioCNPJs].slice(0,5);
  out.diario_total = diarioRows.length;

  // Pega cadastro
  const cadRows = await fetchCadastro();
  out.cadastro_total = cadRows.length;

  // Verifica formatos
  const cadFormatted   = cadRows.filter(r => (r.CNPJ_FUNDO||'').includes('.')).length;
  const cadUnformatted = cadRows.filter(r => !(r.CNPJ_FUNDO||'').includes('.') && (r.CNPJ_FUNDO||'').length > 0).length;
  out.cadastro_cnpj_com_ponto = cadFormatted;
  out.cadastro_cnpj_sem_ponto = cadUnformatted;

  // Amostra de CNPJs do cadastro normalizados
  const cadNormed = cadRows.map(r => norm(r.CNPJ_FUNDO));
  out.cadastro_normed_sample = cadNormed.slice(0,5);

  // Match
  const matches = cadNormed.filter(c => diarioCNPJs.has(c));
  out.matches_count = matches.length;
  out.matches_sample = matches.slice(0,5);

  // Fundo especifico do diário no cadastro?
  const testCNPJ = '00017024000153';
  out.test_cnpj_in_cadastro = cadNormed.includes(testCNPJ);
  out.test_cnpj_raw_in_cadastro = cadRows.find(r => (r.CNPJ_FUNDO||'').includes('17024'));

  return { statusCode: 200, headers: CORS, body: JSON.stringify(out, null, 2) };
};
