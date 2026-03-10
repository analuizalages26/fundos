// /api/debug2 — mostra amostra do cadastro + diário para checar match de CNPJ
const { fetchCadastro, fetchCotaMonth, toYYYYMM, classifyFund } = require('./lib/cvm');
const CORS = { 'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json; charset=utf-8' };

exports.handler = async () => {
  const out = {};

  // 1. Cadastro — primeiros 5 fundos ativos com categoria
  try {
    const rows = await fetchCadastro();
    out.cadastro_total = rows.length;

    const ativos = rows.filter(f => {
      const sit = (f.SIT||'').toUpperCase();
      return sit.includes('FUNCIONAMENTO') || sit.includes('ATIVO');
    });
    out.cadastro_ativos = ativos.length;

    const comCat = ativos.filter(f => classifyFund(f.TP_FUNDO, f.DENOM_SOCIAL));
    out.cadastro_com_categoria = comCat.length;

    // Mostra primeiros 3 como exemplo
    out.cadastro_exemplos = comCat.slice(0,3).map(f => ({
      cnpj_raw: f.CNPJ_FUNDO,
      cnpj_norm: (f.CNPJ_FUNDO||'').replace(/[.\-\/]/g,'').trim(),
      nome: f.DENOM_SOCIAL,
      tipo: f.TP_FUNDO,
      sit: f.SIT,
      cat: classifyFund(f.TP_FUNDO, f.DENOM_SOCIAL),
    }));
  } catch(e) { out.cadastro_error = e.message; }

  // 2. Diário deste mês — primeiros 5 registros
  try {
    const now = new Date();
    const ym = toYYYYMM(now);
    const rows = await fetchCotaMonth(ym);
    out.diario_total_rows = rows.length;

    // Mostra 3 exemplos
    out.diario_exemplos = rows.slice(0,3).map(r => ({
      cnpj_raw: r.CNPJ_FUNDO_CLASSE || r.CNPJ_FUNDO || r.CNPJ,
      cnpj_norm: (r.CNPJ_FUNDO_CLASSE || r.CNPJ_FUNDO || r.CNPJ || '').replace(/[.\-\/]/g,'').trim(),
      dt: r.DT_COMPTC,
      quota: r.VL_QUOTA,
      pl: r.VL_PATRIM_LIQ,
    }));

    // Testa se algum CNPJ do diário bate com o cadastro
    if (out.cadastro_exemplos && rows.length > 0) {
      const cadastroCNPJs = new Set(out.cadastro_exemplos.map(e => e.cnpj_norm));
      const diarioCNPJs = new Set(rows.slice(0,1000).map(r =>
        (r.CNPJ_FUNDO_CLASSE || r.CNPJ_FUNDO || r.CNPJ || '').replace(/[.\-\/]/g,'').trim()
      ));
      const matches = [...cadastroCNPJs].filter(c => diarioCNPJs.has(c));
      out.match_test = { cadastro_sample: [...cadastroCNPJs], matches };
    }
  } catch(e) { out.diario_error = e.message; }

  return { statusCode: 200, headers: CORS, body: JSON.stringify(out, null, 2) };
};
