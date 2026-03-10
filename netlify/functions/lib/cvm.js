const https = require('https');
const http  = require('http');
const zlib  = require('zlib');

const CVM_CAD_OLD  = 'https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv';
const CVM_CAD_ZIP  = 'https://dados.cvm.gov.br/dados/FI/CAD/DADOS/registro_fundo_classe.zip';
const CVM_DIARIO   = ym => `https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_${ym}.zip`;
const CVM_DIARIO_H = ym => `https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/HIST/inf_diario_fi_${ym}.zip`;

// ── HTTP ──────────────────────────────────────────────────────────────────────
function fetchBuffer(url, hops=0) {
  return new Promise((resolve, reject) => {
    if (hops>5) return reject(new Error('Too many redirects'));
    const cli = url.startsWith('https') ? https : http;
    cli.get(url, { headers:{'User-Agent':'FundosBR/4.0','Accept-Encoding':'identity'}, timeout:50000 }, res => {
      if (res.statusCode>=300 && res.statusCode<400 && res.headers.location)
        return fetchBuffer(res.headers.location, hops+1).then(resolve).catch(reject);
      if (res.statusCode!==200) return reject(new Error(`HTTP ${res.statusCode} ${url}`));
      const c=[]; res.on('data',d=>c.push(d)); res.on('end',()=>resolve(Buffer.concat(c))); res.on('error',reject);
    }).on('error',reject).on('timeout',function(){this.destroy();reject(new Error('Timeout'));});
  });
}
const fetchText = async url => (await fetchBuffer(url)).toString('latin1');

// ── CSV ───────────────────────────────────────────────────────────────────────
function parseCSV(text) {
  const lines = text.replace(/\r/g,'').trim().split('\n');
  if (lines.length<2) return [];
  const hdrs = lines[0].split(';').map(h=>h.trim().replace(/^"|"$/g,''));
  const out=[];
  for (let i=1;i<lines.length;i++) {
    const cols=lines[i].split(';');
    if (cols.length<2) continue;
    const o={};
    hdrs.forEach((h,j)=>{ o[h]=(cols[j]||'').trim().replace(/^"|"$/g,''); });
    out.push(o);
  }
  return out;
}

// ── ZIP ───────────────────────────────────────────────────────────────────────
function unzipCSVs(buf) {
  const res=[];
  let off=0;
  while (off<buf.length-30) {
    if (buf[off]===0x50&&buf[off+1]===0x4b&&buf[off+2]===0x03&&buf[off+3]===0x04) {
      const comp=buf.readUInt16LE(off+8), csz=buf.readUInt32LE(off+18);
      const fnl=buf.readUInt16LE(off+26), exl=buf.readUInt16LE(off+28);
      const fn=buf.slice(off+30,off+30+fnl).toString(), ds=off+30+fnl+exl;
      if (csz>0 && fn.toLowerCase().endsWith('.csv')) {
        try {
          const cd=buf.slice(ds,ds+csz);
          res.push({ name:fn, content: comp===0?cd.toString('latin1'):zlib.inflateRawSync(cd).toString('latin1') });
        } catch(e) {}
      }
      off=ds+csz;
    } else off++;
  }
  return res;
}

// ── CNPJ normalisation — strips formatting, always 14 digits ─────────────────
const normCNPJ = s => (s||'').replace(/[.\-\/\s]/g,'').padStart(14,'0');

// ── Classification ────────────────────────────────────────────────────────────
function classifyFund(tipo, nome) {
  const s = ((tipo||'')+' '+(nome||'')).toUpperCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'');
  if (/(FIA\b|FICFIA|LONG.?BIASED|LONG.?ONLY|SMALL.?CAP|DIVIDENDO|ACOES|VALOR ACOES)/.test(s)) return 'acoes';
  if (/(CREDITO.?PRIV|RENDA.?FIXA|DEBENTURE|HIGH.?YIELD|INFLACAO|IPCA|\bRF\b|FIRF|FIC.?RF)/.test(s)) return 'credito';
  if (/(MULTIMERCADO|\bFIM\b|FICFIM|MACRO|QUANT|ARBITRAGEM|LONG.?SHORT|TREND|HEDGE)/.test(s)) return 'multi';
  return null;
}

// ── Date helpers ──────────────────────────────────────────────────────────────
function toYYYYMM(d) { return `${d.getFullYear()}${String(d.getMonth()+1).padStart(2,'0')}`; }

// ── Cota index ────────────────────────────────────────────────────────────────
function buildCotaIndex(rows) {
  const idx={};
  for (const r of rows) {
    // diario uses CNPJ_FUNDO_CLASSE
    const cnpj = normCNPJ(r.CNPJ_FUNDO_CLASSE || r.CNPJ_FUNDO || r.CNPJ);
    const dt   = r.DT_COMPTC || r.DT_REF || '';
    const quota= parseFloat((r.VL_QUOTA||'0').replace(',','.'));
    const pl   = parseFloat((r.VL_PATRIM_LIQ||'0').replace(',','.'));
    const cot  = parseInt(r.NR_COTST||'0',10);
    if (!cnpj||!dt||!quota) continue;
    if (!idx[cnpj]) idx[cnpj]=[];
    idx[cnpj].push({dt,quota,pl,cotistas:cot});
  }
  for (const k of Object.keys(idx)) idx[k].sort((a,b)=>a.dt<b.dt?-1:1);
  return idx;
}

function closestBefore(entries, targetDt) {
  let best=null;
  for (const e of entries) { if (e.dt<=targetDt) best=e; else break; }
  return best;
}

function computeReturns(entries, baseDt) {
  const base = closestBefore(entries, baseDt);
  if (!base) return null;
  const back = (dt,m) => { const d=new Date(dt+'T12:00:00Z'); d.setMonth(d.getMonth()-m); return d.toISOString().slice(0,10); };
  const ret  = (a,b) => (!a||!b||!a.quota||!b.quota) ? null : (b.quota/a.quota-1)*100;
  const y=baseDt.slice(0,4), mo=baseDt.slice(5,7);
  const mtdF = closestBefore(entries,`${y}-${mo}-01`) || entries.find(e=>e.dt>=`${y}-${mo}-01`) || entries[0];
  const ytdF = closestBefore(entries,`${y}-01-01`)    || entries.find(e=>e.dt>=`${y}-01-01`)    || entries[0];
  const f12  = closestBefore(entries,back(baseDt,12)) || entries[0];
  const f24  = closestBefore(entries,back(baseDt,24)) || entries[0];
  return { retMTD:ret(mtdF,base), retYTD:ret(ytdF,base), ret12m:ret(f12,base), ret24m:ret(f24,base),
           pl:base.pl, quota:base.quota, cotistas:base.cotistas, lastDt:base.dt };
}

// ── Fetch cotas (ZIP, fallback HIST) ─────────────────────────────────────────
async function fetchCotaMonth(ym) {
  for (const url of [CVM_DIARIO(ym), CVM_DIARIO_H(ym)]) {
    try {
      const buf  = await fetchBuffer(url);
      const csvs = unzipCSVs(buf);
      if (csvs.length>0) { console.log(`OK ${ym}`); return parseCSV(csvs[0].content); }
    } catch(e) {}
  }
  console.warn(`No data for ${ym}`);
  return [];
}

// ── Cadastro: cad_fi.csv (old) + registro_classe.csv (RCVM175) ───────────────
async function fetchCadastro() {
  const all=[];

  // 1. Old format — cad_fi.csv
  // Columns: CNPJ_FUNDO (formatted "00.017.024/0001-53"), DENOM_SOCIAL, TP_FUNDO, SIT
  try {
    const rows = parseCSV(await fetchText(CVM_CAD_OLD));
    for (const r of rows) {
      all.push({
        CNPJ_FUNDO:   normCNPJ(r.CNPJ_FUNDO),
        DENOM_SOCIAL: r.DENOM_SOCIAL || '',
        TP_FUNDO:     r.TP_FUNDO || r.CLASSE || '',
        SIT:          r.SIT || '',
      });
    }
    console.log(`cad_fi: ${rows.length}`);
  } catch(e) { console.warn('cad_fi error:', e.message); }

  // 2. New RCVM175 — registro_classe.csv
  // Columns: CNPJ_Classe (unformatted, may be <14 digits), Denominacao_Social, Tipo_Classe, Situacao
  try {
    const buf  = await fetchBuffer(CVM_CAD_ZIP);
    const csvs = unzipCSVs(buf);
    for (const f of csvs) {
      if (!f.name.toLowerCase().includes('classe')) continue;
      const rows = parseCSV(f.content);
      for (const r of rows) {
        all.push({
          CNPJ_FUNDO:   normCNPJ(r.CNPJ_Classe),
          DENOM_SOCIAL: r.Denominacao_Social || '',
          TP_FUNDO:     r.Tipo_Classe || '',
          SIT:          r.Situacao || '',
        });
      }
      console.log(`registro_classe: ${rows.length}`);
    }
  } catch(e) { console.warn('registro zip error:', e.message); }

  console.log(`Cadastro total: ${all.length}`);
  return all;
}

module.exports = { fetchText, fetchBuffer, parseCSV, classifyFund, normCNPJ,
                   toYYYYMM, buildCotaIndex, closestBefore, computeReturns,
                   fetchCotaMonth, fetchCadastro };
