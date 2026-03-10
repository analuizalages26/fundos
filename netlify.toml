// netlify/functions/lib/cvm.js
// Shared utilities: fetch CSV from CVM, parse, classify, compute returns

const https = require('https');
const http  = require('http');
const zlib  = require('zlib');

const CVM_BASE = 'https://dados.cvm.gov.br/dados/FI';

// ─── HTTP fetch → Buffer ──────────────────────────────────────────────────────
function fetchBuffer(url, redirects = 0) {
  return new Promise((resolve, reject) => {
    if (redirects > 5) return reject(new Error('Too many redirects'));
    const client = url.startsWith('https') ? https : http;
    const req = client.get(url, {
      headers: { 'User-Agent': 'FundosBR/2.0', 'Accept-Encoding': 'identity' },
      timeout: 45000,
    }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return fetchBuffer(res.headers.location, redirects + 1).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) return reject(new Error(`HTTP ${res.statusCode} -> ${url}`));
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve(Buffer.concat(chunks)));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout: ' + url)); });
  });
}

// ─── HTTP fetch → latin1 string ──────────────────────────────────────────────
async function fetchText(url) {
  const buf = await fetchBuffer(url);
  return buf.toString('latin1');
}

// ─── Parse semicolon CSV ──────────────────────────────────────────────────────
function parseCSV(text) {
  const lines = text.replace(/\r/g, '').trim().split('\n');
  if (lines.length < 2) return [];
  const headers = lines[0].split(';').map(h => h.trim().replace(/^"|"$/g, ''));
  const out = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split(';');
    if (cols.length < 2) continue;
    const obj = {};
    headers.forEach((h, j) => { obj[h] = (cols[j] || '').trim().replace(/^"|"$/g, ''); });
    out.push(obj);
  }
  return out;
}

// ─── Simple ZIP parser — extract CSV files ───────────────────────────────────
function unzipCSVs(buf) {
  const results = [];
  let offset = 0;
  while (offset < buf.length - 30) {
    if (buf[offset] === 0x50 && buf[offset+1] === 0x4b &&
        buf[offset+2] === 0x03 && buf[offset+3] === 0x04) {
      const compression    = buf.readUInt16LE(offset + 8);
      const compressedSize = buf.readUInt32LE(offset + 18);
      const fileNameLen    = buf.readUInt16LE(offset + 26);
      const extraLen       = buf.readUInt16LE(offset + 28);
      const fileName = buf.slice(offset + 30, offset + 30 + fileNameLen).toString();
      const dataStart = offset + 30 + fileNameLen + extraLen;
      if (fileName.toLowerCase().endsWith('.csv') && compressedSize > 0) {
        try {
          const compData = buf.slice(dataStart, dataStart + compressedSize);
          const content = compression === 0
            ? compData.toString('latin1')
            : zlib.inflateRawSync(compData).toString('latin1');
          results.push({ name: fileName, content });
        } catch(e) {}
      }
      offset = dataStart + compressedSize;
    } else {
      offset++;
    }
  }
  return results;
}

// ─── Fund category classification ────────────────────────────────────────────
function classifyFund(tipo, nome) {
  const s = ((tipo || '') + ' ' + (nome || '')).toUpperCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '');

  if (/(ACOES|FIA\b|FICFIA|LONG.?BIASED|LONG.?ONLY|SMALL.?CAP|DIVIDENDO|VALOR)/.test(s)) return 'acoes';
  if (/(CREDITO.?PRIV|RENDA.?FIXA|DEBENTURE|HIGH.?YIELD|INFLACAO|IPCA|RF\b|FIRF|FIC.?RF|CREDITO CORP)/.test(s)) return 'credito';
  if (/(MULTIMERCADO|FIM\b|FICFIM|MACRO|QUANT|ARBITRAGEM|LONG.?SHORT|TREND|HEDGE|SISTEMATIC)/.test(s)) return 'multi';

  return null;
}

// ─── YYYYMM helper ───────────────────────────────────────────────────────────
function toYYYYMM(date) {
  return `${date.getFullYear()}${String(date.getMonth()+1).padStart(2,'0')}`;
}

// ─── Build cota index: cnpj -> sorted [{dt, quota, pl, cotistas}] ─────────────
function buildCotaIndex(rows) {
  const idx = {};
  for (const r of rows) {
    const cnpj  = r.CNPJ_FUNDO || r.CNPJ || '';
    const dt    = r.DT_COMPTC  || r.DT_REF || '';
    const quota = parseFloat((r.VL_QUOTA      || '0').replace(',', '.'));
    const pl    = parseFloat((r.VL_PATRIM_LIQ || '0').replace(',', '.'));
    const cot   = parseInt(r.NR_COTST || '0', 10);
    if (!cnpj || !dt || !quota) continue;
    if (!idx[cnpj]) idx[cnpj] = [];
    idx[cnpj].push({ dt, quota, pl, cotistas: cot });
  }
  for (const k of Object.keys(idx)) idx[k].sort((a, b) => a.dt < b.dt ? -1 : 1);
  return idx;
}

// ─── Closest entry on or before targetDt ────────────────────────────────────
function closestBefore(entries, targetDt) {
  let best = null;
  for (const e of entries) {
    if (e.dt <= targetDt) best = e;
    else break;
  }
  return best;
}

// ─── Compute returns ─────────────────────────────────────────────────────────
function computeReturns(entries, baseDt) {
  const base = closestBefore(entries, baseDt);
  if (!base) return null;

  const shiftBack = (dt, months) => {
    const d = new Date(dt + 'T12:00:00Z');
    d.setMonth(d.getMonth() - months);
    return d.toISOString().slice(0, 10);
  };

  const ret = (from, to) => {
    if (!from || !to || !from.quota || !to.quota) return null;
    return (to.quota / from.quota - 1) * 100;
  };

  const y = baseDt.slice(0,4), m = baseDt.slice(5,7);
  const mtdFrom = closestBefore(entries, `${y}-${m}-01`) || entries.find(e => e.dt >= `${y}-${m}-01`) || entries[0];
  const ytdFrom = closestBefore(entries, `${y}-01-01`)   || entries.find(e => e.dt >= `${y}-01-01`)   || entries[0];
  const from12m = closestBefore(entries, shiftBack(baseDt, 12)) || entries[0];
  const from24m = closestBefore(entries, shiftBack(baseDt, 24)) || entries[0];

  return {
    retMTD: ret(mtdFrom, base),
    retYTD: ret(ytdFrom, base),
    ret12m: ret(from12m, base),
    ret24m: ret(from24m, base),
    pl: base.pl,
    quota: base.quota,
    cotistas: base.cotistas,
    lastDt: base.dt,
  };
}

// ─── Fetch one month of daily cotas ──────────────────────────────────────────
async function fetchCotaMonth(ym) {
  // CVM now distributes diario as zip files
  const urlZip = `${CVM_BASE}/INF/DIARIO/DADOS/inf_diario_fi_${ym}.zip`;
  const urlCsv = `${CVM_BASE}/INF/DIARIO/DADOS/inf_diario_fi_${ym}.csv`;

  try {
    const buf = await fetchBuffer(urlZip);
    const csvs = unzipCSVs(buf);
    if (csvs.length > 0) return parseCSV(csvs[0].content);
  } catch(e) {}

  // Fallback to plain CSV
  const text = await fetchText(urlCsv);
  return parseCSV(text);
}

// ─── Fetch cadastro — merges old cad_fi.csv + new registro_fundo_classe.zip ──
async function fetchCadastro() {
  const allRows = [];

  // 1. Old-format funds (not yet adapted to RCVM175)
  try {
    const text = await fetchText(`${CVM_BASE}/CAD/DADOS/cad_fi.csv`);
    const rows = parseCSV(text);
    // normalise to common field names
    allRows.push(...rows.map(r => ({
      CNPJ_FUNDO: r.CNPJ_FUNDO || r.CNPJ || '',
      DENOM_SOCIAL: r.DENOM_SOCIAL || '',
      TP_FUNDO: r.TP_FUNDO || '',
      SIT: r.SIT || '',
    })));
    console.log(`cad_fi.csv: ${rows.length} rows`);
  } catch(e) { console.warn('cad_fi.csv failed:', e.message); }

  // 2. New-format funds (RCVM175) — ZIP contains registro_fundo.csv + registro_classe.csv
  try {
    const buf = await fetchBuffer(`${CVM_BASE}/CAD/DADOS/registro_fundo_classe.zip`);
    const csvFiles = unzipCSVs(buf);
    for (const f of csvFiles) {
      const isClasse = f.name.toLowerCase().includes('classe');
      const isFundo  = f.name.toLowerCase().includes('fundo') && !isClasse;
      if (!isClasse && !isFundo) continue;
      const rows = parseCSV(f.content);
      console.log(`${f.name}: ${rows.length} rows`);
      for (const r of rows) {
        // registro_classe columns: CNPJ_FUNDO_CLASSE, Denominacao_Social, Tipo_Classe, Situacao
        // registro_fundo columns:  CNPJ_Fundo, Denominacao_Social, Situacao
        allRows.push({
          CNPJ_FUNDO: r.CNPJ_FUNDO_CLASSE || r.CNPJ_Fundo || r.CNPJ_CLASSE || '',
          DENOM_SOCIAL: r.Denominacao_Social || r.DENOM_SOCIAL || '',
          TP_FUNDO: r.Tipo_Classe || r.TP_FUNDO_CLASSE || r.TP_FUNDO || '',
          SIT: r.Situacao || r.SIT || '',
        });
      }
    }
  } catch(e) { console.warn('registro_fundo_classe.zip failed:', e.message); }

  console.log(`Total cadastro rows: ${allRows.length}`);
  return allRows;
}

module.exports = {
  fetchText, fetchBuffer, parseCSV, classifyFund,
  toYYYYMM, buildCotaIndex, closestBefore, computeReturns,
  fetchCotaMonth, fetchCadastro,
  CVM_BASE,
};
