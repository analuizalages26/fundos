// netlify/functions/lib/cvm.js
// Shared utilities: fetch CSV from CVM, parse, classify, compute returns

const https = require('https');
const http  = require('http');

const CVM_BASE = 'https://dados.cvm.gov.br/dados/FI';

// ─── HTTP fetch returning latin-1 decoded string ──────────────────────────────
function fetchText(url, redirects = 0) {
  return new Promise((resolve, reject) => {
    if (redirects > 5) return reject(new Error('Too many redirects'));
    const client = url.startsWith('https') ? https : http;
    const req = client.get(url, {
      headers: { 'User-Agent': 'FundosBR/2.0', 'Accept-Encoding': 'identity' },
      timeout: 45000,
    }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return fetchText(res.headers.location, redirects + 1).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) return reject(new Error(`HTTP ${res.statusCode} → ${url}`));
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        // CVM files are latin-1; decode properly
        const buf = Buffer.concat(chunks);
        try {
          // Node native: try UTF-8 first, fall back to latin1
          const text = buf.toString('latin1');
          resolve(text);
        } catch(e) { resolve(buf.toString('utf8')); }
      });
      res.on('error', reject);
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout: ' + url)); });
  });
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

// ─── Fund category classification ────────────────────────────────────────────
function classifyFund(tipo, nome) {
  const t = (tipo || '').toUpperCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'');
  const n = (nome || '').toUpperCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'');

  // Ações
  if (/(ACOES|AÇÕES|FIA\b|FICFIA|LONG.?BIASED|LONG.?ONLY|SMALL.?CAP|VALOR|DIVIDENDO)/.test(t + ' ' + n)) return 'acoes';

  // Crédito Privado / Renda Fixa
  if (/(CREDITO.?PRIVADO|RENDA.?FIXA|DEBENTURE|CRI|CRA|FIAGRO|CREDIT|HIGH.?YIELD|INFLACAO|IPCA|CDI|RF\b|FIF\b)/.test(t + ' ' + n)) return 'credito';

  // Multimercado
  if (/(MULTIMERCADO|FIM\b|FICFIM|MACRO|QUANT|ARBITRAGEM|LONG.?SHORT|TREND|SYSTEMATIC|HEDGE)/.test(t + ' ' + n)) return 'multi';

  return null;
}

// ─── YYYYMM helpers ──────────────────────────────────────────────────────────
function toYYYYMM(date) {
  return `${date.getFullYear()}${String(date.getMonth()+1).padStart(2,'0')}`;
}

function subtractMonths(dateStr, months) {
  // dateStr: YYYY-MM-DD
  const d = new Date(dateStr + 'T12:00:00Z');
  d.setMonth(d.getMonth() - months);
  return d;
}

// ─── Build cota index: cnpj → sorted array of {dt, quota, pl, cotistas} ──────
function buildCotaIndex(rows) {
  const idx = {};
  for (const r of rows) {
    const cnpj  = r.CNPJ_FUNDO || r.CNPJ || '';
    const dt    = r.DT_COMPTC  || r.DT_REF || '';
    const quota = parseFloat((r.VL_QUOTA      || '0').replace(',','.'));
    const pl    = parseFloat((r.VL_PATRIM_LIQ || '0').replace(',','.'));
    const cot   = parseInt(r.NR_COTST || '0', 10);
    if (!cnpj || !dt || !quota) continue;
    if (!idx[cnpj]) idx[cnpj] = [];
    idx[cnpj].push({ dt, quota, pl, cotistas: cot });
  }
  // sort ascending by date
  for (const k of Object.keys(idx)) idx[k].sort((a,b) => a.dt < b.dt ? -1 : 1);
  return idx;
}

// ─── Find closest entry on or before a target date string ────────────────────
function closestBefore(entries, targetDt) {
  let best = null;
  for (const e of entries) {
    if (e.dt <= targetDt) best = e;
    else break;
  }
  return best;
}

// ─── Compute returns for a fund given a base date ────────────────────────────
function computeReturns(entries, baseDt) {
  const base = closestBefore(entries, baseDt);
  if (!base) return null;

  const addMonths = (dt, m) => {
    const d = new Date(dt + 'T12:00:00Z');
    d.setMonth(d.getMonth() - m);
    return d.toISOString().slice(0,10);
  };

  const ret = (from, to) => {
    if (!from || !to || !from.quota || !to.quota) return null;
    return (to.quota / from.quota - 1) * 100;
  };

  // MTD: first trading day of base month
  const baseYear  = baseDt.slice(0,4);
  const baseMonth = baseDt.slice(5,7);
  const startOfMonth = `${baseYear}-${baseMonth}-01`;
  const mtdFrom = closestBefore(entries, startOfMonth) ||
                  entries.find(e => e.dt >= startOfMonth) || entries[0];

  // YTD: first trading day of base year
  const startOfYear = `${baseYear}-01-01`;
  const ytdFrom = closestBefore(entries, startOfYear) ||
                  entries.find(e => e.dt >= startOfYear) || entries[0];

  const from12m = closestBefore(entries, addMonths(baseDt, 12)) || entries[0];
  const from24m = closestBefore(entries, addMonths(baseDt, 24)) || entries[0];

  return {
    retMTD: ret(mtdFrom, base),
    retYTD: ret(ytdFrom, base),
    ret12m: ret(from12m, base),
    ret24m: ret(from24m, base),
    pl:     base.pl,
    quota:  base.quota,
    cotistas: base.cotistas,
    lastDt: base.dt,
  };
}

// ─── Fetch + parse one month of daily cotas ──────────────────────────────────
async function fetchCotaMonth(ym) {
  const url = `${CVM_BASE}/INF/DIARIO/DADOS/inf_diario_fi_${ym}.csv`;
  const text = await fetchText(url);
  return parseCSV(text);
}

// ─── Fetch cadastro ───────────────────────────────────────────────────────────
async function fetchCadastro() {
  const url = `${CVM_BASE}/CAD/DADOS/cad_fi.csv`;
  const text = await fetchText(url);
  return parseCSV(text);
}

module.exports = {
  fetchText, parseCSV, classifyFund,
  toYYYYMM, subtractMonths,
  buildCotaIndex, closestBefore, computeReturns,
  fetchCotaMonth, fetchCadastro,
  CVM_BASE,
};
