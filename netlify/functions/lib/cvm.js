// netlify/functions/lib/cvm.js
const https = require('https');
const http  = require('http');
const zlib  = require('zlib');

// ── URLs corretas da CVM (verificadas em março/2026) ─────────────────────────
const CVM_CAD_OLD  = 'https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv';
const CVM_CAD_ZIP  = 'https://dados.cvm.gov.br/dados/FI/CAD/DADOS/registro_fundo_classe.zip';
const CVM_DIARIO   = (ym) => `https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_${ym}.zip`;
const CVM_DIARIO_H = (ym) => `https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/HIST/inf_diario_fi_${ym}.zip`;

// ── HTTP → Buffer ─────────────────────────────────────────────────────────────
function fetchBuffer(url, redirects = 0) {
  return new Promise((resolve, reject) => {
    if (redirects > 5) return reject(new Error('Too many redirects'));
    const client = url.startsWith('https') ? https : http;
    const req = client.get(url, {
      headers: { 'User-Agent': 'FundosBR/3.0', 'Accept-Encoding': 'identity' },
      timeout: 50000,
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
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

async function fetchText(url) {
  const buf = await fetchBuffer(url);
  return buf.toString('latin1');
}

// ── Parse CSV com separador ; ─────────────────────────────────────────────────
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

// ── Extrai CSVs de um buffer ZIP ──────────────────────────────────────────────
function unzipCSVs(buf) {
  const results = [];
  let offset = 0;
  while (offset < buf.length - 30) {
    if (buf[offset]===0x50 && buf[offset+1]===0x4b &&
        buf[offset+2]===0x03 && buf[offset+3]===0x04) {
      const compression    = buf.readUInt16LE(offset + 8);
      const compressedSize = buf.readUInt32LE(offset + 18);
      const fileNameLen    = buf.readUInt16LE(offset + 26);
      const extraLen       = buf.readUInt16LE(offset + 28);
      const fileName = buf.slice(offset + 30, offset + 30 + fileNameLen).toString();
      const dataStart = offset + 30 + fileNameLen + extraLen;
      if (compressedSize > 0 && fileName.toLowerCase().endsWith('.csv')) {
        try {
          const cd = buf.slice(dataStart, dataStart + compressedSize);
          const content = compression === 0
            ? cd.toString('latin1')
            : zlib.inflateRawSync(cd).toString('latin1');
          results.push({ name: fileName, content });
        } catch(e) { console.warn('unzip error', fileName, e.message); }
      }
      offset = dataStart + compressedSize;
    } else { offset++; }
  }
  return results;
}

// ── Classificação de categoria ────────────────────────────────────────────────
function classifyFund(tipo, nome) {
  const s = ((tipo || '') + ' ' + (nome || '')).toUpperCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  if (/(FIA\b|FICFIA|LONG.?BIASED|LONG.?ONLY|SMALL.?CAP|DIVIDENDO|ACOES|VALOR ACOES)/.test(s)) return 'acoes';
  if (/(CREDITO.?PRIV|RENDA.?FIXA|DEBENTURE|HIGH.?YIELD|INFLACAO|IPCA|RF\b|FIRF|FIC.?RF)/.test(s)) return 'credito';
  if (/(MULTIMERCADO|FIM\b|FICFIM|MACRO|QUANT|ARBITRAGEM|LONG.?SHORT|TREND|HEDGE)/.test(s)) return 'multi';
  return null;
}

// ── YYYYMM ────────────────────────────────────────────────────────────────────
function toYYYYMM(date) {
  return `${date.getFullYear()}${String(date.getMonth()+1).padStart(2,'0')}`;
}

// ── Índice de cotas: cnpj → [{dt, quota, pl, cotistas}] ──────────────────────
function buildCotaIndex(rows) {
  const idx = {};
  for (const r of rows) {
    const cnpj  = (r.CNPJ_FUNDO || r.CNPJ || '').replace(/[.\-\/]/g, '').trim();
    const dt    = r.DT_COMPTC || r.DT_REF || '';
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

function closestBefore(entries, targetDt) {
  let best = null;
  for (const e of entries) {
    if (e.dt <= targetDt) best = e;
    else break;
  }
  return best;
}

function computeReturns(entries, baseDt) {
  const base = closestBefore(entries, baseDt);
  if (!base) return null;

  const shiftBack = (dt, months) => {
    const d = new Date(dt + 'T12:00:00Z');
    d.setMonth(d.getMonth() - months);
    return d.toISOString().slice(0, 10);
  };
  const ret = (from, to) => (!from || !to || !from.quota || !to.quota)
    ? null : (to.quota / from.quota - 1) * 100;

  const y = baseDt.slice(0,4), m = baseDt.slice(5,7);
  const mtdFrom = closestBefore(entries, `${y}-${m}-01`) || entries.find(e => e.dt >= `${y}-${m}-01`) || entries[0];
  const ytdFrom = closestBefore(entries, `${y}-01-01`)   || entries.find(e => e.dt >= `${y}-01-01`)   || entries[0];
  const from12m = closestBefore(entries, shiftBack(baseDt, 12)) || entries[0];
  const from24m = closestBefore(entries, shiftBack(baseDt, 24)) || entries[0];

  return {
    retMTD: ret(mtdFrom, base), retYTD: ret(ytdFrom, base),
    ret12m: ret(from12m, base), ret24m: ret(from24m, base),
    pl: base.pl, quota: base.quota, cotistas: base.cotistas, lastDt: base.dt,
  };
}

// ── Busca cotas de um mês (ZIP no novo caminho, fallback HIST) ────────────────
async function fetchCotaMonth(ym) {
  // Tenta caminho atual, depois histórico
  for (const url of [CVM_DIARIO(ym), CVM_DIARIO_H(ym)]) {
    try {
      const buf = await fetchBuffer(url);
      const csvs = unzipCSVs(buf);
      if (csvs.length > 0) {
        console.log(`OK ${url.split('/').pop()} → ${csvs[0].name}`);
        return parseCSV(csvs[0].content);
      }
    } catch(e) { /* tenta próximo */ }
  }
  console.warn(`Nenhum arquivo encontrado para ${ym}`);
  return [];
}

// ── Cadastro: combina cad_fi.csv (antigo) + registro_classe.csv (novo RCVM175) ──
async function fetchCadastro() {
  const all = [];

  // 1. Fundos antigos (não adaptados RCVM175)
  try {
    const text = await fetchText(CVM_CAD_OLD);
    const rows = parseCSV(text);
    for (const r of rows) {
      all.push({
        CNPJ_FUNDO:   (r.CNPJ_FUNDO || '').replace(/[.\-\/]/g, '').trim(),
        DENOM_SOCIAL: r.DENOM_SOCIAL || '',
        TP_FUNDO:     r.TP_FUNDO || r.CLASSE || '',
        SIT:          r.SIT || '',
      });
    }
    console.log(`cad_fi.csv: ${rows.length} linhas`);
  } catch(e) { console.warn('cad_fi.csv:', e.message); }

  // 2. Fundos novos RCVM175 — registro_classe.csv
  // Colunas: CNPJ_Classe, Tipo_Classe, Denominacao_Social, Situacao
  try {
    const buf  = await fetchBuffer(CVM_CAD_ZIP);
    const csvs = unzipCSVs(buf);
    for (const f of csvs) {
      if (!f.name.toLowerCase().includes('classe')) continue;
      const rows = parseCSV(f.content);
      for (const r of rows) {
        all.push({
          CNPJ_FUNDO:   (r.CNPJ_Classe || '').replace(/[.\-\/]/g, '').trim(),
          DENOM_SOCIAL: r.Denominacao_Social || '',
          TP_FUNDO:     r.Tipo_Classe || '',
          SIT:          r.Situacao || '',
        });
      }
      console.log(`${f.name}: ${rows.length} linhas`);
    }
  } catch(e) { console.warn('registro_fundo_classe.zip:', e.message); }

  console.log(`Total cadastro: ${all.length} linhas`);
  return all;
}

module.exports = {
  fetchText, fetchBuffer, parseCSV, classifyFund,
  toYYYYMM, buildCotaIndex, closestBefore, computeReturns,
  fetchCotaMonth, fetchCadastro,
};
