// netlify/functions/debug.js
// GET /api/debug — mostra estrutura real dos arquivos CVM
// REMOVA este arquivo após resolver o problema

const { fetchText, fetchBuffer, parseCSV, CVM_BASE } = require('./lib/cvm');
const zlib = require('zlib');

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Content-Type': 'application/json; charset=utf-8',
};

function unzipCSVs(buf) {
  const results = [];
  let offset = 0;
  while (offset < buf.length - 30) {
    if (buf[offset]===0x50 && buf[offset+1]===0x4b && buf[offset+2]===0x03 && buf[offset+3]===0x04) {
      const compression    = buf.readUInt16LE(offset + 8);
      const compressedSize = buf.readUInt32LE(offset + 18);
      const fileNameLen    = buf.readUInt16LE(offset + 26);
      const extraLen       = buf.readUInt16LE(offset + 28);
      const fileName = buf.slice(offset + 30, offset + 30 + fileNameLen).toString();
      const dataStart = offset + 30 + fileNameLen + extraLen;
      if (compressedSize > 0) {
        try {
          const compData = buf.slice(dataStart, dataStart + compressedSize);
          const content = compression === 0
            ? compData.toString('latin1')
            : zlib.inflateRawSync(compData).toString('latin1');
          const header = content.split('\n')[0];
          const row1   = content.split('\n')[1]?.substring(0, 200);
          const lines  = content.split('\n').length;
          results.push({ name: fileName, header, row1, lines, compression });
        } catch(e) { results.push({ name: fileName, error: e.message }); }
      }
      offset = dataStart + compressedSize;
    } else { offset++; }
  }
  return results;
}

exports.handler = async () => {
  const out = {};

  // 1. cad_fi.csv
  try {
    const text = await fetchText(`${CVM_BASE}/CAD/DADOS/cad_fi.csv`);
    const lines = text.split('\n');
    out.cad_fi = {
      ok: true,
      totalLines: lines.length,
      header: lines[0],
      row1: lines[1]?.substring(0, 300),
      row2: lines[2]?.substring(0, 300),
    };
  } catch(e) { out.cad_fi = { ok: false, error: e.message }; }

  // 2. registro_fundo_classe.zip
  try {
    const buf = await fetchBuffer(`${CVM_BASE}/CAD/DADOS/registro_fundo_classe.zip`);
    out.registro_zip = { ok: true, sizeBytes: buf.length, files: unzipCSVs(buf) };
  } catch(e) { out.registro_zip = { ok: false, error: e.message }; }

  // 3. inf_diario_fi_202502.csv
  try {
    const text = await fetchText(`${CVM_BASE}/INF/DIARIO/DADOS/inf_diario_fi_202502.csv`);
    const lines = text.split('\n');
    out.diario_csv = { ok: true, totalLines: lines.length, header: lines[0], row1: lines[1]?.substring(0,200) };
  } catch(e) { out.diario_csv = { ok: false, error: e.message }; }

  // 4. inf_diario_fi_202502.zip
  try {
    const buf = await fetchBuffer(`${CVM_BASE}/INF/DIARIO/DADOS/inf_diario_fi_202502.zip`);
    out.diario_zip = { ok: true, sizeBytes: buf.length, files: unzipCSVs(buf) };
  } catch(e) { out.diario_zip = { ok: false, error: e.message }; }

  return { statusCode: 200, headers: CORS, body: JSON.stringify(out, null, 2) };
};
