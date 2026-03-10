const { fetchBuffer, fetchText, parseCSV } = require('./lib/cvm');
const zlib = require('zlib');
const CORS = { 'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json; charset=utf-8' };

function unzipFirst(buf) {
  let off = 0;
  while (off < buf.length - 30) {
    if (buf[off]===0x50&&buf[off+1]===0x4b&&buf[off+2]===0x03&&buf[off+3]===0x04) {
      const comp=buf.readUInt16LE(off+8), csz=buf.readUInt32LE(off+18);
      const fnl=buf.readUInt16LE(off+26), exl=buf.readUInt16LE(off+28);
      const fn=buf.slice(off+30,off+30+fnl).toString(), ds=off+30+fnl+exl;
      const cd=buf.slice(ds,ds+csz);
      try {
        const ct=comp===0?cd.toString('latin1'):zlib.inflateRawSync(cd).toString('latin1');
        return { name: fn, header: ct.split('\n')[0], lines: ct.split('\n').length, row1: ct.split('\n')[1]?.substring(0,250) };
      } catch(e) { return { name: fn, error: e.message }; }
    } else { off++; }
  }
  return null;
}

exports.handler = async () => {
  const out = {};

  const now = new Date(); now.setDate(now.getDate()-1);
  const ym = `${now.getFullYear()}${String(now.getMonth()+1).padStart(2,'0')}`;

  const diarioUrl = `https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_${ym}.zip`;
  try {
    const buf = await fetchBuffer(diarioUrl);
    out.diario = { ok: true, url: diarioUrl, bytes: buf.length, csv: unzipFirst(buf) };
  } catch(e) { out.diario = { ok: false, url: diarioUrl, error: e.message }; }

  return { statusCode: 200, headers: CORS, body: JSON.stringify(out, null, 2) };
};
