const { fetchText, fetchBuffer, CVM_BASE } = require('./lib/cvm');
const zlib = require('zlib');
const CORS = { 'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json; charset=utf-8' };

exports.handler = async () => {
  const out = {};

  // Try different diario URL patterns
  const ym_list = ['202503','202502','202501','202412'];
  const patterns = [
    ym => `${CVM_BASE}/INF/DIARIO/DADOS/inf_diario_fi_${ym}.csv`,
    ym => `${CVM_BASE}/INF/DIARIO/DADOS/inf_diario_fi_${ym}.zip`,
    ym => `https://dados.cvm.gov.br/dados/FI/INF/DIARIO/DADOS/HIST/inf_diario_fi_${ym}.zip`,
    ym => `https://dados.cvm.gov.br/dados/FI/INF/DIARIO/DADOS/HIST/inf_diario_fi_${ym}.csv`,
  ];

  out.diario_probes = {};
  for (const ym of ym_list) {
    for (const pat of patterns) {
      const url = pat(ym);
      const key = url.split('/DADOS/').pop();
      try {
        const buf = await fetchBuffer(url);
        const sig = buf.slice(0,4).toString('hex');
        const isZip = sig === '504b0304';
        let preview = '';
        if (isZip) {
          // extract first csv header
          let off = 0;
          while (off < buf.length - 30) {
            if (buf[off]===0x50&&buf[off+1]===0x4b&&buf[off+2]===0x03&&buf[off+3]===0x04) {
              const comp = buf.readUInt16LE(off+8);
              const csz  = buf.readUInt32LE(off+18);
              const fnl  = buf.readUInt16LE(off+26);
              const exl  = buf.readUInt16LE(off+28);
              const fn   = buf.slice(off+30, off+30+fnl).toString();
              const ds   = off+30+fnl+exl;
              const cd   = buf.slice(ds, ds+csz);
              const ct   = comp===0 ? cd.toString('latin1') : zlib.inflateRawSync(cd).toString('latin1');
              preview = fn + ' | ' + ct.split('\n')[0];
              break;
            } else { off++; }
          }
        } else {
          preview = buf.toString('latin1').split('\n')[0];
        }
        out.diario_probes[key] = { ok: true, bytes: buf.length, isZip, preview: preview.substring(0,200) };
      } catch(e) {
        out.diario_probes[key] = { ok: false, error: e.message };
      }
    }
  }

  return { statusCode: 200, headers: CORS, body: JSON.stringify(out, null, 2) };
};
