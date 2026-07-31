// Prägt einen GVS-po_token IN DERSELBEN Sitzung, in der auch gestreamt wird.
//
// WARUM: Unser bisheriger Token kommt aus dem bgutil-Container, der eine EIGENE
// BotGuard-Sitzung fährt. YouTube duldet so einen "fremden Ausweis" nur ~1–2 MB
// und schaltet dann auf "Attestierung erforderlich" (STREAM_PROTECTION_STATUS
// 2→3) — das sind die 60 Sekunden, nach denen Kids-Videos abbrechen. Ein echter
// Browser prägt in der Sitzung, in der er auch die Medien holt.
//
// WIE: Chromium mit UNSEREN Login-Cookies öffnet eine Watch-Seite, wir hängen
// uns vor allen Seitenskripten in window.fetch, starten die Wiedergabe und
// fischen aus der ersten SABR-Anfrage (POST auf googlevideo) das Token heraus:
// Rumpf ist protobuf, Feld 19 = streamerContext, darin Feld 2 = po_token.
// Zusätzlich lesen wir die visitorData der Seite — Token und Sitzung gehören
// zusammen und müssen gemeinsam verwendet werden.
//
// Struktur 2026-07-31 am echten Browser gemessen: streamerContext 327 B mit
// clientInfo 58 B, po_token 85 B, playback_cookie 86 B, sabr_contexts 86 B.

const fs = require('fs');
const puppeteer = require('puppeteer-core');

const COOKIE_FILE = process.env.YOUTUBE_COOKIES_FILE || '/home/simon/piped/youtube-cookies.txt';
const CHROME = process.env.CHROME_PATH || '/usr/bin/chromium';
const VIDEO = process.argv[2] || 'dQw4w9WgXcQ';
const TIMEOUT_MS = Number(process.env.CAPTURE_TIMEOUT_MS || 60000);

/// Netscape-Cookies lesen — NUR youtube.com. Ein Browser-Export enthält alle
/// Domains; alles zusammen quittiert Google mit CookieMismatch.
function readCookies() {
  if (!fs.existsSync(COOKIE_FILE)) return [];
  return fs.readFileSync(COOKIE_FILE, 'utf8').split('\n')
    .map(l => l.trim())
    .filter(l => l && !l.startsWith('#'))
    .map(l => l.split('\t'))
    .filter(p => p.length >= 7 && p[0].includes('youtube.com'))
    .map(p => ({
      name: p[5], value: p[6],
      domain: p[0].startsWith('.') ? p[0] : '.' + p[0],
      path: p[2] || '/', secure: p[3] === 'TRUE',
      expires: Number(p[4]) || Math.floor(Date.now() / 1000) + 86400,
    }));
}

/// Minimaler protobuf-Leser: liefert {feld: {start, len}} für Längen-Felder.
function parseFields(buf) {
  const out = {};
  let p = 0;
  while (p < buf.length) {
    let key = 0, sh = 0;
    while (p < buf.length) { const b = buf[p++]; key |= (b & 0x7f) << sh; sh += 7; if (!(b & 0x80)) break; }
    const field = key >> 3, wire = key & 7;
    if (wire === 2) {
      let len = 0; sh = 0;
      while (p < buf.length) { const b = buf[p++]; len |= (b & 0x7f) << sh; sh += 7; if (!(b & 0x80)) break; }
      out[field] = { start: p, len };
      p += len;
    } else if (wire === 0) {
      while (p < buf.length) { const b = buf[p++]; if (!(b & 0x80)) break; }
    } else if (wire === 5) p += 4;
    else if (wire === 1) p += 8;
    else break;
  }
  return out;
}

(async () => {
  const browser = await puppeteer.launch({
    executablePath: CHROME,
    headless: 'new',
    args: [
      '--no-sandbox', '--disable-dev-shm-usage', '--mute-audio',
      '--autoplay-policy=no-user-gesture-required',
      '--window-size=1280,720',
    ],
  });
  try {
    const page = await browser.newPage();
    await page.setUserAgent('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
      + '(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36');
    const cookies = readCookies();
    if (cookies.length) await page.setCookie(...cookies);

    // Hook VOR allen Seitenskripten setzen — sonst ist der Player schneller.
    await page.evaluateOnNewDocument(() => {
      window.__abr = [];
      const orig = window.fetch;
      window.fetch = async function (input, init) {
        try {
          const isReq = input && typeof input === 'object' && typeof input.clone === 'function';
          const url = isReq ? input.url : String(input);
          if (url.includes('googlevideo.com')) {
            const method = isReq ? input.method : ((init && init.method) || 'GET');
            if (method === 'POST') {
              let bytes = null;
              if (isReq) bytes = new Uint8Array(await input.clone().arrayBuffer());
              else if (init && init.body) {
                const b = init.body;
                if (b instanceof ArrayBuffer) bytes = new Uint8Array(b);
                else if (b instanceof Uint8Array) bytes = b;
              }
              if (bytes && bytes.length) window.__abr.push(Array.from(bytes));
            }
          }
        } catch (e) { /* Hook darf die Seite nie stören */ }
        return orig.apply(this, arguments);
      };
    });

    await page.goto('https://www.youtube.com/watch?v=' + VIDEO, { waitUntil: 'domcontentloaded', timeout: 45000 });
    await page.evaluate(() => {
      const v = document.querySelector('video');
      if (v) { v.muted = true; v.play().catch(() => {}); }
    });

    // ⚠️ NICHT die erste Anfrage nehmen: der Player feuert die erste SABR-Runde
    // ab, BEVOR BotGuard fertig ist, und legt solange einen ~10-Byte-Platzhalter
    // hinein (gemessen 2026-07-31). Das echte Token ist ~85 Byte. Also alle
    // Anfragen durchsehen und auf ein plausibles Token warten.
    const MIN_TOKEN = Number(process.env.MIN_TOKEN_BYTES || 40);
    const deadline = Date.now() + TIMEOUT_MS;
    let tokenBytes = null, gesehen = 0, bestLen = 0;
    while (Date.now() < deadline) {
      const bodies = await page.evaluate(() => (window.__abr || []).map(b => b));
      gesehen = bodies.length;
      for (const body of bodies) {
        const buf = Buffer.from(body);
        const top = parseFields(buf);
        if (!top[19]) continue;
        const sc = parseFields(buf.subarray(top[19].start, top[19].start + top[19].len));
        if (!sc[2]) continue;
        const t = buf.subarray(top[19].start + sc[2].start, top[19].start + sc[2].start + sc[2].len);
        if (t.length > bestLen) bestLen = t.length;
        if (t.length >= MIN_TOKEN) { tokenBytes = t; break; }
      }
      if (tokenBytes) break;
      await new Promise(r => setTimeout(r, 1500));
      await page.evaluate(() => {
        const v = document.querySelector('video');
        if (v) { if (v.paused) v.play().catch(() => {}); else v.currentTime = v.currentTime + 30; }
      });
    }
    if (!tokenBytes)
      throw new Error('kein brauchbares Token (Anfragen gesehen: ' + gesehen + ', laengstes Token: ' + bestLen + 'B)');

    // Auch die SITZUNGS-Bausteine mitnehmen: abrUrl und ustreamerConfig
    // entstehen im Player-Aufruf. Wenn die Duldungsgrenze an der Sitzung haengt
    // (immer exakt 61,1 s / 12 Segmente, unabhaengig vom Token), muss der
    // Unterschied hier liegen.
    const sitzung = await page.evaluate(() => {
      const sd = (window.ytInitialPlayerResponse || {}).streamingData || {};
      const cfgKey = 'serverAbrStreamingUrl';
      let ust = '';
      try {
        const raw = JSON.stringify(window.ytInitialPlayerResponse || {});
        const m = raw.match(/"videoPlaybackUstreamerConfig":"([^"]+)"/);
        if (m) ust = m[1];
      } catch (e) {}
      return { abrUrl: sd[cfgKey] || '', ustreamerConfig: ust };
    });

    const visitorData = await page.evaluate(() =>
      (window.ytcfg && ytcfg.data_ && ytcfg.data_.VISITOR_DATA) || '');
    const loggedIn = await page.evaluate(() =>
      !!(window.ytcfg && ytcfg.data_ && ytcfg.data_.LOGGED_IN));

    process.stdout.write(JSON.stringify({
      poToken: tokenBytes.toString('base64'),
      poTokenLen: tokenBytes.length,
      visitorData,
      loggedIn,
      videoId: VIDEO,
      abrUrl: sitzung.abrUrl,
      ustreamerConfig: sitzung.ustreamerConfig,
      mintedAt: Date.now(),
    }) + '\n');
  } finally {
    await browser.close();
  }
})().catch(e => {
  process.stderr.write('FEHLER: ' + e.message + '\n');
  process.exit(1);
});
