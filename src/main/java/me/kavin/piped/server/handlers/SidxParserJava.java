package me.kavin.piped.server.handlers;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/// Minimal ISO 14496-12 sidx box parser. Java port of the Swift SidxParser.
/// Used by SynthHlsHandlers to split single-byte-range segments into multiple
/// keyframe-aligned fragments — YouTube CDN blocks Range requests > ~10-50MB,
/// so a single 130MB segment fails. Each fragment from sidx is typically 1-2MB.
public class SidxParserJava {

    public static class Entry {
        public final double duration; // seconds
        public final int byteSize;
        Entry(double d, int b) { duration = d; byteSize = b; }
    }

    public static class Data {
        public final int timescale;
        public final int firstOffset;
        public final List<Entry> entries;
        Data(int ts, int fo, List<Entry> e) { timescale = ts; firstOffset = fo; entries = e; }
    }

    private static final ConcurrentMap<String, Data> CACHE = new ConcurrentHashMap<>();
    private static final String CHROME_UA =
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

    /// Cache key for a sidx fetch. The sidx (segment index box) is intrinsic to
    /// the video+itag+byte-range and does NOT depend on the cpn. But SynthHls
    /// swaps a fresh cpn into the URL per play (swapCpn, throttle-safety) — so
    /// keying on the raw URL would MISS on every play and re-fetch the index from
    /// googlevideo. Strip the cpn so the index is reused across plays / prefetch.
    private static String cacheKey(String url, int start, int end) {
        String stable = url == null ? "" : url.replaceAll("([?&]cpn=)[A-Za-z0-9_-]{16}", "$1X");
        return stable + "#" + start + "-" + end;
    }

    // Aggressive per-attempt timeouts: these bound socket INACTIVITY, not total
    // transfer time (readAllBytes streams; a slow-but-steady large sidx keeps
    // resetting the read clock). So they catch a transient googlevideo STALL
    // (saw a single 14172ms fetch) without cutting legit large fetches short.
    private static final int SIDX_CONNECT_TIMEOUT_MS = 1500;
    private static final int SIDX_READ_TIMEOUT_MS = 2000;
    private static final int SIDX_MAX_ATTEMPTS = 3;
    private static final int SIDX_MAX_REDIRECTS = 3;

    /// Fetch + parse sidx for given url at byte range [start, end] (inclusive).
    /// Resolves nested sidx-chains recursively. Short timeout + fast retry so a
    /// transient googlevideo range-stall costs ~2s + a retry, not ~14s — the
    /// prewarm only MOVES the sidx fetch, it doesn't make it stall-proof, so the
    /// stall-proofing lives here (shared by prewarm AND tap path).
    public static Data fetch(String url, int start, int end, String cookies) {
        String key = cacheKey(url, start, end);
        Data hit = CACHE.get(key);
        if (hit != null) {
            System.out.println("[SidxCache] HIT itag=" + itagOf(url) + " range=" + start + "-" + end);
            return hit;
        }
        long t0 = System.currentTimeMillis();
        for (int attempt = 1; attempt <= SIDX_MAX_ATTEMPTS; attempt++) {
            // On retry, swap the cpn nonce + open a fresh connection — a transient
            // stall is usually a single slow googlevideo edge/connection; a new
            // attempt routes around it. sidx is cpn-independent, so the swapped
            // url resolves to the same cache key.
            String attemptUrl = attempt == 1 ? url : swapCpn(url);
            long a0 = System.currentTimeMillis();
            Data out = fetchOnce(attemptUrl, start, end, cookies);
            if (out != null) {
                CACHE.put(key, out);
                System.out.println("[SidxCache] MISS itag=" + itagOf(url) + " range=" + start + "-" + end
                        + " fetchMs=" + (System.currentTimeMillis() - t0) + " entries=" + out.entries.size()
                        + (attempt > 1 ? " attempts=" + attempt : ""));
                return out;
            }
            System.out.println("[SidxCache] RETRY itag=" + itagOf(url) + " range=" + start + "-" + end
                    + " attempt=" + attempt + " ms=" + (System.currentTimeMillis() - a0));
        }
        System.out.println("[SidxCache] GIVEUP itag=" + itagOf(url) + " range=" + start + "-" + end
                + " fetchMs=" + (System.currentTimeMillis() - t0));
        return null;
    }

    /// sidx aus einer LOKALEN Datei parsen (SABR-Cache) — ohne HTTP.
    ///
    /// ⚠️ SELBSTBLOCKADE (gefunden 2026-07-25): der frühere Weg holte den sidx per
    /// HTTP gegen unseren EIGENEN `/sabr`-Endpoint. Der belegt aber denselben
    /// 2-Slot-`YT_RESOLVE_LIMITER` wie `/synth-hls` — und AVPlayer fragt Video-
    /// UND Audio-Playlist PARALLEL an. Beide Slots sind damit von den Playlist-
    /// Buildern belegt, deren innere sidx-Holer keinen mehr bekommen:
    /// `tryAcquire(500ms)` scheitert → 503 → `fetch` gibt null → der Aufrufer
    /// lieferte `#ERROR: sabr sidx parse failed` als **200-Body** → AVPlayer sieht
    /// keine gültige Playlist → **-12646**, sofortiger Abbruch ohne Retry.
    /// Reproduziert mit echtem AVPlayer auf einem kalten Made-for-Kids-Video.
    /// Die Datei liegt lokal vor — der Netz-Umweg war ohnehin sinnlos.
    public static Data fromFile(java.nio.file.Path file, int start, int end) {
        try (var ch = java.nio.channels.FileChannel.open(
                file, java.nio.file.StandardOpenOption.READ)) {
            return fromChannel(ch, start, end);
        } catch (IOException e) {
            return null;
        }
    }

    private static Data fromChannel(java.nio.channels.FileChannel ch, int start, int end)
            throws IOException {
        final byte[] data = readRange(ch, start, end);
        if (data == null) return null;
        final RawBox top = parseSidx(data);
        if (top == null) return null;
        final List<Entry> flat = new ArrayList<>();
        int cursor = end + 1 + top.firstOffset;
        for (RawEntry e : top.entries) {
            if (e.isIndex) {   // verschachtelter sidx — liegt in derselben Datei
                final Data sub = fromChannel(ch, cursor, cursor + e.byteSize - 1);
                if (sub != null) flat.addAll(sub.entries);
            } else {
                flat.add(new Entry(e.duration, e.byteSize));
            }
            cursor += e.byteSize;
        }
        return new Data(top.timescale, top.firstOffset, flat);
    }

    /// Exakter Range-Read; null wenn die Datei (noch) zu kurz ist — der Aufrufer
    /// wartet dann auf mehr Bytes, statt eine halbe Box zu parsen.
    private static byte[] readRange(java.nio.channels.FileChannel ch, int start, int end)
            throws IOException {
        final long size = ch.size();
        if (start < 0 || end < start || start >= size) return null;
        final int len = end - start + 1;
        if (start + (long) len > size) return null;
        final ByteBuffer buf = ByteBuffer.allocate(len);
        ch.position(start);
        while (buf.hasRemaining() && ch.read(buf) > 0) { /* fill */ }
        return buf.hasRemaining() ? null : buf.array();
    }

    /// One network attempt. Returns null on timeout / non-2xx / parse-fail so the
    /// caller retries. Nested index chains recurse through the public fetch (each
    /// gets its own retry + cache). Does NOT cache — the outer fetch does.
    private static Data fetchOnce(String url, int start, int end, String cookies) {
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setRequestMethod("GET");
            // Auto-follow drops the Range header on the redirected request (-> 200 full
            // file or a reject); disable it so our manual loop re-applies Range and keeps 206.
            conn.setInstanceFollowRedirects(false);
            conn.setRequestProperty("Range", "bytes=" + start + "-" + end);
            conn.setRequestProperty("User-Agent", CHROME_UA);
            if (cookies != null) conn.setRequestProperty("Cookie", cookies);
            conn.setConnectTimeout(SIDX_CONNECT_TIMEOUT_MS);
            conn.setReadTimeout(SIDX_READ_TIMEOUT_MS);
            int code = conn.getResponseCode();
            // Follow googlevideo's CDN load-balancer redirect (302 cms_redirect ->
            // final edge with ipbypass). NewPipe's downloader disables HttpURLConnection
            // auto-follow globally, and auto-follow can silently drop the Range header
            // anyway, so follow manually and RE-APPLY Range to keep getting 206 partials.
            int redirectHops = 0;
            while ((code == 301 || code == 302 || code == 303 || code == 307 || code == 308)
                    && redirectHops < SIDX_MAX_REDIRECTS) {
                String loc = conn.getHeaderField("Location");
                conn.disconnect();
                if (loc == null) return null;
                conn = (HttpURLConnection) new URL(loc).openConnection();
                conn.setRequestMethod("GET");
                conn.setInstanceFollowRedirects(false);
                conn.setRequestProperty("Range", "bytes=" + start + "-" + end);
                conn.setRequestProperty("User-Agent", CHROME_UA);
                if (cookies != null) conn.setRequestProperty("Cookie", cookies);
                conn.setConnectTimeout(SIDX_CONNECT_TIMEOUT_MS);
                conn.setReadTimeout(SIDX_READ_TIMEOUT_MS);
                System.out.println("[SidxCache] REDIRECT itag=" + itagOf(url) + " hop=" + (redirectHops + 1));
                code = conn.getResponseCode();
                redirectHops++;
            }
            if (code != 200 && code != 206) {
                conn.disconnect();
                return null;
            }
            byte[] data;
            try (var is = conn.getInputStream()) {
                data = is.readAllBytes();
            }
            conn.disconnect();

            RawBox top = parseSidx(data);
            if (top == null) return null;

            List<Entry> flat = new ArrayList<>();
            int cursor = end + 1 + top.firstOffset;
            for (RawEntry e : top.entries) {
                if (e.isIndex) {
                    Data sub = fetch(url, cursor, cursor + e.byteSize - 1, cookies);
                    if (sub != null) flat.addAll(sub.entries);
                } else {
                    flat.add(new Entry(e.duration, e.byteSize));
                }
                cursor += e.byteSize;
            }
            return new Data(top.timescale, top.firstOffset, flat);
        } catch (IOException e) {
            return null;
        }
    }

    private static final java.security.SecureRandom CPN_RNG = new java.security.SecureRandom();
    private static final char[] CPN_ALPHA =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_".toCharArray();

    /// Swap the cpn tracking nonce for a fresh one (cpn ∉ signed sparams, so the
    /// URL stays valid). Used only on retry to dodge a per-connection stall.
    private static String swapCpn(String url) {
        if (url == null) return null;
        char[] c = new char[16];
        for (int i = 0; i < 16; i++) c[i] = CPN_ALPHA[CPN_RNG.nextInt(CPN_ALPHA.length)];
        return url.replaceAll("([?&]cpn=)[A-Za-z0-9_-]{16}", "$1" + new String(c));
    }

    private static String itagOf(String url) {
        if (url == null) return "?";
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("[?&]itag=([0-9]+)").matcher(url);
        return m.find() ? m.group(1) : "?";
    }

    private static class RawEntry {
        final boolean isIndex;
        final int byteSize;
        final double duration;
        RawEntry(boolean ii, int bs, double d) { isIndex = ii; byteSize = bs; duration = d; }
    }
    private static class RawBox {
        final int timescale;
        final int firstOffset;
        final List<RawEntry> entries;
        RawBox(int ts, int fo, List<RawEntry> e) { timescale = ts; firstOffset = fo; entries = e; }
    }

    private static RawBox parseSidx(byte[] data) {
        int offset = 0;
        while (offset + 8 <= data.length) {
            long size = readU32(data, offset);
            String type = new String(data, offset + 4, 4);
            if (type.equals("sidx")) {
                return parseSidxBox(data, offset, (int) size);
            }
            if (size < 8 || offset + size > data.length) return null;
            offset += (int) size;
        }
        return null;
    }

    private static RawBox parseSidxBox(byte[] data, int offset, int size) {
        int p = offset + 8; // skip size + type
        if (p >= data.length) return null;
        int version = data[p] & 0xff;
        p += 4; // version + flags

        p += 4; // reference_ID
        if (p + 4 > data.length) return null;
        int timescale = (int) readU32(data, p);
        p += 4;

        int firstOffset;
        if (version == 0) {
            if (p + 8 > data.length) return null;
            p += 4; // earliest_presentation_time
            firstOffset = (int) readU32(data, p);
            p += 4;
        } else {
            if (p + 16 > data.length) return null;
            p += 8; // earliest_presentation_time 64-bit
            firstOffset = (int) readU64(data, p);
            p += 8;
        }

        if (p + 4 > data.length) return null;
        p += 2; // reserved
        int refCount = ((data[p] & 0xff) << 8) | (data[p + 1] & 0xff);
        p += 2;

        List<RawEntry> entries = new ArrayList<>(refCount);
        for (int i = 0; i < refCount; i++) {
            if (p + 12 > data.length) return null;
            long sizeWord = readU32(data, p);
            boolean isIndex = (sizeWord & 0x80000000L) != 0;
            int refSize = (int) (sizeWord & 0x7FFFFFFFL);
            p += 4;
            long subDur = readU32(data, p);
            p += 4;
            p += 4; // SAP info
            double durSec = timescale > 0 ? (double) subDur / (double) timescale : 0;
            entries.add(new RawEntry(isIndex, refSize, durSec));
        }
        return new RawBox(timescale, firstOffset, entries);
    }

    private static long readU32(byte[] d, int o) {
        return ((long)(d[o] & 0xff) << 24) | ((d[o+1] & 0xff) << 16) | ((d[o+2] & 0xff) << 8) | (d[o+3] & 0xff);
    }

    private static long readU64(byte[] d, int o) {
        long v = 0;
        for (int i = 0; i < 8; i++) v = (v << 8) | (d[o+i] & 0xff);
        return v;
    }
}
