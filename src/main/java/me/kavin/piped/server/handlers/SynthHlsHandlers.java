package me.kavin.piped.server.handlers;

import me.kavin.piped.utils.CollectionUtils;
import me.kavin.piped.utils.obj.PipedStream;
import me.kavin.piped.utils.obj.Streams;
import me.kavin.piped.utils.Multithreading;
import org.schabi.newpipe.extractor.stream.StreamInfo;
import org.schabi.newpipe.extractor.services.youtube.extractors.YoutubeStreamExtractor;
import java.net.HttpURLConnection;
import java.net.URL;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.List;

/// Synthesizes an HLS manifest from Piped's DASH streams.
/// AVPlayer receives an external http:// URL pointing at Pi5; native HLS engine
/// handles it (aggressive prefetching). Segments are routed through /yt-proxy to
/// work around piped-proxy's HEAD-request and large-range limitations.
public class SynthHlsHandlers {

    public static byte[] masterPlaylist(String videoId) throws Exception {
        return masterPlaylist(videoId, 1080, DEFAULT_VIDEO_CODECS);
    }

    public static byte[] masterPlaylist(String videoId, int maxH, String[] codecs) throws Exception {
        // requireVerified=false: the master only lists variant playlist names,
        // never segment URLs, so it doesn't need the throttle HEAD-probe /
        // WebEmbed re-resolve. Skipping that keeps this (the dominant
        // FILE_LOADED cost) fast; the variant/audio fetches verify before any
        // segment URL is served.
        Streams streams = fetchStreams(videoId, false);
        if (isSabrMode(videoId, streams)) return sabrMaster(videoId);
        List<PipedStream> videos = pickedVideoStreams(streams, maxH, codecs);
        PipedStream audio = pickedAudioStream(streams);
        if (videos.isEmpty() || audio == null) {
            return "#ERROR: no playable streams".getBytes(StandardCharsets.UTF_8);
        }
        // mpv drops the query on relative variant resolution, so the picked
        // rendition must be re-encoded into the variant URI — else video0.m3u8
        // would serve the default 1080/avc and mismatch this master's CODECS.
        final String q = "?maxh=" + maxH + "&codecs=" + String.join(",", codecs);
        StringBuilder sb = new StringBuilder();
        sb.append("#EXTM3U\n");
        sb.append("#EXT-X-VERSION:7\n");
        sb.append("#EXT-X-INDEPENDENT-SEGMENTS\n");
        String audioCodec = audio.codec != null ? audio.codec : "mp4a.40.2";
        sb.append("#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio\",NAME=\"Audio\",DEFAULT=YES,AUTOSELECT=YES,URI=\"audio.m3u8\"\n");
        for (int i = 0; i < videos.size(); i++) {
            PipedStream v = videos.get(i);
            int bw = (v.bitrate > 0 ? v.bitrate : 5_000_000) + (audio.bitrate > 0 ? audio.bitrate : 128_000);
            int w = v.width > 0 ? v.width : 1920;
            int h = v.height > 0 ? v.height : 1080;
            int fps = v.fps > 0 ? v.fps : 30;
            String vCodec = v.codec != null ? v.codec : "avc1.64002a";
            // Apple HLS requires VIDEO-RANGE for non-SDR (av01 HDR: transfer-chars
            // field tc=16 -> PQ/HDR10, tc=18 -> HLG). Without it AVPlayer rejects the
            // HDR variant (CoreMediaErrorDomain -12927). SDR variants stay untouched
            // (byte-identical master) since SDR is the implicit default.
            String vr = hlsVideoRange(vCodec);
            String vrAttr = vr.equals("SDR") ? "" : ("VIDEO-RANGE=" + vr + ",");
            sb.append(String.format("#EXT-X-STREAM-INF:BANDWIDTH=%d,AVERAGE-BANDWIDTH=%d,RESOLUTION=%dx%d,FRAME-RATE=%d,%sCODECS=\"%s,%s\",AUDIO=\"audio\"\n",
                    bw, bw, w, h, fps, vrAttr, vCodec, audioCodec));
            sb.append("video").append(i).append(".m3u8").append(q).append("\n");
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] audioPlaylist(String videoId) throws Exception {
        Streams streams = fetchStreams(videoId);
        if (isSabrMode(videoId, streams))
            return sabrStreamPlaylist(videoId,
                    me.kavin.piped.utils.sabr.SabrCache.itagsFor(videoId)[0]);
        PipedStream audio = pickedAudioStream(streams);
        if (audio == null) return "#ERROR".getBytes(StandardCharsets.UTF_8);
        return streamPlaylist(audio, streams.duration);
    }

    public static byte[] videoPlaylist(String videoId, int idx) throws Exception {
        return videoPlaylist(videoId, idx, 1080, DEFAULT_VIDEO_CODECS);
    }

    public static byte[] videoPlaylist(String videoId, int idx, int maxH, String[] codecs) throws Exception {
        Streams streams = fetchStreams(videoId);
        if (isSabrMode(videoId, streams))
            return sabrStreamPlaylist(videoId,
                    me.kavin.piped.utils.sabr.SabrCache.itagsFor(videoId)[1]);
        List<PipedStream> videos = pickedVideoStreams(streams, maxH, codecs);
        if (idx < 0 || idx >= videos.size()) return "#ERROR".getBytes(StandardCharsets.UTF_8);
        return streamPlaylist(videos.get(idx), streams.duration);
    }

    /// Builds an HLS playlist for one DASH stream. Uses sidx fragment boundaries
    /// for multi-segment ranges (~1-2MB each). Falls back to single-segment if
    /// sidx fetch fails — that path may 403 on YouTube's CDN for large files.
    private static byte[] streamPlaylist(PipedStream stream, long durationSeconds) {
        int initLen = stream.initEnd - stream.initStart + 1;
        int mediaStart = stream.indexEnd + 1;
        long mediaLen = stream.contentLength - mediaStart;
        double dur = (double) durationSeconds;
        // THE SPLIT: stamp a FRESH single-use cpn into the (possibly long-cached)
        // URL for THIS play, so the long resolve cache stays throttle-safe.
        String freshUrl = swapCpn(stream.url);
        String segUrl = rewriteToYtProxy(freshUrl);

        SidxParserJava.Data sidx = null;
        if (stream.indexStart > 0 && stream.indexEnd > stream.indexStart) {
            // null cookies: the index is public; no need to forward the backend's
            // ~10KB authenticated cookie jar. Goes via the proxy like the segments
            // (proxy strips the hop-by-hop Connection header that else 400s).
            sidx = SidxParserJava.fetch(freshUrl, stream.indexStart, stream.indexEnd, null);
        }

        StringBuilder sb = new StringBuilder();
        sb.append("#EXTM3U\n");
        sb.append("#EXT-X-VERSION:7\n");

        if (sidx != null && !sidx.entries.isEmpty()) {
            int maxSegDur = 0;
            for (SidxParserJava.Entry e : sidx.entries) {
                int d = (int) Math.ceil(e.duration);
                if (d > maxSegDur) maxSegDur = d;
            }
            sb.append("#EXT-X-TARGETDURATION:").append(Math.max(maxSegDur, 1)).append('\n');
            sb.append("#EXT-X-MEDIA-SEQUENCE:0\n");
            // PLAYLIST-TYPE:VOD removed — triggers Apple HLS conservative ~60s buffer cap
            sb.append("#EXT-X-INDEPENDENT-SEGMENTS\n");
            sb.append(String.format("#EXT-X-MAP:URI=\"%s\",BYTERANGE=\"%d@%d\"\n",
                    segUrl, initLen, stream.initStart));
            long cursor = stream.indexEnd + 1L + sidx.firstOffset;
            for (SidxParserJava.Entry e : sidx.entries) {
                sb.append(String.format("#EXTINF:%.3f,\n", e.duration));
                sb.append(String.format("#EXT-X-BYTERANGE:%d@%d\n", e.byteSize, cursor));
                sb.append(segUrl).append('\n');
                cursor += e.byteSize;
            }
        } else {
            int durInt = (int) Math.ceil(dur);
            sb.append("#EXT-X-TARGETDURATION:").append(durInt).append('\n');
            sb.append("#EXT-X-MEDIA-SEQUENCE:0\n");
            // PLAYLIST-TYPE:VOD removed — triggers Apple HLS conservative ~60s buffer cap
            sb.append("#EXT-X-INDEPENDENT-SEGMENTS\n");
            sb.append(String.format("#EXT-X-MAP:URI=\"%s\",BYTERANGE=\"%d@%d\"\n",
                    segUrl, initLen, stream.initStart));
            sb.append(String.format("#EXTINF:%.3f,\n", dur));
            sb.append(String.format("#EXT-X-BYTERANGE:%d@%d\n", mediaLen, mediaStart));
            sb.append(segUrl).append('\n');
        }
        sb.append("#EXT-X-ENDLIST");
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    // ---- SABR mode: serve the manifest from the local /sabr cache files when a
    // video has no direct-URL streams (YouTube SABR-only) -- or when forced via
    // YT_FORCE_SABR for testing. The SABR fmp4 is standard (ftyp/moov/sidx/...),
    // so the same sidx->HLS byte-range segmentation applies; only the URL and the
    // box offsets come from the /sabr file instead of the resolved DASH stream.

    private static boolean isSabrMode(String videoId, Streams streams) {
        if (videoId.equals(System.getenv("YT_FORCE_SABR"))) return true;
        // Storm-marked by StreamHandlers stage 4 (WebEmbed+TVHTML5 segments 403,
        // SABR viability probed) — serve from /sabr while the mark lives (30 min).
        if (me.kavin.piped.utils.sabr.SabrCache.isStormMarked(videoId)) return true;
        // Auto: resolve yielded video formats but NONE has a usable URL.
        if (streams == null || streams.videoStreams == null || streams.videoStreams.isEmpty()) return false;
        for (PipedStream v : streams.videoStreams) {
            if (v.url != null && !v.url.isEmpty()) return false;
        }
        System.out.println("[ResolvePath] " + videoId + " -> SABR-FALLBACK (no direct video urls)");
        return true;
    }

    private static byte[] sabrMaster(String videoId) {
        StringBuilder sb = new StringBuilder();
        sb.append("#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-INDEPENDENT-SEGMENTS\n");
        sb.append("#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio\",NAME=\"Audio\",DEFAULT=YES,AUTOSELECT=YES,URI=\"audio.m3u8\"\n");
        sb.append("#EXT-X-STREAM-INF:BANDWIDTH=5128000,AVERAGE-BANDWIDTH=5128000,RESOLUTION=1920x1080,FRAME-RATE=30,CODECS=\"avc1.640028,mp4a.40.2\",AUDIO=\"audio\"\n");
        sb.append("video0.m3u8?maxh=1080&codecs=avc\n");
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] sabrStreamPlaylist(String videoId, int itag) throws Exception {
        final java.nio.file.Path file = me.kavin.piped.utils.sabr.SabrCache.ensureFile(videoId, itag);
        if (file == null) return "#ERROR: sabr download failed".getBytes(StandardCharsets.UTF_8);
        final int[] box = scanSabrSidx(file);
        if (box == null) {
            // Cache so short it lacks even ftyp/moov/sidx — a storm killed the
            // download before the first UMP media arrived. Kick a refill.
            me.kavin.piped.utils.sabr.SabrCache.requestRefill(videoId);
            return "#ERROR: sabr sidx not found".getBytes(StandardCharsets.UTF_8);
        }
        final int sidxStart = box[0];
        final int sidxEnd = sidxStart + box[1] - 1;
        final String segUrl = "/sabr/" + videoId + "/" + itag;
        final String fetchUrl = "http://localhost:" + me.kavin.piped.consts.Constants.PORT + "/sabr/" + videoId + "/" + itag;
        final SidxParserJava.Data sidx = SidxParserJava.fetch(fetchUrl, sidxStart, sidxEnd, null);
        if (sidx == null || sidx.entries.isEmpty()) {
            me.kavin.piped.utils.sabr.SabrCache.requestRefill(videoId);
            return "#ERROR: sabr sidx parse failed".getBytes(StandardCharsets.UTF_8);
        }

        StringBuilder sb = new StringBuilder();
        sb.append("#EXTM3U\n#EXT-X-VERSION:7\n");
        int maxSegDur = 0;
        for (SidxParserJava.Entry e : sidx.entries) {
            int d = (int) Math.ceil(e.duration);
            if (d > maxSegDur) maxSegDur = d;
        }
        sb.append("#EXT-X-TARGETDURATION:").append(Math.max(maxSegDur, 1)).append('\n');
        sb.append("#EXT-X-MEDIA-SEQUENCE:0\n#EXT-X-INDEPENDENT-SEGMENTS\n");
        // init segment = everything before the sidx box (ftyp + moov) = [0, sidxStart-1]
        sb.append(String.format("#EXT-X-MAP:URI=\"%s\",BYTERANGE=\"%d@%d\"\n", segUrl, sidxStart, 0));
        // HONEST playlist (2026-07-23): the sidx indexes the WHOLE video, but a
        // storm-truncated SABR download leaves the cache file short — advertising
        // segments beyond the file made /sabr answer 416 and AVPlayer refuse the
        // video outright. Emit only segments whose bytes are fully on disk; while
        // truncated, serve EVENT-style (no ENDLIST -> the player polls and the
        // playlist grows as the refill fills the cache).
        final long fileLen = java.nio.file.Files.size(file);
        long cursor = sidxEnd + 1L + sidx.firstOffset;
        int emitted = 0;
        for (SidxParserJava.Entry e : sidx.entries) {
            if (cursor + e.byteSize > fileLen) break;
            sb.append(String.format("#EXTINF:%.3f,\n", e.duration));
            sb.append(String.format("#EXT-X-BYTERANGE:%d@%d\n", e.byteSize, cursor));
            sb.append(segUrl).append('\n');
            cursor += e.byteSize;
            emitted++;
        }
        if (emitted >= sidx.entries.size()) {
            sb.append("#EXT-X-ENDLIST");
        } else if (me.kavin.piped.utils.sabr.SabrCache.isPartialTerminal(videoId)) {
            // Kids-Readahead-Cap (ab Verdict) bzw. Paced-Refill erschöpft — die
            // Teil-Playlist EHRLICH als VOD BEENDEN: der Player spielt die
            // vorhandenen Segmente sauber durch, statt an einer (fast sicher)
            // nie wachsenden Live-Playlist zu sterben (AVPlayer -12646). Der
            // Hintergrund-Refill läuft weiter; wächst der Cache doch, liefert
            // der nächste Playlist-Build mehr.
            System.out.println("[SynthHls] " + videoId + "/" + itag + " sabr cache truncated + cap/exhausted: "
                    + emitted + "/" + sidx.entries.size() + " -> partial ENDLIST");
            sb.append("#EXT-X-ENDLIST");
            me.kavin.piped.utils.sabr.SabrCache.requestRefill(videoId);
        } else {
            System.out.println("[SynthHls] " + videoId + "/" + itag + " sabr cache truncated: "
                    + emitted + "/" + sidx.entries.size() + " segments on disk ("
                    + fileLen + "B) -> EVENT playlist + refill");
            me.kavin.piped.utils.sabr.SabrCache.requestRefill(videoId);
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /// Scans the SABR fmp4's top-level boxes for the sidx box; returns {offset, size}.
    private static int[] scanSabrSidx(java.nio.file.Path file) throws java.io.IOException {
        final byte[] h = new byte[8192];
        int got = 0;
        try (java.io.InputStream is = java.nio.file.Files.newInputStream(file)) {
            int n;
            while (got < h.length && (n = is.read(h, got, h.length - got)) > 0) got += n;
        }
        int off = 0;
        while (off + 8 <= got) {
            final long size = ((h[off] & 0xffL) << 24) | ((h[off + 1] & 0xffL) << 16)
                    | ((h[off + 2] & 0xffL) << 8) | (h[off + 3] & 0xffL);
            final String type = new String(h, off + 4, 4, StandardCharsets.ISO_8859_1);
            if ("sidx".equals(type)) return new int[]{off, (int) size};
            if (size <= 0 || size > 100_000_000L) break;
            off += (int) size;
        }
        return null;
    }

    private static final String YOUTUBE_COOKIES = loadCookies();

    private static String loadCookies() {
        String p = System.getenv("YOUTUBE_COOKIES_FILE");
        if (p == null || p.isEmpty()) p = "/app/youtube-cookies.txt";
        java.io.File f = new java.io.File(p);
        if (!f.exists()) return null;
        StringBuilder sb = new StringBuilder();
        try (java.io.BufferedReader r = new java.io.BufferedReader(new java.io.FileReader(f))) {
            String line;
            while ((line = r.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;
                String[] parts = line.split("\t");
                if (parts.length < 7) continue;
                if (sb.length() > 0) sb.append("; ");
                sb.append(parts[5]).append("=").append(parts[6]);
            }
        } catch (Exception e) { return null; }
        return sb.length() == 0 ? null : sb.toString();
    }

    private static final ConcurrentMap<String, CacheEntry> streamsCache = new ConcurrentHashMap<>();
    // ReentrantLock instead of `synchronized (streamsCache)` for the resolve
    // section: synchronized + blocking I/O PINS the virtual-thread carrier
    // (Java 21), so a stuck YT resolve holding it starves carriers. A
    // ReentrantLock lets the vthread unmount during the blocking resolve.
    // Still a single global lock (serializes resolves = avoids duplicate YT
    // hits for the same video + caps concurrent YT load) — just de-pinned.
    private static final java.util.concurrent.locks.ReentrantLock resolveLock =
            new java.util.concurrent.locks.ReentrantLock();
    // THE SPLIT (2026-06-06): cache the expensive resolve (poToken + player-
    // response + nsig + signed URLs, valid ~5h via the `expire` param) for a long
    // TTL, but stamp a FRESH cpn into every served segment URL per play
    // (swapCpn() in streamPlaylist). This is what makes a long TTL safe: the
    // 2026-05-29 5min-bump outage reused the SAME cpn across plays → googlevideo
    // single-use-cpn throttle → resolve-hang. The cpn is NOT in the URL's signed
    // params (sparams), so swapping it keeps the signature valid (verified
    // 2026-06-06: swapped-cpn fetch returns 206). Fresh cpn per play = single-use
    // = no reuse-throttle. Result: the master HITs this cache (no 1.4s re-resolve
    // = the cold-tap win) while segment URLs stay throttle-safe.
    // Env-overridable for instant revert without a rebuild (set 10000 to disable).
    private static final long CACHE_TTL_MS = envLong("SYNTH_RESOLVE_TTL_MS", 300_000L);

    private static long envLong(String k, long def) {
        try { String v = System.getenv(k); return v == null || v.isEmpty() ? def : Long.parseLong(v.trim()); }
        catch (Exception e) { return def; }
    }

    // swapCpn replaces the cpn= tracking nonce in a googlevideo URL with a fresh
    // 16-char one, so a long-cached (cpn-stamped) URL becomes single-use per play.
    // cpn is not signature-covered (see THE SPLIT above), so this is safe.
    private static final java.security.SecureRandom CPN_RNG = new java.security.SecureRandom();
    private static final char[] CPN_ALPHABET =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_".toCharArray();
    static String freshCpn() {
        char[] c = new char[16];
        for (int i = 0; i < 16; i++) c[i] = CPN_ALPHABET[CPN_RNG.nextInt(CPN_ALPHABET.length)];
        return new String(c);
    }
    static String swapCpn(String url) {
        if (url == null) return null;
        // cpn is 16 URL-safe chars; replace the value, keep everything else.
        return url.replaceAll("([?&]cpn=)[A-Za-z0-9_-]{16}", "$1" + freshCpn());
    }

    private static class CacheEntry {
        final Streams streams;
        final long createdAt;
        final boolean urlsVerified;   // throttle-checked (+ WebEmbed-upgraded if needed)
        CacheEntry(Streams s, boolean verified) {
            this.streams = s; this.createdAt = System.currentTimeMillis(); this.urlsVerified = verified;
        }
        boolean fresh() { return System.currentTimeMillis() - createdAt < CACHE_TTL_MS; }
    }

    /// Resolve-Reuse: let StreamHandlers seed this cache from its /streams
    /// resolve, so a follow-up /synth-hls/<id>/master build reuses it (cache
    /// hit, ~ms) instead of a second YouTube resolve (~1.2s + extra IP-block
    /// risk). StreamHandlers already runs the throttle-check + WebEmbed retry,
    /// so its Streams are URL-verified (pass verified=true). Same short TTL —
    /// only the common scroll-prefetch → tap window benefits; longer gaps just
    /// re-resolve as before.
    /// Cold-tap Lever #1 (2026-06-27): expose a fresh, URL-verified cached
    /// resolve so the /streams tap can reuse what a prior /streams?light
    /// prefetch already resolved, instead of a full StreamInfo.getInfo
    /// re-resolve. Returns null on miss/stale/unverified.
    public static Streams getFreshVerifiedStreams(String videoId) {
        CacheEntry e = streamsCache.get(videoId);
        return (e != null && e.fresh() && e.urlsVerified) ? e.streams : null;
    }

    /// Serve-stale (backlog #6, built 2026-07-05): the last known-good resolve,
    /// PAST its freshness TTL but with googlevideo URLs still signature-valid
    /// (the `expire` param covers ~6h; the cache TTL is only minutes). Used as a
    /// fallback when a re-resolve fails during a transient throttle wave —
    /// serving a stale-but-valid resolve beats surfacing "Video nicht
    /// verfügbar". Entries stay in the map until overwritten, so this works
    /// long after fresh() lapses. cpn freshness is handled downstream per play
    /// (swapCpn), so an old resolve is throttle-safe to serve. Returns null if
    /// there's no entry or its URLs are within 120s of expiry.
    public static Streams getStaleServableStreams(String videoId) {
        CacheEntry e = streamsCache.get(videoId);
        if (e == null || e.streams == null) return null;
        Streams s = e.streams;
        if (s.videoStreams == null || s.videoStreams.isEmpty()
                || s.audioStreams == null || s.audioStreams.isEmpty()) return null;
        long exp = urlExpireEpoch(s.videoStreams.get(0).url);
        if (exp <= 0 || exp < System.currentTimeMillis() / 1000 + 120) return null;
        return s;
    }

    private static final java.util.regex.Pattern EXPIRE_RE =
            java.util.regex.Pattern.compile("[?&]expire=(\\d+)");

    /// The googlevideo `expire` epoch of a stream URL (signature validity), or 0.
    private static long urlExpireEpoch(String url) {
        if (url == null) return 0;
        var m = EXPIRE_RE.matcher(url);
        if (!m.find()) return 0;
        try { return Long.parseLong(m.group(1)); } catch (NumberFormatException e) { return 0; }
    }

    public static void cacheStreams(String videoId, Streams s, boolean urlsVerified) {
        cacheStreams(videoId, s, urlsVerified, false, 1080, DEFAULT_VIDEO_CODECS);
    }

    public static void cacheStreams(String videoId, Streams s, boolean urlsVerified, boolean syncWarm) {
        cacheStreams(videoId, s, urlsVerified, syncWarm, 1080, DEFAULT_VIDEO_CODECS);
    }

    // maxH/codecs select WHICH rendition to warm — must match what the tap will
    // request on the synth-hls URL, else the prewarm warms the wrong rendition and
    // the tap's variant-serve goes cold (the cold-tap regression). The app sends the
    // same ?maxh=&codecs= on /streams?light and the synth-hls URL.
    public static void cacheStreams(String videoId, Streams s, boolean urlsVerified, boolean syncWarm,
                                    int maxH, String[] codecs) {
        if (videoId != null && s != null
                && s.videoStreams != null && !s.videoStreams.isEmpty()
                && s.audioStreams != null && !s.audioStreams.isEmpty()) {
            streamsCache.put(videoId, new CacheEntry(s, urlsVerified));
            // Lever #2: warm the sidx index for the streams mpv opens at tap
            // (audio + video0) DURING this prefetch, so the variant serve at tap is a
            // SidxParserJava cache HIT instead of a ~270ms googlevideo round-trip each.
            // syncWarm=true (the /streams?light prefetch path): BLOCK until the sidx is
            // cached, so the prefetch returns only once the tap is guaranteed a HIT.
            // The warm runs OFF the cold-tap critical path (it's the prefetch), and the
            // async fire-and-forget variant lost the race under fast light-prefetch
            // (tap arrived before the warm finished). syncWarm=false keeps the old
            // async behavior for non-prefetch callers.
            if (urlsVerified) warmSidx(s, syncWarm, maxH, codecs);
        }
    }

    // Bounded, daemon: caps concurrent sidx warm-fetches so a broad feed prefetch
    // can't spawn unbounded threads; excess queues and runs as slots free.
    private static final java.util.concurrent.ExecutorService SIDX_WARM_POOL =
            java.util.concurrent.Executors.newFixedThreadPool(4, r -> {
                Thread t = new Thread(r, "sidx-warm");
                t.setDaemon(true);
                return t;
            });

    /// Pre-fetch the sidx index (cpn-independent, cached) for the audio + first
    /// video variant — exactly the two playlists mpv opens on a cold tap. The
    /// cache key is cpn-stripped, so warming with the resolve's own URL produces
    /// the same key the tap-time swapCpn'd fetch looks up → HIT.
    private static void warmSidx(Streams s, boolean sync, int maxH, String[] codecs) {
        try {
            final java.util.List<java.util.concurrent.Future<?>> fs = new ArrayList<>();
            List<PipedStream> videos = pickedVideoStreams(s, maxH, codecs);
            if (!videos.isEmpty()) { var f = warmOne(videos.get(0)); if (f != null) fs.add(f); }
            var fa = warmOne(pickedAudioStream(s)); if (fa != null) fs.add(fa);
            if (sync) {
                // Block until both sidx fetches are cached (audio + video0 run on the
                // pool concurrently). Bounded so a hung googlevideo can't stall the
                // prefetch forever; SidxParser has its own timeout+retry underneath.
                for (var f : fs) {
                    try { f.get(4, java.util.concurrent.TimeUnit.SECONDS); }
                    catch (Exception ignored) { /* best-effort; tap falls back to cold fetch */ }
                }
            }
        } catch (Exception ignored) { /* best-effort */ }
    }

    private static java.util.concurrent.Future<?> warmOne(PipedStream stream) {
        if (stream == null || stream.url == null) return null;
        if (!(stream.indexStart > 0 && stream.indexEnd > stream.indexStart)) return null;
        final String url = stream.url;
        final int is = stream.indexStart, ie = stream.indexEnd;
        return SIDX_WARM_POOL.submit(() -> {
            try { SidxParserJava.fetch(url, is, ie, null); }
            catch (Throwable ignored) { /* best-effort warm */ }
            // Lever #2 (prefetch-time, 2026-06-27): also kick the first media
            // segment's download into the yt-proxy disk cache NOW. The variant-
            // build prewarm fires only ~200ms before AVPlayer's first segment
            // request -- too late to finish a 1-2MB segment, so the cold tap ate
            // a ~887ms googlevideo round-trip. Running it here (audio + video0, on
            // this bounded pool, seconds ahead during the scroll/foreground
            // prefetch) makes that first fetch a disk HIT (~250ms).
            try { prewarmFirstSegment(stream); }
            catch (Throwable ignored) { /* best-effort warm */ }
        });
    }

    /// Lever #2 helper: parse a piped-proxy stream URL into the yt-proxy
    /// (host, path, query) and start the cached first-chunk download. Mirrors
    /// the extraction in rewriteToYtProxy, minus the URL-rewrite return.
    /// Idempotent (prewarm early-returns if the .mp4 already exists) and
    /// non-blocking (the download runs on the yt-proxy's own thread).
    private static void prewarmFirstSegment(PipedStream stream) {
        if (stream == null || stream.url == null) return;
        String url = stream.url;
        int q = url.indexOf('?');
        if (q < 0) return;
        String query = url.substring(q + 1);
        String host = null;
        for (String pair : query.split("&")) {
            if (pair.startsWith("host=")) { host = pair.substring(5); break; }
        }
        if (host == null) return;
        int pathStart = url.indexOf('/', 8);
        String path = (pathStart >= 0 && pathStart < q) ? url.substring(pathStart, q) : "/";
        String rest = java.util.Arrays.stream(query.split("&"))
                .filter(p -> !p.startsWith("host=")).collect(java.util.stream.Collectors.joining("&"));
        YtProxyHandlers.prewarm(host, path, rest);
    }

    /// Default to verified URLs — used by the segment-serving paths.
    private static Streams fetchStreams(String videoId) throws Exception {
        return fetchStreams(videoId, true);
    }

    /// requireVerified=false (master playlist) skips the throttle HEAD-probe +
    /// WebEmbed re-resolve. The master lists only variant names, so no segment
    /// URL is ever served from an unverified entry. requireVerified=true
    /// (variant/audio) guarantees the throttle check ran before segment URLs go
    /// out. A master fetch that just resolved+cached the streams lets the
    /// follow-up variant fetch verify from cache WITHOUT a second
    /// StreamInfo.getInfo (only the cheap HEAD; WebEmbed only if actually 403'd).
    private static Streams fetchStreams(String videoId, boolean requireVerified) throws Exception {
        CacheEntry e = streamsCache.get(videoId);
        if (e != null && e.fresh() && (!requireVerified || e.urlsVerified)) return e.streams;
        resolveLock.lock();
        // Stage timing for the cold path — split resolve vs throttle-HEAD vs
        // WebEmbed-retry over real plays, so we can see where a slow cold-start
        // actually goes (the Pi back-end vs the client player pipeline). One
        // [SynthHls-timing] line per cache-miss/verify; pure cache hits are
        // silent (they returned above).
        long t0 = System.currentTimeMillis();
        boolean didResolve = false, didThrottleCheck = false, throttled = false;
        boolean webembedTried = false, webembedOk = false;
        long resolveMs = 0, throttleMs = 0, webembedMs = 0;
        try {
            e = streamsCache.get(videoId);
            if (e != null && e.fresh() && (!requireVerified || e.urlsVerified)) return e.streams;

            // Reuse fresh-but-unverified streams (just cached by a master fetch)
            // so we don't pay a second StreamInfo.getInfo just to verify.
            Streams s;
            if (e != null && e.fresh()) {
                s = e.streams;
            } else {
                long r0 = System.currentTimeMillis();
                try {
                    s = resolveStreams(videoId);
                } catch (Exception rex) {
                    // Serve-stale net: re-resolve dead (throttle wave), but the
                    // previous resolve's URLs are still signature-valid → serve
                    // those instead of erroring. Cache entry is NOT re-stamped,
                    // so the next request tries a real resolve again.
                    Streams stale = getStaleServableStreams(videoId);
                    if (stale != null) {
                        System.out.println("[SynthHls] " + videoId + " resolve failed ("
                                + rex.getClass().getSimpleName() + ") -> serving STALE resolve (urls valid)");
                        return stale;
                    }
                    throw rex;
                }
                resolveMs = System.currentTimeMillis() - r0;
                didResolve = true;
            }

            boolean verified = false;
            if (requireVerified) {
                // Throttle-Check: ANDROID-URLs werden von googlevideo per-IP-Pattern
                // fuer deep byte-range-fetches 403'd. HEAD-Test gegen clen/2 deckt das
                // auf -- bei 403 retry mit force-WebEmbed (= WebEmbed-modern-URLs werden
                // nicht so throttled). Siehe StreamHandlers fuer Detail-Notes.
                didThrottleCheck = true;
                long th0 = System.currentTimeMillis();
                throttled = isStreamsThrottled(s);
                throttleMs = System.currentTimeMillis() - th0;
                if (throttled) {
                    System.out.println("[SynthHls] " + videoId + " URLs throttled (HEAD=403 auf clen/2), retry mit force-WebEmbed");
                    webembedTried = true;
                    long w0 = System.currentTimeMillis();
                    Streams retryS = resolveStreamsWebEmbed(videoId);
                    webembedMs = System.currentTimeMillis() - w0;
                    if (retryS != null && !retryS.audioStreams.isEmpty() && !retryS.videoStreams.isEmpty()) {
                        s = retryS;
                        webembedOk = true;
                        System.out.println("[SynthHls] " + videoId + " WebEmbed-retry success");
                    } else {
                        System.out.println("[SynthHls] " + videoId + " WebEmbed-retry non-healthy, behalte original");
                    }
                }
                verified = true;
            }
            streamsCache.put(videoId, new CacheEntry(s, verified));
            return s;
        } finally {
            if (didResolve || didThrottleCheck) {
                System.out.println("[SynthHls-timing] " + videoId
                        + " kind=" + (requireVerified ? "variant" : "master")
                        + " resolveMs=" + resolveMs
                        + " throttled=" + throttled + " throttleMs=" + throttleMs
                        + " webembed=" + webembedTried + " webembedOk=" + webembedOk + " webembedMs=" + webembedMs
                        + " totalMs=" + (System.currentTimeMillis() - t0));
            }
            resolveLock.unlock();
        }
    }

    /// Resolve StreamInfo via NewPipe, retrying up to 3x on a degraded result
    /// (0 video or 0 audio — happens non-deterministically). Wrapped in
    /// Multithreading.supplyAsync to match StreamHandlers' threading context
    /// (a direct getInfo() can return degraded streams).
    private static Streams resolveStreams(String videoId) throws Exception {
        try {
            return resolveStreamsInner(videoId);
        } catch (Exception ex) {
            if (me.kavin.piped.utils.EgressManager.isSignInBlock(ex)
                    && me.kavin.piped.utils.EgressManager.flipOnBotFlag()) {
                System.out.println("[SynthHls] " + videoId + " sign-in-blocked -> egress-flip to "
                        + me.kavin.piped.utils.EgressManager.activeLabel() + ", retry");
                return resolveStreamsInner(videoId);
            }
            // Transient resolve failure (innertube timeout/IO during a googlevideo
            // throttle wave). Waves pass within seconds — observed 2026-07-05: one
            // attempt timed out at 10s, the next landed in 2s. One paused retry
            // turns "Video nicht verfügbar" into a slightly slower play. Only ONE
            // retry: the fetchStreams caller has a serve-stale net behind this.
            System.out.println("[SynthHls] " + videoId + " resolve failed ("
                    + ex.getClass().getSimpleName() + ") -> one-shot retry in 2s");
            Thread.sleep(2000);
            return resolveStreamsInner(videoId);
        }
    }

    private static Streams resolveStreamsInner(String videoId) throws Exception {
        Streams s = null;
        for (int attempt = 0; attempt < 3; attempt++) {
            StreamInfo info = Multithreading.supplyAsync(() -> {
                try { return StreamInfo.getInfo("https://www.youtube.com/watch?v=" + videoId); }
                catch (Exception ex) { throw new RuntimeException(ex); }
            }).get();
            s = CollectionUtils.collectStreamInfo(info);
            if (!s.audioStreams.isEmpty() && !s.videoStreams.isEmpty()) break;
            // audio=0 is the persistent OER pattern (the Android client never
            // returns adaptive audio for these uploads); retrying NewPipe never
            // recovers it — only the WebEmbed fallback below does. Break out now
            // instead of burning 2 more attempts (~2.5s): those wasted retries
            // pushed consistently-audio=0 channels past the app's request timeout
            // ("Server antwortet nicht"). The video=0 case below IS transient, so
            // keep retrying that one.
            if (s.audioStreams.isEmpty()) break;
            System.out.println("[SynthHls] " + videoId + " attempt " + (attempt + 1) + " degraded (v=" + s.videoStreams.size() + " a=" + s.audioStreams.size() + "), retrying");
        }
        // Persistent audio=0 (e.g. ARD/WDR-OER uploads like "Die Maus") — the
        // Android client never returns adaptive audio; web_embedded does. Fall
        // back like the throttle path does. Verified per yt-dlp: android=0 audio,
        // web_embedded=4 incl. m4a.
        if (s != null && s.audioStreams.isEmpty()) {
            System.out.println("[SynthHls] " + videoId + " persistent degraded (audio=0), force-WebEmbed fallback");
            Streams retryS = resolveStreamsWebEmbed(videoId);
            if (retryS != null && !retryS.audioStreams.isEmpty() && !retryS.videoStreams.isEmpty()) {
                s = retryS;
                System.out.println("[SynthHls] " + videoId + " WebEmbed-fallback success (a=" + s.audioStreams.size() + " v=" + s.videoStreams.size() + ")");
            }
        }
        return s;
    }

    /// Re-resolve with force-WebEmbed (modern URLs that googlevideo throttles
    /// less). Returns null on failure — caller keeps the original streams.
    private static Streams resolveStreamsWebEmbed(String videoId) {
        try {
            StreamInfo retryInfo = Multithreading.supplyAsync(() -> {
                // ThreadLocal muss INSIDE des Lambdas gesetzt werden -- supplyAsync
                // laeuft auf Worker-Thread, ThreadLocals propagieren nicht ueber den
                // Thread-Pool-Hop hinweg.
                YoutubeStreamExtractor.FORCE_WEB_EMBED_FOR_THREAD.set(Boolean.TRUE);
                try {
                    return StreamInfo.getInfo("https://www.youtube.com/watch?v=" + videoId);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                } finally {
                    YoutubeStreamExtractor.FORCE_WEB_EMBED_FOR_THREAD.remove();
                }
            }).get();
            return CollectionUtils.collectStreamInfo(retryInfo);
        } catch (Exception ex) {
            System.out.println("[SynthHls] " + videoId + " WebEmbed-retry failed: " + ex.getMessage());
            return null;
        }
    }

    // Default: single H.264 1080p-or-less variant (the safe default that avoids
    // ABR-switching confusion + works on every device). Higher renditions are
    // opt-in via ?maxh=&codecs= (the app sends only HW-decodable codecs).
    /** VIDEO-RANGE for an av01 codec string (transfer-characteristics field). */
    private static String hlsVideoRange(String codec) {
        if (codec == null || !codec.startsWith("av01")) return "SDR";
        String[] f = codec.split("\\.");
        if (f.length >= 8) {
            if ("16".equals(f[7])) return "PQ";
            if ("18".equals(f[7])) return "HLG";
        }
        return "SDR";
    }

    static final String[] DEFAULT_VIDEO_CODECS = {"avc"};

    private static List<PipedStream> pickedVideoStreams(Streams streams) {
        return pickedVideoStreams(streams, 1080, DEFAULT_VIDEO_CODECS);
    }

    // Pick ONE videoOnly rendition (single-variant — preserves the cold-tap
    // prewarm): the highest <= maxH within the FIRST preferred codec that has any
    // matching rendition (codec preference beats resolution, per the app contract).
    private static List<PipedStream> pickedVideoStreams(Streams streams, int maxH, String[] codecPref) {
        List<PipedStream> out = new ArrayList<>();
        for (String pref : codecPref) {
            PipedStream best = null;
            for (PipedStream s : streams.videoStreams) {
                if (s.codec == null || !codecMatches(s.codec, pref)) continue;
                int h = s.height;
                if (h <= 0 || h > maxH) continue;
                if (best == null || h > best.height || (h == best.height && s.bitrate > best.bitrate)) best = s;
            }
            if (best != null) { out.add(best); return out; }
        }
        return out;
    }

    private static boolean codecMatches(String codec, String pref) {
        switch (pref) {
            case "av1": return codec.startsWith("av01") || codec.startsWith("av1");
            case "vp9": return codec.startsWith("vp9")  || codec.startsWith("vp09");
            case "avc": case "h264": return codec.startsWith("avc");
            default: return false;
        }
    }

    // ?maxh= — cap requested rendition height; default 1080 (today's behavior).
    public static int parseMaxH(String s) {
        if (s == null || s.isEmpty()) return 1080;
        try { int v = Integer.parseInt(s.trim()); return v > 0 ? v : 1080; } catch (Exception e) { return 1080; }
    }

    // ?codecs=av1,vp9,avc — preference order; default {"avc"} (today's behavior).
    public static String[] parseCodecs(String s) {
        if (s == null || s.isEmpty()) return DEFAULT_VIDEO_CODECS;
        String[] parts = s.trim().toLowerCase().split(",");
        List<String> out = new ArrayList<>();
        for (String p : parts) { p = p.trim(); if (!p.isEmpty()) out.add(p); }
        return out.isEmpty() ? DEFAULT_VIDEO_CODECS : out.toArray(new String[0]);
    }

    private static PipedStream pickedAudioStream(Streams streams) {
        PipedStream best = null;
        for (PipedStream s : streams.audioStreams) {
            if (s.codec == null || !s.codec.startsWith("mp4a")) continue;
            if (s.audioTrackType != null && !s.audioTrackType.equals("ORIGINAL")) continue;
            if (best == null || s.bitrate > best.bitrate) best = s;
        }
        return best;
    }

    private static String rewriteToYtProxy(String pipedProxyUrl) {
        int q = pipedProxyUrl.indexOf('?');
        if (q < 0) return pipedProxyUrl;
        String query = pipedProxyUrl.substring(q + 1);
        String host = null;
        for (String pair : query.split("&")) {
            if (pair.startsWith("host=")) { host = pair.substring(5); break; }
        }
        if (host == null) return pipedProxyUrl;
        int pathStart = pipedProxyUrl.indexOf('/', 8);
        String path = (pathStart >= 0 && pathStart < q) ? pipedProxyUrl.substring(pathStart, q) : "/";
        // Route segments through the caching yt-proxy (chunked Range
        // downloader = full speed). On a 403 (degraded URL) the yt-proxy
        // 302-redirects back to this piped-proxy URL, so no hard fail.
        String rest = java.util.Arrays.stream(query.split("&"))
                .filter(p -> !p.startsWith("host=")).collect(java.util.stream.Collectors.joining("&"));
        // Start the first-chunk download now (variant build is ~200ms before
        // mpv asks for the first segment) so it is a cache HIT, not a ~850ms
        // synchronous googlevideo pull. Single fetch - the yt-proxy caches.
        YtProxyHandlers.prewarm(host, path, rest);
        return "/yt-proxy/" + host + path
                + (rest.isEmpty() ? "" : "?" + rest);
    }

    /**
     * HEAD-byte-range-Check fuer Throttle-Detection — auf dem tatsaechlich
     * servierten Video-Stream (pickedVideoStreams). Vorher StreamInfo-basiert;
     * jetzt auf den collected Streams, damit der Check aus dem Cache heraus
     * laufen kann, ohne StreamInfo erneut aufzuloesen.
     */
    private static boolean isStreamsThrottled(Streams s) {
        if (s == null) return false;
        List<PipedStream> vids = pickedVideoStreams(s);
        if (vids.isEmpty()) return false;
        PipedStream v = vids.get(0);
        String url = v.url;
        if (url == null || url.isEmpty() || !url.startsWith("http")) return false;
        long clen = v.contentLength;
        if (clen < 10_000_000L) return false;
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setRequestMethod("HEAD");
            long offset = clen / 2;
            conn.setRequestProperty("Range", "bytes=" + offset + "-" + (offset + 1000));
            conn.setConnectTimeout(2000);
            conn.setReadTimeout(3000);
            int code = conn.getResponseCode();
            conn.disconnect();
            return code == 403;
        } catch (Exception e) {
            return false;
        }
    }

}
