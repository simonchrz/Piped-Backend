package me.kavin.piped.server.handlers;

import io.activej.http.HttpHeaderValue;
import io.activej.http.HttpHeaders;
import io.activej.http.HttpMethod;
import io.activej.http.HttpRequest;
import io.activej.http.HttpResponse;
import io.activej.bytebuf.ByteBuf;
import io.activej.csp.ChannelSupplier;
import io.activej.promise.Promise;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/// HTTP proxy for YouTube googlevideo.com URLs with single-connection-per-cpn
/// streaming cache.
///
/// CORRECTION (2026-06-09): the **Strategy** paragraph below is STALE/WRONG.
/// The actual downloader (startDownloader) uses sequential BOUNDED Range
/// chunks (DL_CHUNK_BYTES, same cpn) -- a no-Range full GET is the
/// anti-download-throttled path (~31 KB/s), Range is full speed (verified
/// 13.4 MB/s through this handler). See memory googlevideo_throttle_noRange_not_cpn.
/// The text from here to '// Also handles' describes a design that is no
/// longer in use; trust the code, not this paragraph.
///
/// **Strategy**: one open-ended request to googlevideo per (videoId, itag).
/// googlevideo streams the entire file in a single 200 OK TCP connection
/// (no Range header → no per-cpn rate-limit triggered). The downloader thread
/// writes bytes to a temp file as they arrive. Range requests block on a
/// monitor until the requested offset has been downloaded, then read from the
/// file. Subsequent watches / seeks within the file: instant from disk.
///
/// **Why not per-range cache**: previous implementation cached `videoId_itag_range`.
/// Worked for replays of the SAME range, but every NEW range request hit
/// googlevideo and triggered the per-cpn 403 throttle on the 2nd-3rd chunk.
/// Streaming-per-cpn means a single allowed-from-googlevideo connection ever.
///
/// Also handles:
/// - HEAD → wait for upstream headers, return Content-Length.
/// - GET no-Range → wait for upstream headers, return full file (or headers).
/// - Chrome UA + YouTube cookies for upstream auth.
public class YtProxyHandlers {

    private static final String PREFIX = "/yt-proxy/";
    private static final String CHROME_UA =
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

    private static final String YOUTUBE_COOKIES = loadCookies();
    private static final Path CACHE_DIR = ensureCacheDir();
    private static final long MAX_CACHE_BYTES = 16L * 1024 * 1024 * 1024;  // 16 GB
    private static final long HEADER_WAIT_MS = 15_000;
    private static final long DOWNLOAD_WAIT_MS = 60_000;
    private static final int READ_BUF = 256 * 1024;
    /// activej builds the entire HTTP response body in memory before returning,
    /// so we cap any single 206 to 10 MiB. mpv/ffmpeg follow up with the next
    /// chunk's Range request as needed — same pattern the old chunk-rewrite
    /// path used, just now backed by a single upstream connection per cpn.
    private static final long MAX_RESPONSE_CHUNK = 10L * 1024 * 1024;
    /// Seek fast-path threshold. The downloader fills the file sequentially from
    /// offset 0; a Range request normally blocks until the fill reaches it. For
    /// a forward seek whose target is more than this many bytes AHEAD of the
    /// current fill, blocking would take seconds (16s+ for a mid-point seek on a
    /// long video), so instead fetch that range DIRECTLY from googlevideo
    /// (bounded Range = ~1.4 MB/s, ~1s) and return at once. The sequential
    /// downloader keeps running to complete the full-file cache for next time.
    private static final long SEEK_AHEAD_MARGIN = 16L * 1024 * 1024;

    /// Bounded read-ahead: the downloader pauses once it is this far AHEAD of the
    /// client's furthest requested byte, instead of eagerly racing to the end of the
    /// file. Stops a video the user has moved on from (mpv no longer requesting)
    /// from hogging Pi bandwidth and starving the next tap's first segment.
    private static final long READAHEAD_MARGIN_BYTES = 12L * 1024 * 1024;
    /// If the client requests nothing for this long while the downloader is paused
    /// at the read-ahead cap, treat the stream as abandoned (mpv switched videos)
    /// and abort the downloader entirely.
    private static final long ABANDON_IDLE_MS = 10_000;

    /// (videoId + "_" + itag) → active StreamSession.
    private static final ConcurrentHashMap<String, StreamSession> SESSIONS = new ConcurrentHashMap<>();

    // First-chunk cache hit/miss counters (a range already downloaded when
    // the request arrived = HIT; one that had to block on the in-progress
    // download = MISS = the populate-timing race the variant-build prewarm
    // targets). Logged per request with the running rate — server-side
    // ground truth, unbiased by app-side throttle.
    private static final AtomicLong ytCacheHits = new AtomicLong(0);
    private static final AtomicLong ytCacheMisses = new AtomicLong(0);
    private static final AtomicLong ytSeekFetches = new AtomicLong(0);

    static {
        // Cleanup orphan .tmp files from previous run (server restart mid-download).
        if (CACHE_DIR != null) {
            try (var stream = Files.list(CACHE_DIR)) {
                for (Path p : (Iterable<Path>) stream::iterator) {
                    if (p.getFileName().toString().endsWith(".tmp")) {
                        try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                    }
                }
            } catch (IOException ignored) {}
        }
    }

    /// Pre-warm the cache for a (videoId,itag): create the StreamSession +
    /// start the chunked-range download NOW, so by the time mpv requests the
    /// first segment the leading bytes are already on disk (cache HIT ~250ms
    /// instead of a ~850ms synchronous googlevideo pull). Called from the
    /// synth-hls variant build, which runs ~200ms before mpv's first segment
    /// request. Idempotent; returns at once, download runs in the background.
    /// Safe vs the old piped-proxy warmup double-fetch concern: the yt-proxy
    /// CACHES, so this is a single fetch.
    public static void prewarm(String host, String path, String rawQuery) {
        if (host == null || host.isEmpty()) return;
        String targetUrl = "https://" + host + path
                + (rawQuery == null || rawQuery.isEmpty() ? "" : "?" + rawQuery);
        String videoId = "_", itag = "_";
        if (rawQuery != null) {
            for (String p : rawQuery.split("&")) {
                if (p.startsWith("id=")) videoId = p.substring(3);
                else if (p.startsWith("itag=")) itag = p.substring(5);
            }
        }
        String key = safeKey(videoId + "_" + itag);
        if (Files.exists(CACHE_DIR.resolve(key + ".mp4"))) return;
        SESSIONS.computeIfAbsent(key, k -> {
            StreamSession sess = new StreamSession(k, CACHE_DIR.resolve(k + ".tmp"),
                    CACHE_DIR.resolve(k + ".mp4"), targetUrl);
            startDownloader(sess);
            return sess;
        });
    }

    // Streaming entry point for the GET /yt-proxy/ route. A large-range request
    // (AVPlayer wants the whole, often single, segment) is served as a FULL-range
    // stream from the filling disk cache, so the response length matches what was
    // asked (AVPlayer treats a short read as a fatal -12939). Everything else falls
    // back to handle()'s byte[] path. The ChannelSupplier MUST be created on the
    // eventloop, so the blocking prep runs via Promise.ofBlocking and the stream is
    // built in the .then callback (which runs on the eventloop).
    public static Promise<HttpResponse> handleAsync(HttpRequest request, java.util.concurrent.Executor executor) {
        return Promise.ofBlocking(executor, () -> prepareStream(request)).then(prep -> {
            if (prep != null && prep.sess != null) {
                final StreamSession sess = prep.sess;
                final long fStart = prep.start, fEnd = prep.end, fTotal = prep.total;
                final long SLICE = 4L * 1024 * 1024;
                final java.util.concurrent.atomic.AtomicLong posRef =
                        new java.util.concurrent.atomic.AtomicLong(prep.start);
                ChannelSupplier<ByteBuf> body = ChannelSupplier.ofSupplier(() ->
                    Promise.ofBlocking(executor, () -> {
                        long p = posRef.get();
                        if (p > fEnd) return null; // end of stream
                        long sliceEnd = Math.min(p + SLICE - 1, fEnd);
                        sess.noteClientRequest(sliceEnd);
                        byte[] slice;
                        try {
                            sess.waitUntilDownloaded(sliceEnd, DOWNLOAD_WAIT_MS);
                            slice = readRange(currentBytesPath(sess), p, sliceEnd);
                        } catch (Exception e) {
                            byte[] direct = directRangeFetch(swapCpn(sess.targetUrl), p, sliceEnd);
                            if (direct == null)
                                throw new IOException("yt-proxy stream stall " + p + "-" + sliceEnd);
                            slice = direct;
                        }
                        posRef.set(sliceEnd + 1);
                        return ByteBuf.wrapForReading(slice);
                    }));
                return Promise.of(HttpResponse.ofCode(206).withBodyStream(body)
                        .withHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValue.of("video/mp4"))
                        .withHeader(HttpHeaders.CONTENT_RANGE, HttpHeaderValue.of("bytes " + fStart + "-" + fEnd + "/" + fTotal))
                        .withHeader(HttpHeaders.CONTENT_LENGTH, HttpHeaderValue.of(String.valueOf(fEnd - fStart + 1)))
                        .withHeader(HttpHeaders.ACCEPT_RANGES, HttpHeaderValue.of("bytes")));
            }
            return Promise.ofBlocking(executor, () -> {
                try {
                    return handle(request);
                } catch (Exception e) {
                    return HttpResponse.ofCode(500).withBody(("yt-proxy: " + e.getMessage()).getBytes());
                }
            });
        });
    }

    private static final class StreamPrep {
        final StreamSession sess;
        final long start, end, total;
        StreamPrep(StreamSession sess, long start, long end, long total) {
            this.sess = sess; this.start = start; this.end = end; this.total = total;
        }
    }

    // Blocking prep: returns a StreamPrep only for a non-cached, large-range GET
    // (the case that needs full-range streaming); null otherwise (handle() takes it).
    private static StreamPrep prepareStream(HttpRequest request) {
        try {
            String fullPath = request.getPath();
            if (!fullPath.startsWith(PREFIX)) return null;
            if (request.getMethod() == HttpMethod.HEAD) return null;
            String reqRange = request.getHeader(HttpHeaders.RANGE);
            if (reqRange == null || reqRange.isEmpty()) return null;
            String afterPrefix = fullPath.substring(PREFIX.length());
            int firstSlash = afterPrefix.indexOf('/');
            if (firstSlash < 0) return null;
            String host = afterPrefix.substring(0, firstSlash);
            String path = afterPrefix.substring(firstSlash);
            String rawQuery = request.getQuery();
            String targetUrl = "https://" + host + path
                    + (rawQuery == null || rawQuery.isEmpty() ? "" : "?" + rawQuery);
            Map<String, String> qp = request.getQueryParameters();
            String key = safeKey(qp.getOrDefault("id", "_") + "_" + qp.getOrDefault("itag", "_"));
            Path finalPath = CACHE_DIR.resolve(key + ".mp4");
            if (Files.exists(finalPath)) return null; // fully cached -> handle() serves full range
            StreamSession sess = SESSIONS.computeIfAbsent(key, k -> {
                Path tmpPath = CACHE_DIR.resolve(k + ".tmp");
                StreamSession ses = new StreamSession(k, tmpPath, finalPath, targetUrl);
                startDownloader(ses);
                return ses;
            });
            sess.waitUntilHeadersReady(HEADER_WAIT_MS);
            if (sess.failed) return null;
            long total = sess.totalLength;
            long[] rb = parseRange(reqRange, total);
            if (rb == null) return null;
            long start = rb[0];
            long reqEnd = Math.min(rb[1], total - 1);
            if (reqEnd - start + 1 <= MAX_RESPONSE_CHUNK) return null; // small -> capped path
            System.out.printf("[YtProxy] %s STREAM-FULL %d-%d (%dMB)%n",
                    key, start, reqEnd, (reqEnd - start + 1) / (1024 * 1024));
            return new StreamPrep(sess, start, reqEnd, total);
        } catch (Exception e) {
            return null; // fall back to handle()
        }
    }

    public static HttpResponse handle(HttpRequest request) throws IOException {
        String fullPath = request.getPath();
        if (!fullPath.startsWith(PREFIX)) return HttpResponse.ofCode(404);
        String afterPrefix = fullPath.substring(PREFIX.length());
        int firstSlash = afterPrefix.indexOf('/');
        if (firstSlash < 0) return HttpResponse.ofCode(404);
        String host = afterPrefix.substring(0, firstSlash);
        String path = afterPrefix.substring(firstSlash);

        // Use the raw query string verbatim — googlevideo's signature validation
        // is sensitive to parameter order (Java Map reordering broke it with 403).
        String rawQuery = request.getQuery();
        String targetUrl = "https://" + host + path
                + (rawQuery == null || rawQuery.isEmpty() ? "" : "?" + rawQuery);

        Map<String, String> qp = request.getQueryParameters();
        String videoId = qp.getOrDefault("id", "_");
        String itag = qp.getOrDefault("itag", "_");
        String key = safeKey(videoId + "_" + itag);
        boolean isHead = request.getMethod() == HttpMethod.HEAD;
        String reqRange = request.getHeader(HttpHeaders.RANGE);
        boolean noRange = reqRange == null || reqRange.isEmpty();

        // Cache hit on completed file
        Path finalPath = CACHE_DIR.resolve(key + ".mp4");
        if (Files.exists(finalPath)) {
            try {
                Files.setLastModifiedTime(finalPath, java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis()));
            } catch (IOException ignored) {}
            long total = Files.size(finalPath);
            return serveFromFile(finalPath, total, reqRange, noRange, isHead);
        }

        // Get-or-create session (atomic; concurrent first requests share one downloader)
        StreamSession sess = SESSIONS.computeIfAbsent(key, k -> {
            Path tmpPath = CACHE_DIR.resolve(k + ".tmp");
            StreamSession s = new StreamSession(k, tmpPath, finalPath, targetUrl);
            startDownloader(s);
            return s;
        });

        try {
            sess.waitUntilHeadersReady(HEADER_WAIT_MS);
        } catch (Exception e) {
            if (sess.upstream403) return ytProxyFallbackRedirect(host, path, rawQuery);
            return HttpResponse.ofCode(504).withBody(("yt-proxy: header wait: " + e.getMessage()).getBytes());
        }
        if (sess.failed) {
            if (sess.upstream403) return ytProxyFallbackRedirect(host, path, rawQuery);
            return HttpResponse.ofCode(502).withBody("yt-proxy: upstream failed".getBytes());
        }
        long total = sess.totalLength;

        // No-range / HEAD: return file or headers
        if (noRange) {
            HttpResponse resp = HttpResponse.ofCode(isHead ? 200 : 200)
                    .withHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValue.of("video/mp4"))
                    .withHeader(HttpHeaders.CONTENT_LENGTH, HttpHeaderValue.of(String.valueOf(total)))
                    .withHeader(HttpHeaders.ACCEPT_RANGES, HttpHeaderValue.of("bytes"));
            if (isHead) return resp;
            try { sess.waitUntilDownloaded(total - 1, DOWNLOAD_WAIT_MS); }
            catch (Exception e) { return HttpResponse.ofCode(504).withBody(("yt-proxy: full download: " + e.getMessage()).getBytes()); }
            byte[] body = Files.readAllBytes(currentBytesPath(sess));
            return resp.withBody(body);
        }

        // Range GET — cap response chunk to keep memory bounded + return fast.
        long[] rb = parseRange(reqRange, total);
        if (rb == null) return HttpResponse.ofCode(416);
        long start = rb[0];
        long end = Math.min(rb[1], Math.min(start + MAX_RESPONSE_CHUNK - 1, total - 1));
        boolean hit = Files.exists(sess.finalPath) || sess.downloadedBytes.get() >= end + 1;

        // Seek fast-path: a forward seek to bytes the sequential downloader won't
        // reach for a while → fetch this range directly instead of blocking on
        // the fill (which is 16s+ for a mid-point seek). Defensive: only on a
        // clean 206; any failure falls through to the normal blocking path.
        long dl0 = sess.downloadedBytes.get();
        if (!hit && start > dl0 + SEEK_AHEAD_MARGIN) {
            byte[] direct = directRangeFetch(sess.targetUrl, start, end);
            if (direct != null) {
                long n = ytSeekFetches.incrementAndGet();
                System.out.printf("[YtProxy] %s SEEK-FETCH %d-%d (fill@%d, %dMB ahead, #%d)%n",
                        key, start, end, dl0, (start - dl0) / (1024 * 1024), n);
                return HttpResponse.ofCode(206).withBody(direct)
                        .withHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValue.of("video/mp4"))
                        .withHeader(HttpHeaders.CONTENT_RANGE, HttpHeaderValue.of("bytes " + start + "-" + end + "/" + total))
                        .withHeader(HttpHeaders.CONTENT_LENGTH, HttpHeaderValue.of(String.valueOf(end - start + 1)))
                        .withHeader(HttpHeaders.ACCEPT_RANGES, HttpHeaderValue.of("bytes"));
            }
            // direct fetch failed → fall through to the blocking path below.
        }

        // Record the client's interest so the downloader keeps ~READAHEAD_MARGIN ahead
        // of here (and doesn't abort this as abandoned while we're actively pulling).
        sess.noteClientRequest(end);
        long waitStart = System.nanoTime();
        try { sess.waitUntilDownloaded(end, DOWNLOAD_WAIT_MS); }
        catch (Exception e) {
            // The sequential downloader gave up (CDN stalled on this range — common
            // on audio=0/WebEmbed segments). Don't freeze the client with a 504:
            // serve THIS range from a one-shot fresh connection + fresh cpn (the same
            // cushion the seek fast-path uses). Recovers a transient googlevideo stall
            // without the client having to fully reload the video.
            byte[] direct = directRangeFetch(swapCpn(sess.targetUrl), start, end);
            if (direct != null) {
                long n = ytSeekFetches.incrementAndGet();
                System.out.println("[YtProxy] " + key + " STALL-RECOVER direct " + start + "-" + end
                        + " (downloader gave up; #" + n + ")");
                return HttpResponse.ofCode(206).withBody(direct)
                        .withHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValue.of("video/mp4"))
                        .withHeader(HttpHeaders.CONTENT_RANGE, HttpHeaderValue.of("bytes " + start + "-" + end + "/" + total))
                        .withHeader(HttpHeaders.CONTENT_LENGTH, HttpHeaderValue.of(String.valueOf(end - start + 1)))
                        .withHeader(HttpHeaders.ACCEPT_RANGES, HttpHeaderValue.of("bytes"));
            }
            return HttpResponse.ofCode(504).withBody(("yt-proxy: range wait: " + e.getMessage()).getBytes());
        }
        long waitMs = (System.nanoTime() - waitStart) / 1_000_000;
        (hit ? ytCacheHits : ytCacheMisses).incrementAndGet();
        long tot = ytCacheHits.get() + ytCacheMisses.get();
        System.out.printf("[YtProxy] %s range %d-%d %s wait=%dms (hit-rate %d/%d = %.0f%%)%n",
                key, start, end, hit ? "HIT" : "MISS", waitMs,
                ytCacheHits.get(), tot, 100.0 * ytCacheHits.get() / Math.max(1, tot));

        byte[] body = readRange(currentBytesPath(sess), start, end);
        return HttpResponse.ofCode(206).withBody(body)
                .withHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValue.of("video/mp4"))
                .withHeader(HttpHeaders.CONTENT_RANGE, HttpHeaderValue.of("bytes " + start + "-" + end + "/" + total))
                .withHeader(HttpHeaders.CONTENT_LENGTH, HttpHeaderValue.of(String.valueOf(end - start + 1)))
                .withHeader(HttpHeaders.ACCEPT_RANGES, HttpHeaderValue.of("bytes"));
    }

    // On a googlevideo 403 (degraded/blocked URL: old/long videos whose
    // ANDROID_VR resolve yields legacy progressive itags that 403 byte-range
    // fetches), redirect the player to the piped-proxy streaming path instead
    // of hard-failing. piped-proxy forwards the player request shape, which
    // googlevideo throttles-but-serves rather than 403s. Reconstructs the
    // pre-rewrite piped-proxy URL (host moves back from path to query param).
    private static HttpResponse ytProxyFallbackRedirect(String host, String path, String rawQuery) {
        String fb = me.kavin.piped.consts.Constants.PROXY_PART + path + "?"
                + (rawQuery == null || rawQuery.isEmpty() ? "" : rawQuery + "&") + "host=" + host;
        System.out.println("[YtProxy] 403 fallback -> piped-proxy " + host + path);
        return HttpResponse.ofCode(302).withHeader(HttpHeaders.LOCATION, HttpHeaderValue.of(fb));
    }

    private static Path currentBytesPath(StreamSession sess) {
        // Once renamed to finalPath, downloader is done. Before that, read from tmp.
        return Files.exists(sess.finalPath) ? sess.finalPath : sess.tmpPath;
    }

    private static HttpResponse serveFromFile(Path file, long total, String reqRange, boolean noRange, boolean isHead) throws IOException {
        if (noRange) {
            HttpResponse resp = HttpResponse.ofCode(200)
                    .withHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValue.of("video/mp4"))
                    .withHeader(HttpHeaders.CONTENT_LENGTH, HttpHeaderValue.of(String.valueOf(total)))
                    .withHeader(HttpHeaders.ACCEPT_RANGES, HttpHeaderValue.of("bytes"));
            if (isHead) return resp;
            return resp.withBody(Files.readAllBytes(file));
        }
        long[] rb = parseRange(reqRange, total);
        if (rb == null) return HttpResponse.ofCode(416);
        long start = rb[0], end = rb[1];
        byte[] body = readRange(file, start, end);
        return HttpResponse.ofCode(206).withBody(body)
                .withHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValue.of("video/mp4"))
                .withHeader(HttpHeaders.CONTENT_RANGE, HttpHeaderValue.of("bytes " + start + "-" + end + "/" + total))
                .withHeader(HttpHeaders.CONTENT_LENGTH, HttpHeaderValue.of(String.valueOf(end - start + 1)))
                .withHeader(HttpHeaders.ACCEPT_RANGES, HttpHeaderValue.of("bytes"));
    }

    private static byte[] readRange(Path file, long start, long end) throws IOException {
        int len = (int) (end - start + 1);
        byte[] buf = new byte[len];
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {
            raf.seek(start);
            raf.readFully(buf);
        }
        return buf;
    }

    /// Direct bounded Range GET from googlevideo for [start,end] — the seek
    /// fast-path. googlevideo serves bounded ranges at full speed (~1.4 MB/s, no
    /// per-cpn 403 on current ANDROID_VR URLs — verified), so this returns a
    /// forward-seek segment in ~1s instead of blocking until the sequential
    /// downloader fills there (16s+ on a long video). Returns null on ANY
    /// failure (non-206, short read, exception) → caller falls back to the
    /// blocking path. Does NOT write the shared cache file (no writer
    /// concurrency with the downloader, which still completes the full cache).
    private static byte[] directRangeFetch(String targetUrl, long start, long end) {
        int len = (int) (end - start + 1);
        if (len <= 0 || len > MAX_RESPONSE_CHUNK) return null;
        try {
            java.util.Map<String, String> rh = new java.util.HashMap<>();
            rh.put("Range", "bytes=" + start + "-" + end);
            rh.put("User-Agent", CHROME_UA);
            if (YOUTUBE_COOKIES != null) rh.put("Cookie", YOUTUBE_COOKIES);
            // Egress the SAME family the resolve used (v4-locked URLs must be
            // fetched from v4); raw HttpURLConnection obeyed JVM preferIPv6.
            rocks.kavin.reqwest4j.Response resp = rocks.kavin.reqwest4j.ReqwestUtils.fetchWithProxy(
                    targetUrl, "GET", new byte[0], rh, me.kavin.piped.utils.EgressManager.activeEgress())
                    .get(40_000, java.util.concurrent.TimeUnit.MILLISECONDS);
            if (resp.status() != 206) return null;
            byte[] body = resp.body();
            return body != null && body.length == len ? body : null;
        } catch (Exception e) {
            return null;
        }
    }

    private static final long DL_CHUNK_BYTES = 8L * 1024 * 1024; // bounded range chunk

    // Segment-fetch stall-proofing (analogous to the sidx fix). The read timeout
    // bounds socket INACTIVITY, not transfer time (read() returns as soon as any
    // bytes arrive), so a slow-but-flowing stream is fine — only a true stall (no
    // bytes for ~2.5s) trips it. On a stall, retry with a fresh cpn + new
    // connection: a transient googlevideo range-stall (saw 5-15s cold-tap hangs)
    // is usually one slow edge; a new attempt routes around it. cpn-agnostic
    // cache key + verified-safe fresh-cpn segments => no throttle risk.
    private static final int SEG_CONNECT_TIMEOUT_MS = 2000;
    private static final int SEG_READ_TIMEOUT_MS = 2500;
    private static final int SEG_MAX_STALLS = 3; // consecutive zero-progress attempts before giving up
    private static final java.security.SecureRandom SEG_CPN_RNG = new java.security.SecureRandom();
    private static final char[] SEG_CPN_ALPHA =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_".toCharArray();

    private static String swapCpn(String url) {
        if (url == null) return null;
        char[] c = new char[16];
        for (int i = 0; i < 16; i++) c[i] = SEG_CPN_ALPHA[SEG_CPN_RNG.nextInt(SEG_CPN_ALPHA.length)];
        return url.replaceAll("([?&]cpn=)[A-Za-z0-9_-]{16}", "$1" + new String(c));
    }

    private static void startDownloader(StreamSession sess) {
        Thread t = new Thread(() -> {
            try (RandomAccessFile raf = new RandomAccessFile(sess.tmpPath.toFile(), "rw")) {
                raf.setLength(0);
                long offset = 0;
                long total = -1;
                long lastNotify = 0;
                System.out.println("[YtProxy] " + sess.key + " GET(range) "
                        + sess.targetUrl.substring(0, Math.min(160, sess.targetUrl.length())) + "...");
                // Fill the file via sequential BOUNDED range requests. googlevideo
                // serves bounded ranges at full speed; a no-Range full GET is
                // throttled to ~31 KB/s (anti-download). Range requests on the same
                // cpn are NOT rate-limited (verified). See project memory
                // googlevideo_throttle_noRange_not_cpn.
                int consecutiveStalls = 0;
                while (total < 0 || offset < total) {
                    // Bounded read-ahead: pause once we're READAHEAD_MARGIN past the
                    // client's furthest request; resume when it advances. If the client
                    // goes idle while paused (mpv switched to another video), abort so
                    // this abandoned stream stops hogging bandwidth for the next tap.
                    synchronized (sess.lock) {
                        while (offset > sess.lastRequestedEnd + READAHEAD_MARGIN_BYTES) {
                            long idle = System.currentTimeMillis() - sess.lastRequestTimeMs;
                            if (idle > ABANDON_IDLE_MS) {
                                System.out.println("[YtProxy] " + sess.key + " abandoned (client idle "
                                        + idle + "ms, fill@" + offset + "/" + total + ") — abort read-ahead");
                                sess.failed = true;
                                sess.lock.notifyAll();
                                SESSIONS.remove(sess.key, sess);
                                try { Files.deleteIfExists(sess.tmpPath); } catch (Exception ignored) {}
                                return;
                            }
                            // Fixed 1s poll: re-check idle every second. (NOT
                            // wait(ABANDON_IDLE_MS-idle) — that lands on wait(0) at the
                            // boundary, which blocks forever with no client to notify.)
                            try { sess.lock.wait(1000); }
                            catch (InterruptedException ie) { return; }
                        }
                    }
                    long chunkEnd = (total < 0) ? offset + DL_CHUNK_BYTES - 1
                                                : Math.min(offset + DL_CHUNK_BYTES - 1, total - 1);
                    final long before = offset;
                    // First try the original cpn; on a stall, fresh cpn + new connection.
                    final String url = consecutiveStalls == 0 ? sess.targetUrl : swapCpn(sess.targetUrl);
                    try {
                        java.util.Map<String, String> rh = new java.util.HashMap<>();
                        rh.put("Range", "bytes=" + offset + "-" + chunkEnd);
                        rh.put("User-Agent", CHROME_UA);
                        if (YOUTUBE_COOKIES != null) rh.put("Cookie", YOUTUBE_COOKIES);
                        // Family-pinned (activeEgress): v4-locked URLs must be pulled
                        // from v4. Raw HttpURLConnection obeyed JVM preferIPv6 -> v6 ->
                        // 403 on the subset of videos whose googlevideo server has AAAA.
                        rocks.kavin.reqwest4j.Response resp = rocks.kavin.reqwest4j.ReqwestUtils.fetchWithProxy(
                                url, "GET", new byte[0], rh, me.kavin.piped.utils.EgressManager.activeEgress())
                                .get(SEG_CONNECT_TIMEOUT_MS + SEG_READ_TIMEOUT_MS + 10_000L,
                                        java.util.concurrent.TimeUnit.MILLISECONDS);
                        int code = resp.status();
                        if (code == 403) {
                            // Mid-stream 403 = a throttle storm hit the active egress
                            // family AFTER resolve. Nudge the reactive autoflip so the
                            // NEXT resolve/tap egresses on the clean family instead of
                            // every subsequent video also landing on the flagged one
                            // (the resolve path flips on bot-flag; the streaming path
                            // never did). flipOnBotFlag() only flips when the OTHER
                            // family probes clean, so a both-family storm is a no-op.
                            // This session still 302-falls-back to the client (same
                            // public IP as the box -> the ip-signed URL stays valid).
                            boolean flipped = me.kavin.piped.utils.EgressManager.flipOnBotFlag();
                            System.out.println("[YtProxy] " + sess.key + " upstream HTTP 403 (egress="
                                    + me.kavin.piped.utils.EgressManager.activeLabel() + ") -> 302 fallback"
                                    + (flipped ? " + egress-flipped" : ""));
                            sess.upstream403 = true;
                            fail(sess);
                            return;
                        }
                        if (code != 206 && code != 200) {
                            System.out.println("[YtProxy] " + sess.key + " upstream HTTP " + code
                                    + " (stall " + (consecutiveStalls + 1) + "/" + SEG_MAX_STALLS + ")");
                        } else {
                            if (total < 0) {
                                total = parseTotalFromHeaders(resp.headers(), resp.body() == null ? 0 : resp.body().length);
                                if (total < 0) { fail(sess); return; }
                                synchronized (sess.lock) {
                                    sess.totalLength = total;
                                    sess.lock.notifyAll();
                                }
                            }
                            byte[] body = resp.body();
                            if (body != null && body.length > 0) {
                                raf.write(body);
                                offset += body.length;
                                lastNotify = sess.downloadedBytes.addAndGet(body.length);
                                synchronized (sess.lock) { sess.lock.notifyAll(); }
                            }
                            if (code == 200) break;
                        }
                    } catch (Exception e) {
                        System.out.println("[YtProxy] " + sess.key + " range stall at offset " + offset
                                + " (" + e.getClass().getSimpleName() + ", stall "
                                + (consecutiveStalls + 1) + "/" + SEG_MAX_STALLS + ")");
                    }
                    // Progress resets the stall budget; pure no-progress attempts count
                    // down. A moving stream never hits the cap (bounded ~2.5s per stall).
                    if (offset > before) {
                        consecutiveStalls = 0;
                    } else if (++consecutiveStalls >= SEG_MAX_STALLS) {
                        System.out.println("[YtProxy] " + sess.key + " gave up after " + SEG_MAX_STALLS
                                + " stalls at offset " + offset);
                        fail(sess);
                        return;
                    }
                }
                Files.move(sess.tmpPath, sess.finalPath, StandardCopyOption.REPLACE_EXISTING);
                synchronized (sess.lock) {
                    sess.complete = true;
                    sess.lock.notifyAll();
                }
                System.out.println("[YtProxy] " + sess.key + " complete " + sess.totalLength + " bytes");
                SESSIONS.remove(sess.key);
                maybeEvict();
            } catch (Exception e) {
                System.out.println("[YtProxy] " + sess.key + " downloader failed: " + e.getMessage());
                fail(sess);
            }
        }, "yt-proxy-stream-" + sess.key);
        t.setDaemon(true);
        t.start();
        sess.downloader = t;
    }

    /// Total file size from the first chunk: Content-Range "bytes s-e/TOTAL"
    /// (206), else Content-Length (200 = server ignored Range).
    private static long parseTotalFromHeaders(java.util.Map<String, String> h, int bodyLen) {
        String cr = h.get("content-range");
        if (cr == null) cr = h.get("Content-Range");
        if (cr != null) {
            int slash = cr.lastIndexOf('/');
            if (slash >= 0) {
                try { return Long.parseLong(cr.substring(slash + 1).trim()); } catch (Exception ignored) {}
            }
        }
        String cl = h.get("content-length");
        if (cl == null) cl = h.get("Content-Length");
        if (cl != null) {
            try { return Long.parseLong(cl.trim()); } catch (Exception ignored) {}
        }
        return bodyLen > 0 ? bodyLen : -1;
    }

    private static long parseUpstreamTotal(HttpURLConnection conn) {
        String cr = conn.getHeaderField("Content-Range");
        if (cr != null) {
            int slash = cr.lastIndexOf('/');
            if (slash >= 0) {
                try { return Long.parseLong(cr.substring(slash + 1).trim()); } catch (Exception ignored) {}
            }
        }
        return conn.getHeaderFieldLong("Content-Length", -1);
    }

    private static void fail(StreamSession sess) {
        synchronized (sess.lock) {
            sess.failed = true;
            sess.lock.notifyAll();
        }
        SESSIONS.remove(sess.key);
        try { Files.deleteIfExists(sess.tmpPath); } catch (IOException ignored) {}
    }

    private static class StreamSession {
        final String key;
        final Path tmpPath;
        final Path finalPath;
        final String targetUrl;
        volatile long totalLength = -1;
        volatile boolean failed = false;
        volatile boolean complete = false;
        volatile boolean upstream403 = false;
        final AtomicLong downloadedBytes = new AtomicLong(0);
        final Object lock = new Object();
        volatile Thread downloader;
        // Furthest byte the client has requested + when it last asked — drives the
        // bounded read-ahead + abandoned-abort in the downloader loop.
        volatile long lastRequestedEnd = 0;
        volatile long lastRequestTimeMs;

        StreamSession(String key, Path tmpPath, Path finalPath, String targetUrl) {
            this.key = key;
            this.tmpPath = tmpPath;
            this.finalPath = finalPath;
            this.targetUrl = targetUrl;
            this.lastRequestTimeMs = System.currentTimeMillis();
        }

        // Called on each client range request: advance the read-ahead target and
        // wake the (possibly paused) downloader so it keeps ~READAHEAD_MARGIN ahead.
        void noteClientRequest(long endByte) {
            synchronized (lock) {
                if (endByte > lastRequestedEnd) lastRequestedEnd = endByte;
                lastRequestTimeMs = System.currentTimeMillis();
                lock.notifyAll();
            }
        }

        void waitUntilHeadersReady(long timeoutMs) throws InterruptedException, IOException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            synchronized (lock) {
                while (!failed && totalLength < 0 && !complete) {
                    long rem = deadline - System.currentTimeMillis();
                    if (rem <= 0) throw new IOException("header timeout");
                    lock.wait(Math.min(rem, 1000));
                }
            }
            if (failed) throw new IOException("session failed before headers");
        }

        void waitUntilDownloaded(long endByte, long timeoutMs) throws InterruptedException, IOException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            synchronized (lock) {
                while (!failed && !complete && downloadedBytes.get() <= endByte) {
                    long rem = deadline - System.currentTimeMillis();
                    if (rem <= 0) throw new IOException("download timeout at " + downloadedBytes.get() + " < " + (endByte + 1));
                    lock.wait(Math.min(rem, 1000));
                }
            }
            if (failed) throw new IOException("session failed during download");
        }
    }

    // ---- Helpers ----

    private static String safeKey(String s) {
        return s.replaceAll("[^A-Za-z0-9._-]", "_");
    }

    private static long[] parseRange(String r, long total) {
        if (r == null || !r.startsWith("bytes=")) return null;
        try {
            String body = r.substring(6);
            int dash = body.indexOf('-');
            if (dash < 0) return null;
            String startStr = body.substring(0, dash).trim();
            String endStr = body.substring(dash + 1).trim();
            long start = startStr.isEmpty() ? 0 : Long.parseLong(startStr);
            long end = endStr.isEmpty() ? total - 1 : Long.parseLong(endStr);
            if (end >= total) end = total - 1;
            if (start < 0 || start > end) return null;
            return new long[]{start, end};
        } catch (Exception e) { return null; }
    }

    private static long lastEvictCheck = 0;
    private static synchronized void maybeEvict() {
        long now = System.currentTimeMillis();
        if (now - lastEvictCheck < 30_000) return;
        lastEvictCheck = now;
        try {
            long total = 0;
            java.util.List<Path> files = new java.util.ArrayList<>();
            try (var stream = Files.list(CACHE_DIR)) {
                for (Path p : (Iterable<Path>) stream::iterator) {
                    if (p.getFileName().toString().endsWith(".mp4")) files.add(p);
                }
            }
            for (Path p : files) total += Files.size(p);
            if (total <= MAX_CACHE_BYTES) return;
            files.sort((a, b) -> {
                try {
                    return Long.compare(Files.getLastModifiedTime(a).toMillis(),
                            Files.getLastModifiedTime(b).toMillis());
                } catch (IOException e) { return 0; }
            });
            for (Path p : files) {
                if (total <= MAX_CACHE_BYTES * 8 / 10) break;
                try {
                    long sz = Files.size(p);
                    Files.delete(p);
                    total -= sz;
                } catch (IOException ignored) {}
            }
        } catch (IOException ignored) {}
    }

    private static Path ensureCacheDir() {
        String envDir = System.getenv("YT_PROXY_CACHE_DIR");
        Path dir = Paths.get(envDir != null && !envDir.isEmpty() ? envDir : "/app/yt-proxy-cache");
        try {
            Files.createDirectories(dir);
            return dir;
        } catch (IOException e) {
            System.out.println("[YtProxy] cache dir setup failed: " + e.getMessage());
            return null;
        }
    }

    private static String loadCookies() {
        String path = System.getenv("YOUTUBE_COOKIES_FILE");
        if (path == null || path.isEmpty()) path = "/app/youtube-cookies.txt";
        java.io.File f = new java.io.File(path);
        if (!f.exists()) return null;
        StringBuilder sb = new StringBuilder();
        try (java.io.BufferedReader r = new java.io.BufferedReader(new java.io.FileReader(f))) {
            String line;
            while ((line = r.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;
                String[] parts = line.split("\t");
                if (parts.length < 7) continue;
                // ⚠️ NUR youtube.com-Cookies. Ein Export aus dem Browser enthaelt
                // ALLE Domains (gemessen: 258 Cookies, davon 25 fuer YouTube).
                // Schickt man alles zusammen, antwortet Google mit 302 auf
                // accounts.google.com/CookieMismatch — wir waren dadurch NIE
                // angemeldet, obwohl die Datei gueltige Login-Cookies enthielt.
                // Mit dem Filter: HTTP 200 und "LOGGED_IN":true.
                if (!parts[0].contains("youtube.com")) continue;
                if (sb.length() > 0) sb.append("; ");
                sb.append(parts[5]).append("=").append(parts[6]);
            }
        } catch (Exception e) { return null; }
        return sb.length() == 0 ? null : sb.toString();
    }
}
