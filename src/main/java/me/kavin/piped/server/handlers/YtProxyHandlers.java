package me.kavin.piped.server.handlers;

import io.activej.http.HttpHeaderValue;
import io.activej.http.HttpHeaders;
import io.activej.http.HttpMethod;
import io.activej.http.HttpRequest;
import io.activej.http.HttpResponse;

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

    /// (videoId + "_" + itag) → active StreamSession.
    private static final ConcurrentHashMap<String, StreamSession> SESSIONS = new ConcurrentHashMap<>();

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
            return HttpResponse.ofCode(504).withBody(("yt-proxy: header wait: " + e.getMessage()).getBytes());
        }
        if (sess.failed) return HttpResponse.ofCode(502).withBody("yt-proxy: upstream failed".getBytes());
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
        try { sess.waitUntilDownloaded(end, DOWNLOAD_WAIT_MS); }
        catch (Exception e) { return HttpResponse.ofCode(504).withBody(("yt-proxy: range wait: " + e.getMessage()).getBytes()); }

        byte[] body = readRange(currentBytesPath(sess), start, end);
        return HttpResponse.ofCode(206).withBody(body)
                .withHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValue.of("video/mp4"))
                .withHeader(HttpHeaders.CONTENT_RANGE, HttpHeaderValue.of("bytes " + start + "-" + end + "/" + total))
                .withHeader(HttpHeaders.CONTENT_LENGTH, HttpHeaderValue.of(String.valueOf(end - start + 1)))
                .withHeader(HttpHeaders.ACCEPT_RANGES, HttpHeaderValue.of("bytes"));
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

    private static final long DL_CHUNK_BYTES = 8L * 1024 * 1024; // bounded range chunk

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
                while (total < 0 || offset < total) {
                    long chunkEnd = (total < 0) ? offset + DL_CHUNK_BYTES - 1
                                                : Math.min(offset + DL_CHUNK_BYTES - 1, total - 1);
                    HttpURLConnection conn = (HttpURLConnection) new URL(sess.targetUrl).openConnection();
                    try {
                        conn.setRequestMethod("GET");
                        conn.setInstanceFollowRedirects(true);
                        conn.setConnectTimeout(10_000);
                        conn.setReadTimeout(30_000);
                        conn.setRequestProperty("User-Agent", CHROME_UA);
                        if (YOUTUBE_COOKIES != null) conn.setRequestProperty("Cookie", YOUTUBE_COOKIES);
                        conn.setRequestProperty("Range", "bytes=" + offset + "-" + chunkEnd);
                        int code = conn.getResponseCode();
                        if (code != 206 && code != 200) {
                            String errBody = "";
                            try {
                                java.io.InputStream es = conn.getErrorStream();
                                if (es != null) errBody = new String(es.readAllBytes(), 0,
                                        Math.min(200, es.available() > 0 ? es.available() : 200));
                            } catch (Exception ignored) {}
                            System.out.println("[YtProxy] " + sess.key + " upstream HTTP " + code + " body=" + errBody);
                            fail(sess);
                            return;
                        }
                        if (total < 0) {
                            total = parseUpstreamTotal(conn);
                            if (total < 0) { fail(sess); return; }
                            synchronized (sess.lock) {
                                sess.totalLength = total;
                                sess.lock.notifyAll();
                            }
                        }
                        try (InputStream is = conn.getInputStream()) {
                            byte[] buf = new byte[READ_BUF];
                            int n;
                            while ((n = is.read(buf)) > 0) {
                                raf.write(buf, 0, n);
                                offset += n;
                                long now = sess.downloadedBytes.addAndGet(n);
                                if (now - lastNotify >= READ_BUF) {
                                    synchronized (sess.lock) { sess.lock.notifyAll(); }
                                    lastNotify = now;
                                }
                            }
                        }
                        if (code == 200) break; // server ignored Range, whole file already read
                    } finally {
                        conn.disconnect();
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
        final AtomicLong downloadedBytes = new AtomicLong(0);
        final Object lock = new Object();
        volatile Thread downloader;

        StreamSession(String key, Path tmpPath, Path finalPath, String targetUrl) {
            this.key = key;
            this.tmpPath = tmpPath;
            this.finalPath = finalPath;
            this.targetUrl = targetUrl;
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
                if (sb.length() > 0) sb.append("; ");
                sb.append(parts[5]).append("=").append(parts[6]);
            }
        } catch (Exception e) { return null; }
        return sb.length() == 0 ? null : sb.toString();
    }
}
