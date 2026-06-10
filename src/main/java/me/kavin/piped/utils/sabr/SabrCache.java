package me.kavin.piped.utils.sabr;

import io.activej.http.HttpHeaderValue;
import io.activej.http.HttpHeaders;
import io.activej.http.HttpResponse;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/// SABR serving layer (Baustein 4). Backs /sabr/<videoId>/<itag>: on first hit
/// for a videoId it runs ONE SabrSession (download-once), writes the reassembled
/// fmp4 per itag (audio 140 + video 137) to the cache dir, then range-serves the
/// file. The HLS/sidx layer can point at these byte-range-addressable files
/// unchanged. Concurrent audio+video requests for the same video share the one
/// download (per-videoId lock).
public final class SabrCache {

    private static final Path DIR = ensureDir();
    private static final long MAX_CACHE_BYTES = 8L * 1024 * 1024 * 1024; // 8 GB
    private static final ConcurrentHashMap<String, Object> LOCKS = new ConcurrentHashMap<>();

    /// Ensures the video has been SABR-downloaded (once, per-videoId lock) and
    /// returns the cached file for the itag, or null if unavailable. Lets the
    /// synth-hls layer read the file directly (box scan) without an HTTP hop.
    public static Path ensureFile(String videoId, int itag) throws Exception {
        final Path file = DIR.resolve(safe(videoId) + "_" + itag + ".bin");
        if (!Files.exists(file)) {
            synchronized (LOCKS.computeIfAbsent(videoId, k -> new Object())) {
                if (!Files.exists(file)) {
                    download(videoId);
                }
            }
        }
        return Files.exists(file) ? file : null;
    }

    public static HttpResponse handle(String videoId, int itag, String range, boolean head) throws Exception {
        final Path file = ensureFile(videoId, itag);
        if (file == null) {
            return HttpResponse.ofCode(502).withBody("sabr: no media for itag".getBytes());
        }
        try {
            Files.setLastModifiedTime(file, java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis()));
        } catch (IOException ignored) {}
        return serveFile(file, itag, range, head);
    }

    private static void download(String videoId) throws Exception {
        final Map<Integer, byte[]> media = SabrHandlers.runSession(videoId);
        for (Map.Entry<Integer, byte[]> e : media.entrySet()) {
            if (e.getValue() == null || e.getValue().length == 0) continue;
            final Path tmp = DIR.resolve(safe(videoId) + "_" + e.getKey() + ".tmp");
            Files.write(tmp, e.getValue());
            Files.move(tmp, DIR.resolve(safe(videoId) + "_" + e.getKey() + ".bin"),
                    StandardCopyOption.REPLACE_EXISTING);
        }
        maybeEvict();
    }

    private static HttpResponse serveFile(Path file, int itag, String range, boolean head) throws IOException {
        final long total = Files.size(file);
        final String ct = itag == 140 ? "audio/mp4" : "video/mp4";
        final boolean noRange = range == null || range.isEmpty();
        if (noRange) {
            final HttpResponse r = HttpResponse.ofCode(200)
                    .withHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValue.of(ct))
                    .withHeader(HttpHeaders.CONTENT_LENGTH, HttpHeaderValue.of(String.valueOf(total)))
                    .withHeader(HttpHeaders.ACCEPT_RANGES, HttpHeaderValue.of("bytes"));
            return head ? r : r.withBody(Files.readAllBytes(file));
        }
        final long[] se = parseRange(range, total);
        if (se == null) return HttpResponse.ofCode(416);
        final long start = se[0];
        final long end = se[1];
        final HttpResponse r = HttpResponse.ofCode(206)
                .withHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValue.of(ct))
                .withHeader(HttpHeaders.CONTENT_RANGE, HttpHeaderValue.of("bytes " + start + "-" + end + "/" + total))
                .withHeader(HttpHeaders.CONTENT_LENGTH, HttpHeaderValue.of(String.valueOf(end - start + 1)))
                .withHeader(HttpHeaders.ACCEPT_RANGES, HttpHeaderValue.of("bytes"));
        return head ? r : r.withBody(readRange(file, start, end));
    }

    private static long[] parseRange(String r, long total) {
        if (r == null || !r.startsWith("bytes=")) return null;
        try {
            final String body = r.substring(6);
            final int dash = body.indexOf('-');
            if (dash < 0) return null;
            final String s = body.substring(0, dash).trim();
            final String e = body.substring(dash + 1).trim();
            long start = s.isEmpty() ? 0 : Long.parseLong(s);
            long end = e.isEmpty() ? total - 1 : Long.parseLong(e);
            if (end >= total) end = total - 1;
            if (start < 0 || start > end) return null;
            return new long[]{start, end};
        } catch (Exception ex) {
            return null;
        }
    }

    private static byte[] readRange(Path file, long start, long end) throws IOException {
        final int len = (int) (end - start + 1);
        final byte[] buf = new byte[len];
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {
            raf.seek(start);
            raf.readFully(buf);
        }
        return buf;
    }

    private static long lastEvict = 0;
    private static synchronized void maybeEvict() {
        final long now = System.currentTimeMillis();
        if (now - lastEvict < 30_000) return;
        lastEvict = now;
        try {
            long total = 0;
            final List<Path> files = new ArrayList<>();
            try (var stream = Files.list(DIR)) {
                for (Path p : (Iterable<Path>) stream::iterator) {
                    if (p.getFileName().toString().endsWith(".bin")) files.add(p);
                }
            }
            for (Path p : files) total += Files.size(p);
            if (total <= MAX_CACHE_BYTES) return;
            files.sort((a, b) -> {
                try {
                    return Long.compare(Files.getLastModifiedTime(a).toMillis(),
                            Files.getLastModifiedTime(b).toMillis());
                } catch (IOException ex) {
                    return 0;
                }
            });
            for (Path p : files) {
                if (total <= MAX_CACHE_BYTES * 8 / 10) break;
                try {
                    final long sz = Files.size(p);
                    Files.delete(p);
                    total -= sz;
                } catch (IOException ignored) {}
            }
        } catch (IOException ignored) {}
    }

    private static String safe(String s) {
        return s.replaceAll("[^A-Za-z0-9._-]", "_");
    }

    private static Path ensureDir() {
        final String env = System.getenv("SABR_CACHE_DIR");
        final Path dir = Paths.get(env != null && !env.isEmpty() ? env : "/app/sabr-cache");
        try {
            Files.createDirectories(dir);
            try (var stream = Files.list(dir)) {
                for (Path p : (Iterable<Path>) stream::iterator) {
                    if (p.getFileName().toString().endsWith(".tmp")) {
                        try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("[SabrCache] dir setup failed: " + e.getMessage());
        }
        return dir;
    }
}
