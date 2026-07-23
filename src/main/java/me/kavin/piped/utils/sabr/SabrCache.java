package me.kavin.piped.utils.sabr;

import io.activej.http.HttpHeaderValue;
import io.activej.http.HttpHeaders;
import io.activej.http.HttpResponse;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
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

    // ── storm-fallback marks ────────────────────────────────────────────────
    // videoIds whose direct googlevideo URLs are 403-storming (WebEmbed AND
    // TVHTML5 dead); synth-hls serves them via /sabr instead — stage 4 of the
    // resolve chain (StreamHandlers). TTL'd so a video returns to the normal
    // /yt-proxy path once the transient storm has passed.
    private static final ConcurrentHashMap<String, Long> STORM_MARKS = new ConcurrentHashMap<>();
    private static final long STORM_TTL_MS = 30 * 60_000L;

    public static void markStorm(String videoId) {
        STORM_MARKS.put(videoId, System.currentTimeMillis() + STORM_TTL_MS);
    }

    public static boolean isStormMarked(String videoId) {
        final Long exp = STORM_MARKS.get(videoId);
        if (exp == null) return false;
        if (exp < System.currentTimeMillis()) {
            STORM_MARKS.remove(videoId);
            return false;
        }
        return true;
    }

    /// Ensures the video has been SABR-downloaded (once, per-videoId lock) and
    /// returns the cached file for the itag, or null if unavailable. Lets the
    /// synth-hls layer read the file directly (box scan) without an HTTP hop.
    public static Path ensureFile(String videoId, int itag) throws Exception {
        final Path file = DIR.resolve(safe(videoId) + "_" + itag + ".bin");
        if (!Files.exists(file)) {
            synchronized (LOCKS.computeIfAbsent(videoId, k -> new Object())) {
                // anyFileFor guard: if the session already ran but produced
                // DIFFERENT itags (video without 1080p avc), a request for the
                // absent itag must not re-trigger the whole download forever.
                if (!Files.exists(file) && !anyFileFor(videoId)) {
                    download(videoId);
                }
            }
        }
        return Files.exists(file) ? file : null;
    }

    /// The itags the SABR session ACTUALLY picked for this video, as
    /// {audio, video}. Ensures the download ran (once); reads the manifest
    /// download() writes next to the media files. Falls back to {140, 137}
    /// (the preferred picks) for pre-manifest cache entries.
    public static int[] itagsFor(String videoId) throws Exception {
        final Path manifest = DIR.resolve(safe(videoId) + ".itags");
        if (!Files.exists(manifest)) {
            synchronized (LOCKS.computeIfAbsent(videoId, k -> new Object())) {
                if (!Files.exists(manifest) && !anyFileFor(videoId)) {
                    download(videoId);
                }
            }
        }
        if (Files.exists(manifest)) {
            final String[] parts = Files.readString(manifest).trim().split("\\s+");
            if (parts.length == 2) {
                return new int[]{Integer.parseInt(parts[0]), Integer.parseInt(parts[1])};
            }
        }
        return new int[]{140, 137};
    }

    private static boolean anyFileFor(String videoId) {
        try (var s = Files.newDirectoryStream(DIR, safe(videoId) + "_*.bin")) {
            return s.iterator().hasNext();
        } catch (IOException e) {
            return false;
        }
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
        // Attempt 1 on the global ACTIVE egress family. If the session dies with
        // SABR_ERROR or produced nothing (googlevideo hard-403 on that family —
        // seen 2026-07-23 on made-for-kids content during a storm), retry ONCE on
        // the other family: resolve + session are family-pinned together (gvs
        // URLs are IP-signed). Whichever attempt wrote more survives (keep-larger
        // publish), so a refill can never regress an existing partial cache.
        final String fam1 = me.kavin.piped.utils.EgressManager.activeEgress();
        SabrHandlers.SabrMedia result = attempt(videoId, fam1);
        if (result == null || (!result.complete()
                && ("SABR_ERROR".equals(result.stopReason()) || result.segments() == 0))) {
            final String fam2 = me.kavin.piped.utils.EgressManager.otherFamily();
            if (!fam2.equals(fam1)) {
                System.out.println("[SabrCache] " + videoId + " attempt on " + fam1
                        + (result == null ? " threw" : " " + result.stopReason()
                        + " (segs=" + result.segments() + ")")
                        + " -> family-retry on " + fam2);
                final SabrHandlers.SabrMedia r2 = attempt(videoId, fam2);
                if (r2 != null && (result == null || r2.segments() > result.segments())) {
                    result = r2;
                    System.out.println("[SabrCache] " + videoId + " family-retry " + fam2
                            + " won (segs=" + r2.segments() + " complete=" + r2.complete() + ")");
                }
            }
        }
        if (result == null)
            throw new IllegalStateException("sabr: both family attempts failed for " + videoId);
        // Manifest with the ACTUAL picked itags so the serving layer doesn't
        // have to guess (videos without 1080p avc don't yield 137).
        Files.writeString(DIR.resolve(safe(videoId) + ".itags"),
                result.audioItag() + " " + result.videoItag());
        maybeEvict();
    }

    /// One SABR download attempt on an explicit egress family: streams each
    /// format to a fresh .part (Sink), then publishes .part -> .bin — but only
    /// when the .part is LARGER than any existing .bin, so a storm-crippled
    /// attempt (or refill) can't overwrite a better earlier cache. Returns the
    /// session result, or null when the session threw before finishing.
    private static SabrHandlers.SabrMedia attempt(String videoId, String family) {
        final Map<Integer, Path> parts = new ConcurrentHashMap<>();
        final Map<Integer, OutputStream> opened = new ConcurrentHashMap<>();
        final SabrSession.Sink sink = itag -> {
            final Path part = DIR.resolve(safe(videoId) + "_" + itag + ".part");
            Files.deleteIfExists(part);
            final OutputStream os = new BufferedOutputStream(Files.newOutputStream(part), 1 << 20);
            parts.put(itag, part);
            opened.put(itag, os);
            return os;
        };
        SabrHandlers.SabrMedia result = null;
        try {
            result = SabrHandlers.runSession(videoId, sink, family);
        } catch (Exception e) {
            System.out.println("[SabrCache] " + videoId + " attempt(" + family + ") threw: " + e.getMessage());
        } finally {
            // the session closes the streams it was handed; this is defensive for
            // the error path (runSession throws before the session's finally runs).
            for (OutputStream os : opened.values()) { try { os.close(); } catch (IOException ignored) {} }
        }
        for (Map.Entry<Integer, Path> e : parts.entrySet()) {
            final Path part = e.getValue();
            final Path bin = DIR.resolve(safe(videoId) + "_" + e.getKey() + ".bin");
            try {
                final long partSize = Files.exists(part) ? Files.size(part) : 0;
                final long binSize = Files.exists(bin) ? Files.size(bin) : 0;
                if (partSize > binSize) {
                    Files.move(part, bin, StandardCopyOption.REPLACE_EXISTING);
                } else {
                    Files.deleteIfExists(part);
                }
            } catch (IOException ignored) {}
        }
        return result;
    }

    // ── refill (incomplete cache heal) ──────────────────────────────────────
    // The synth-hls playlist layer calls requestRefill when the sidx promises
    // more bytes than the cache file holds (= a storm truncated the download).
    // Async + per-video cooldown so playlist polls don't stack sessions; the
    // keep-larger publish in attempt() makes refills monotonic.
    private static final ConcurrentHashMap<String, Long> REFILL_LAST = new ConcurrentHashMap<>();
    private static final long REFILL_COOLDOWN_MS = 5 * 60_000L;

    public static void requestRefill(String videoId) {
        final long now = System.currentTimeMillis();
        final boolean[] go = {false};
        REFILL_LAST.compute(videoId, (k, prev) -> {
            if (prev != null && now - prev < REFILL_COOLDOWN_MS) return prev;
            go[0] = true;
            return now;
        });
        if (!go[0]) return;
        Thread.ofVirtual().name("sabr-refill-" + videoId).start(() -> {
            synchronized (LOCKS.computeIfAbsent(videoId, k -> new Object())) {
                try {
                    System.out.println("[SabrCache] " + videoId + " refill (incomplete cache)");
                    download(videoId);
                } catch (Exception e) {
                    System.out.println("[SabrCache] " + videoId + " refill failed: " + e.getMessage());
                }
            }
        });
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
                    final String n = p.getFileName().toString();
                    if (n.endsWith(".tmp") || n.endsWith(".part")) {   // orphaned partial downloads
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
