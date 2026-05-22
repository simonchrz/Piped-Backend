package me.kavin.piped.utils;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Calls the kuckuck-nsig sidecar (POST http://nsig-decoder:4417/decrypt_n) to
 * deobfuscate YouTube's n-throttle query parameter. Cached + best-effort:
 * if the sidecar is down or returns an error, the original URL is returned
 * unchanged so the call site falls through to whatever degraded throughput
 * googlevideo serves up.
 *
 * Today our ANDROID_VR + ANDROID cascade returns n-free URLs, so this path
 * is dormant. It becomes load-bearing only if Google patches the Android
 * clients to also include the n-throttle parameter.
 *
 * Disabled when the NSIG_DECODER_URL env var is unset.
 */
public final class NSigClient {

    private static final String DECODER_URL = System.getenv("NSIG_DECODER_URL");
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(500))
            .build();
    private static final Pattern N_QUERY = Pattern.compile("([?&])n=([A-Za-z0-9_-]+)");
    /** Cache obfuscated -> decrypted to avoid repeat sidecar calls for the same n. */
    private static final ConcurrentMap<String, String> CACHE = new ConcurrentHashMap<>(1024);

    private NSigClient() {}

    public static boolean isEnabled() {
        return DECODER_URL != null && !DECODER_URL.isEmpty();
    }

    /**
     * If {@code url} is a googlevideo videoplayback URL with an obfuscated
     * n-param, request decryption from the sidecar and return the URL with
     * the decoded n. On any error (sidecar down, decrypt fails, no n in URL)
     * return the original url unchanged.
     */
    public static String maybeRewriteN(final String url) {
        if (!isEnabled() || url == null) return url;
        if (!url.contains("googlevideo.com")) return url;
        Matcher m = N_QUERY.matcher(url);
        if (!m.find()) return url;
        final String oldN = m.group(2);
        final String decoded = decryptN(oldN);
        if (decoded == null || decoded.equals(oldN)) return url;
        // Replace only the FIRST n= occurrence (m.group(1) keeps ? or & separator).
        return url.substring(0, m.start()) + m.group(1) + "n=" + decoded + url.substring(m.end());
    }

    private static String decryptN(final String obfuscated) {
        final String cached = CACHE.get(obfuscated);
        if (cached != null) return cached;
        try {
            // JSON-encode the n value defensively; google's n is alphanum + _ -
            final String body = "{\"n\":\"" + obfuscated.replace("\\", "\\\\").replace("\"", "\\\"") + "\"}";
            final HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(DECODER_URL))
                    .timeout(Duration.ofSeconds(3))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            final HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                System.out.println("[NSig] sidecar returned " + resp.statusCode() + " for n=" + obfuscated.substring(0, Math.min(8, obfuscated.length())) + "...");
                return null;
            }
            // Lazy parse: response is {"n":"...", "playerUrl":"..."}
            final String resBody = resp.body();
            final Matcher nm = Pattern.compile("\"n\"\\s*:\\s*\"([^\"]+)\"").matcher(resBody);
            if (!nm.find()) return null;
            final String decoded = nm.group(1);
            CACHE.put(obfuscated, decoded);
            return decoded;
        } catch (Exception e) {
            System.out.println("[NSig] decrypt call failed: " + e.getMessage());
            return null;
        }
    }
}
