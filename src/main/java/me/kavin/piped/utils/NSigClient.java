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
        // No-op: NPE's YoutubeJavaScriptPlayerManager now handles n-decode via
        // the same sidecar. A second pass here would re-encode the already
        // decoded n and produce a garbage 9-char value -> HTTP 403.
        return url;
    }

    /// EXPLIZITE n-Entschluesselung fuer URLs, die NICHT durch NewPipeExtractor
    /// gelaufen sind — konkret die `serverAbrStreamingUrl` aus unserem eigenen
    /// Player-Call in SabrHandlers. maybeRewriteN() ist bewusst ein No-Op, weil
    /// NPE seine URLs schon entschluesselt; diese hier hat das noch NICHT.
    /// Browser-Mitschnitt 2026-07-25: der echte Player schickt `n` entschluesselt
    /// (Player-Antwort 16 Zeichen -> Request 14, verschieden).
    public static String rewriteNUnprocessed(final String url) {
        if (!isEnabled() || url == null) return url;
        final Matcher m = Pattern.compile("([?&]n=)([A-Za-z0-9_-]+)").matcher(url);
        if (!m.find()) return url;
        final String dec = decryptN(m.group(2));
        if (dec == null || dec.equals(m.group(2))) return url;
        return url.substring(0, m.start(2)) + dec + url.substring(m.end(2));
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
