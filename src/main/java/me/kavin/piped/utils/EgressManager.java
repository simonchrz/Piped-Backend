package me.kavin.piped.utils;

import org.schabi.newpipe.extractor.exceptions.SignInConfirmNotBotException;
import rocks.kavin.reqwest4j.ReqwestUtils;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Auto-flips the YouTube egress IP family when YouTube bot-flags one.
 *
 * Both the public IPv4 (direct egress) and the IPv6 /64 (via the yt-v6-proxy
 * tinyproxy) get bot-flagged independently and alternately — the clean family
 * changes over time (a whole-/64 flag can't be dodged by rotating within the
 * prefix). Every reqwest goes through {@link #activeProxy()} (see
 * DownloaderImpl), so flipping the active family redirects all subsequent
 * traffic with no restart and no per-process re-init (reqwest4j's client is a
 * once-set OnceLock; re-init would panic — hence the per-request fetchWithProxy
 * fork instead).
 *
 * Purely reactive: there is no periodic canary (which would add YouTube traffic
 * and itself raise the bot-flag risk). When a resolve throws
 * SignInConfirmNotBotException the handler calls {@link #flipOnBotFlag()}, which
 * probes the OTHER family once and flips to it if it's clean, then the handler
 * retries the resolve. A short post-flip window collapses concurrent failures
 * onto one probe.
 *
 * Config (env): YT_V6_PROXY (default http://yt-v6-proxy:8888),
 * YT_EGRESS_AUTOFLIP (default true), YT_EGRESS_INITIAL ("" = v4-direct default,
 * or the proxy URL to start on v6 — used to validate the flip while v6 is the
 * flagged family).
 */
public class EgressManager {

    private static final String V6_PROXY = envOr("YT_V6_PROXY", "http://yt-v6-proxy:8888");
    private static final boolean ENABLED = !"false".equalsIgnoreCase(envOr("YT_EGRESS_AUTOFLIP", "true"));
    private static final String CANARY_ID = envOr("YT_EGRESS_CANARY", "jNQXAC9IVRw");

    // "" = direct (v4). V6_PROXY = via the IPv6 tinyproxy.
    private static final AtomicReference<String> ACTIVE =
            new AtomicReference<>(envOr("YT_EGRESS_INITIAL", ""));

    private static final Object LOCK = new Object();
    private static volatile long lastFlip = 0;
    private static final long RECENT_FLIP_MS = 30_000;

    static {
        System.out.println("[Egress] per-request egress, autoflip=" + ENABLED
                + ", v6=" + V6_PROXY + ", start=" + label(ACTIVE.get()));
    }

    /** The proxy URL all reqwest traffic should use right now ("" = direct/v4). */
    public static String activeProxy() {
        return ACTIVE.get();
    }

    public static String activeLabel() {
        return label(ACTIVE.get());
    }

    /**
     * Called from a resolve handler that just observed a YouTube bot-flag on the
     * active family. Probes the OTHER family once; if it's clean, flips ACTIVE to
     * it and returns true (caller should retry the resolve — it will now egress
     * via the clean family). Returns false if the alternate is also flagged (no
     * clean egress; caller surfaces the original error). Concurrent callers within
     * RECENT_FLIP_MS of a successful flip get true without re-probing.
     */
    public static boolean flipOnBotFlag() {
        if (!ENABLED) return false;
        synchronized (LOCK) {
            if (System.currentTimeMillis() - lastFlip < RECENT_FLIP_MS) {
                return true; // already flipped just now — retry on the fresh active
            }
            String cur = ACTIVE.get();
            String other = cur.isEmpty() ? V6_PROXY : "";
            if (probeClean(other)) {
                ACTIVE.set(other);
                lastFlip = System.currentTimeMillis();
                System.out.println("[Egress] " + label(cur) + " bot-flagged -> flipped to " + label(other));
                return true;
            }
            System.out.println("[Egress] alternate egress " + label(other)
                    + " also unclean -> staying on " + label(cur) + " (no clean YouTube egress)");
            return false;
        }
    }

    /** Walks the cause chain for a YouTube sign-in / bot-confirm block. */
    public static boolean isSignInBlock(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof SignInConfirmNotBotException) return true;
            String m = c.getMessage();
            if (m != null && m.contains("blocked anonymous watch access")) return true;
        }
        return false;
    }

    // True iff YouTube serves a normal (non-bot-walled) watch page via this egress.
    private static boolean probeClean(String proxy) {
        try {
            var resp = ReqwestUtils.fetchWithProxy(
                    "https://www.youtube.com/watch?v=" + CANARY_ID, "GET", null,
                    Map.of("User-Agent",
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"),
                    proxy).get(12, TimeUnit.SECONDS);
            String body = new String(resp.body());
            boolean flagged = body.contains("\"status\":\"LOGIN_REQUIRED\"")
                    || body.contains("confirm you’re not a bot")
                    || body.contains("confirm you're not a bot");
            return body.contains("\"playabilityStatus\"") && !flagged;
        } catch (Exception e) {
            return false;
        }
    }

    private static String label(String proxy) {
        return (proxy == null || proxy.isEmpty()) ? "v4-direct" : "v6-proxy";
    }

    private static String envOr(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isEmpty()) ? def : v;
    }
}
