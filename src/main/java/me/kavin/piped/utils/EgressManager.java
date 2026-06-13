package me.kavin.piped.utils;

import org.schabi.newpipe.extractor.exceptions.SignInConfirmNotBotException;
import rocks.kavin.reqwest4j.ReqwestUtils;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Auto-flips the YouTube egress IP family when YouTube bot-flags one.
 *
 * Both the public IPv4 and the IPv6 /64 get bot-flagged independently and
 * alternately — the clean family changes over time (a whole-/64 flag can't be
 * dodged by rotating within the prefix). Egress family is selected per request:
 * DownloaderImpl passes {@link #activeEgress()} ("v4"/"v6") to
 * ReqwestUtils.fetchWithProxy, whose forked native side binds the request socket
 * to that IP family (no CONNECT proxy — the old yt-v6-proxy was a pre-fork
 * workaround). Flipping ACTIVE redirects all subsequent traffic with no restart.
 *
 * Purely reactive: no periodic canary (which would add YouTube traffic and itself
 * raise the bot-flag risk). On a resolve's SignInConfirmNotBotException a handler
 * calls {@link #flipOnBotFlag()}, which probes the OTHER family once and flips to
 * it if clean, then the handler retries. A short post-flip window collapses
 * concurrent failures onto one probe.
 *
 * Config (env): YT_EGRESS_AUTOFLIP (default true), YT_EGRESS_INITIAL ("v4"
 * default, or "v6"), YT_EGRESS_CANARY (default a stable public video id).
 */
public class EgressManager {

    private static final boolean ENABLED = !"false".equalsIgnoreCase(envOr("YT_EGRESS_AUTOFLIP", "true"));
    private static final String CANARY_ID = envOr("YT_EGRESS_CANARY", "jNQXAC9IVRw");

    // "v4" or "v6" — passed straight to ReqwestUtils.fetchWithProxy as the family.
    private static final AtomicReference<String> ACTIVE =
            new AtomicReference<>(normalize(envOr("YT_EGRESS_INITIAL", "v4")));

    private static final Object LOCK = new Object();
    private static volatile long lastFlip = 0;
    private static final long RECENT_FLIP_MS = 30_000;

    static {
        System.out.println("[Egress] per-request family egress, autoflip=" + ENABLED
                + ", start=" + ACTIVE.get());
    }

    /** The egress family all reqwest traffic should use right now ("v4"/"v6"). */
    public static String activeEgress() {
        return ACTIVE.get();
    }

    public static String activeLabel() {
        return ACTIVE.get();
    }

    /**
     * Called from a resolve handler that just observed a YouTube bot-flag on the
     * active family. Probes the OTHER family once; if clean, flips ACTIVE to it and
     * returns true (caller should retry — it will now egress via the clean family).
     * Returns false if the alternate is also flagged. Concurrent callers within
     * RECENT_FLIP_MS of a successful flip get true without re-probing.
     */
    public static boolean flipOnBotFlag() {
        if (!ENABLED) return false;
        synchronized (LOCK) {
            if (System.currentTimeMillis() - lastFlip < RECENT_FLIP_MS) {
                return true; // already flipped just now — retry on the fresh family
            }
            String cur = ACTIVE.get();
            String other = cur.equals("v6") ? "v4" : "v6";
            if (probeClean(other)) {
                ACTIVE.set(other);
                lastFlip = System.currentTimeMillis();
                System.out.println("[Egress] " + cur + " bot-flagged -> flipped to " + other);
                return true;
            }
            System.out.println("[Egress] alternate egress " + other
                    + " also unclean -> staying on " + cur + " (no clean YouTube egress)");
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

    // True iff YouTube serves a normal (non-bot-walled) watch page via this family.
    private static boolean probeClean(String family) {
        try {
            var resp = ReqwestUtils.fetchWithProxy(
                    "https://www.youtube.com/watch?v=" + CANARY_ID, "GET", null,
                    Map.of("User-Agent",
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"),
                    family).get(12, TimeUnit.SECONDS);
            String body = new String(resp.body());
            boolean flagged = body.contains("\"status\":\"LOGIN_REQUIRED\"")
                    || body.contains("confirm you’re not a bot")
                    || body.contains("confirm you're not a bot");
            return body.contains("\"playabilityStatus\"") && !flagged;
        } catch (Exception e) {
            return false;
        }
    }

    private static String normalize(String s) {
        return (s != null && s.contains("v6")) ? "v6" : "v4";
    }

    private static String envOr(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isEmpty()) ? def : v;
    }
}
