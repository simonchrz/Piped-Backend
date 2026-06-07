package me.kavin.piped.utils;

import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.Nullable;
import org.schabi.newpipe.extractor.services.youtube.PoTokenProvider;
import org.schabi.newpipe.extractor.services.youtube.PoTokenResult;
import rocks.kavin.reqwest4j.ReqwestUtils;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.regex.Pattern;

import static me.kavin.piped.consts.Constants.mapper;

@RequiredArgsConstructor
public class BgPoTokenProvider implements PoTokenProvider {

    private final String bgHelperUrl;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    private String getWebVisitorData() throws Exception {
        var html = RequestUtils.sendGet("https://www.youtube.com").get();
        var matcher = Pattern.compile("visitorData\":\"([\\w%-]+)\"").matcher(html);

        if (matcher.find()) {
            return matcher.group(1);
        }

        throw new RuntimeException("Failed to get visitor data");
    }

    private final Queue<PoTokenResult> validPoTokens = new ConcurrentLinkedQueue<>();

    // ---- Warm pool: eliminates the cold per-tap poToken mint ----
    // The web poToken is bound to visitorData (NOT the videoId, see content_binding
    // in createWebClientPoToken) and is valid for hours, so one token serves all
    // videos. The legacy path pulled each used token out of circulation for 10-15s
    // → under feed-prefetch load the pool emptied and EVERY resolve minted
    // synchronously (~1.2s = the cold-tap MISS / black spinner, ~51% of taps).
    // The warm pool keeps K tokens minted in the BACKGROUND and reuses them
    // round-robin, so getInfo never waits on a mint. Minting also drops from
    // 1-per-resolve to K-per-TTL → LESS BotGuard load, lower IP-block risk.
    // Kill-switch: POTOKEN_WARM_POOL=false → exact legacy behavior.
    private final boolean warmPoolEnabled = !"false".equalsIgnoreCase(env("POTOKEN_WARM_POOL"));
    private final int poolSize = envInt("POTOKEN_POOL_SIZE", 4);
    private final long tokenTtlMs = envLong("POTOKEN_TTL_MS", 1_800_000L); // 30 min refresh
    private final java.util.concurrent.CopyOnWriteArrayList<Warm> warmPool = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final java.util.concurrent.atomic.AtomicInteger rr = new java.util.concurrent.atomic.AtomicInteger();
    private final java.util.concurrent.atomic.AtomicInteger mintTotal = new java.util.concurrent.atomic.AtomicInteger();
    private final java.util.concurrent.atomic.AtomicInteger coldMint = new java.util.concurrent.atomic.AtomicInteger();
    private volatile boolean warmerStarted = false;

    private static final class Warm {
        final PoTokenResult result; final long mintedAt;
        Warm(PoTokenResult r, long t) { result = r; mintedAt = t; }
    }

    private static String env(String k) { String v = System.getenv(k); return v == null ? "" : v.trim(); }
    private static int envInt(String k, int d) { try { String v = env(k); return v.isEmpty() ? d : Integer.parseInt(v); } catch (Exception e) { return d; } }
    private static long envLong(String k, long d) { try { String v = env(k); return v.isEmpty() ? d : Long.parseLong(v); } catch (Exception e) { return d; } }

    private PoTokenResult getPoTokenPooled() throws Exception {
        if (!warmPoolEnabled) return getPoTokenLegacy();
        ensureWarmerStarted();
        long now = System.currentTimeMillis();
        Object[] snap = warmPool.toArray();   // stable snapshot (CoW) — no index race
        int n = snap.length;
        for (int i = 0; i < n; i++) {
            Warm w = (Warm) snap[Math.floorMod(rr.getAndIncrement(), n)];
            if (now - w.mintedAt <= tokenTtlMs) return w.result;   // reuse — the hot path
        }
        // Pool empty or all-expired (cold start / warmer behind) → mint once now.
        PoTokenResult t = createWebClientPoToken();
        if (t != null) {
            warmPool.add(new Warm(t, System.currentTimeMillis()));
            mintTotal.incrementAndGet();
            int c = coldMint.incrementAndGet();
            System.out.println("[Piped/Bg] poToken COLD-mint (pool empty), coldTotal=" + c + " poolNow=" + warmPool.size());
        }
        return t;
    }

    private synchronized void ensureWarmerStarted() {
        if (warmerStarted) return;
        warmerStarted = true;
        scheduler.scheduleWithFixedDelay(this::warmTask, 0, 5, TimeUnit.SECONDS);
        System.out.println("[Piped/Bg] poToken warm-pool started: size=" + poolSize + " ttlMs=" + tokenTtlMs);
    }

    private void warmTask() {
        try {
            long now = System.currentTimeMillis();
            int before = warmPool.size();
            warmPool.removeIf(w -> now - w.mintedAt > tokenTtlMs);   // drop stale before they 403
            int minted = 0;
            while (warmPool.size() < poolSize) {
                PoTokenResult t = createWebClientPoToken();
                if (t == null) break;                                // mint failed; retry next cycle
                warmPool.add(new Warm(t, System.currentTimeMillis()));
                mintTotal.incrementAndGet();
                minted++;
            }
            if (minted > 0 || warmPool.size() != before)
                System.out.println("[Piped/Bg] poToken warm-pool: size=" + warmPool.size() + "/" + poolSize
                        + " mintedThisCycle=" + minted + " mintTotal=" + mintTotal.get() + " coldTotal=" + coldMint.get());
        } catch (Exception e) {
            System.out.println("[Piped/Bg] warm-pool task error: " + e.getMessage());
        }
    }

    // Legacy single-use-per-15s behavior — kept as kill-switch (POTOKEN_WARM_POOL=false).
    private PoTokenResult getPoTokenLegacy() throws Exception {
        PoTokenResult poToken = validPoTokens.poll();
        if (poToken == null) poToken = createWebClientPoToken();
        if (poToken == null) return null;
        int delay = 10_000 + ThreadLocalRandom.current().nextInt(5000);
        PoTokenResult finalPoToken = poToken;
        scheduler.schedule(() -> validPoTokens.offer(finalPoToken), delay, TimeUnit.MILLISECONDS);
        return poToken;
    }

    private PoTokenResult createWebClientPoToken() throws Exception {
        String visitorDate = getWebVisitorData();
        System.out.println("[Piped/Bg] /get_pot POST content_binding length=" + visitorDate.length());
        // Brainicism's bgutil-pot-server: POST /get_pot mit content_binding (volle visitorData ok)
        String poToken = ReqwestUtils.fetch(bgHelperUrl + "/get_pot", "POST", mapper.writeValueAsBytes(mapper.createObjectNode().put(
                "content_binding", visitorDate
        )), Map.of(
                "Content-Type", "application/json"
        )).thenApply(response -> {
            try {
                int status = response.status();
                String body = new String(response.body());
                System.out.println("[Piped/Bg] /generate response status=" + status + " body=" + body.substring(0, Math.min(200, body.length())));
                return mapper.readTree(body).get("poToken").asText();
            } catch (Exception e) {
                System.out.println("[Piped/Bg] /generate parse failed: " + e.getMessage());
                return null;
            }
        }).join();

        if (poToken != null) {
            System.out.println("[Piped/Bg] new PoToken: " + poToken.substring(0, Math.min(20, poToken.length())) + "... visitor=" + visitorDate.substring(0, Math.min(20, visitorDate.length())) + "...");
            return new PoTokenResult(visitorDate, poToken, null);
        }
        System.out.println("[Piped/Bg] bg-helper returned null poToken!");
        return null;
    }

    @Override
    public @Nullable PoTokenResult getWebClientPoToken(String videoId) {
        System.out.println("[Piped/Bg] getWebClientPoToken called for " + videoId);
        try {
            return getPoTokenPooled();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public @Nullable PoTokenResult getWebEmbedClientPoToken(String videoId) {
        System.out.println("[Piped/Bg] getWebEmbedClientPoToken called for " + videoId);
        // Custom: gleiches Token-Pool wie Web-Client — bgutils-Web-PoToken
        // funktioniert auch fuer den Web-Embedded Player Client.
        try {
            return getPoTokenPooled();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    // TEST: Web-PoToken auch fuer Android/iOS — laut yt-dlp wiki nicht cross-platform,
    // aber kostet uns nichts den Test zu fahren ob YouTube es trotzdem akzeptiert.
    @Override
    public @Nullable PoTokenResult getAndroidClientPoToken(String videoId) {
        System.out.println("[Piped/Bg] getAndroidClientPoToken called for " + videoId);
        try { return getPoTokenPooled(); } catch (Exception e) { e.printStackTrace(); }
        return null;
    }

    @Override
    public @Nullable PoTokenResult getIosClientPoToken(String videoId) {
        System.out.println("[Piped/Bg] getIosClientPoToken called for " + videoId);
        try { return getPoTokenPooled(); } catch (Exception e) { e.printStackTrace(); }
        return null;
    }
}
