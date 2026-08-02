package me.kavin.piped.utils;

import org.jetbrains.annotations.Nullable;
import org.schabi.newpipe.extractor.services.youtube.PoTokenProvider;
import org.schabi.newpipe.extractor.services.youtube.PoTokenResult;
import rocks.kavin.reqwest4j.ReqwestUtils;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.regex.Pattern;

import static me.kavin.piped.consts.Constants.mapper;

public class BgPoTokenProvider implements PoTokenProvider {

    private final String bgHelperUrl;

    // Singleton handle so non-NewPipe call sites (the SABR session, which talks
    // to googlevideo directly, not via YoutubeStreamExtractor) can reuse the warm
    // pool + bg-helper without threading an instance through. Set in the ctor;
    // Main.java constructs exactly one and hands it to setPoTokenProvider().
    private static volatile BgPoTokenProvider INSTANCE;

    public static @Nullable BgPoTokenProvider instance() { return INSTANCE; }

    public BgPoTokenProvider(String bgHelperUrl) {
        this.bgHelperUrl = bgHelperUrl;
        INSTANCE = this;
    }

    /// SABR accessor: a pooled visitorData-bound token (the same shape yt-dlp uses
    /// for logged-out googlevideo streaming). Returns null if the pool/helper is
    /// unavailable. Used by SabrHandlers to authorize the ANDROID player call
    /// (visitorData in context) + the ABR streamerContext (po_token bytes).
    public @Nullable PoTokenResult sabrSessionPoToken() {
        try {
            return getPoTokenPooled();
        } catch (Exception e) {
            System.out.println("[Piped/Bg] sabrSessionPoToken failed: " + e.getMessage());
            return null;
        }
    }

    /// SABR accessor: a videoId-content-bound token (fallback experiment path if
    /// the visitorData-bound one is rejected by the ANDROID SABR session).
    public @Nullable String sabrContentBoundPoToken(String videoId) {
        return mintContentBoundPoToken(videoId);
    }

    /// po_token mit BELIEBIGER content_binding minten (Protokoll-Analyse
    /// 2026-07-25): die SABR-Session startet mit einem an `visitorData`
    /// gebundenen Token und laeuft damit ~7 Runden; die Re-Attestierung mit einem
    /// an die videoId gebundenen Token wird abgelehnt. Damit laesst sich testen,
    /// ob die Bindung identisch zur Session-Bindung sein muss.
    public @Nullable String sabrPoTokenForBinding(String binding) {
        return mintContentBoundPoToken(binding);
    }

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    /// visitorData der ANGEMELDETEN Sitzung holen.
    ///
    /// ⚠️ Das war die Naht in unserer Identitaet: Player-Aufrufe und
    /// Segment-Abrufe laufen laengst MIT den Login-Cookies, das visitorData
    /// wurde aber ANONYM gescrapt — und der BotGuard-Token haengt an genau
    /// diesem anonymen visitorData. Wir haben also eine angemeldete mit einer
    /// anonymen Identitaet gemischt. Ein echter eingeloggter Browser bekommt
    /// dieselben (Kids-)Videos ausgeliefert, waehrend wir 403 kassieren —
    /// gemessen 2026-07-30 mit Safari gegen unsere IP. Deshalb hier dieselben
    /// Cookies verwenden, damit visitorData, Token und Abruf EINE Identitaet
    /// sind. Kill-Switch: POTOKEN_AUTH_VISITOR=0.
    /// 🔑 Besucherkennung MERKEN. Der frische Abruf laedt die komplette
    /// (angemeldete) youtube.com-Startseite — gemessen 2026-08-02 rund 950 ms,
    /// und das bei JEDER Token-Praegung, also bei jedem kalten Tap. Die Kennung
    /// ist aber sitzungsstabil: sie identifiziert unsere Sitzung, nicht das
    /// Video. Zum Vergleich die anderen Posten derselben Praegung: Aufgabe
    /// holen 38 ms, get_pot 357 ms.
    ///
    /// ⚠️ Bei Fehlern NICHT merken — sonst friert ein CookieMismatch-Rueckfall
    /// auf die anonyme Kennung fuer eine halbe Stunde ein.
    private static final long VISITOR_MEMO_MS = 30L * 60_000L;
    private volatile String visitorMemo = null;
    private volatile long visitorMemoBis = 0L;

    private String getWebVisitorData() throws Exception {
        final String memo = visitorMemo;
        if (memo != null && System.currentTimeMillis() < visitorMemoBis) return memo;
        final String frisch = holeWebVisitorDataFrisch();
        visitorMemo = frisch;
        visitorMemoBis = System.currentTimeMillis() + VISITOR_MEMO_MS;
        return frisch;
    }

    /// Verwirft die gemerkte Kennung — aufzurufen, wenn ein Aufruf mit ihr
    /// scheitert (abgelaufene Sitzung).
    void vergissVisitorData() {
        visitorMemo = null;
        visitorMemoBis = 0L;
    }

    private String holeWebVisitorDataFrisch() throws Exception {
        final boolean useAuth = !"0".equals(env("POTOKEN_AUTH_VISITOR"));
        final String cookies = useAuth ? loadCookieHeader() : null;
        final String html;
        if (cookies != null) {
            html = new String(rocks.kavin.reqwest4j.ReqwestUtils.fetch(
                    "https://www.youtube.com", "GET", null,
                    java.util.Map.of("User-Agent", me.kavin.piped.consts.Constants.USER_AGENT,
                            "Cookie", cookies,
                            "Accept-Language", "de-DE,de;q=0.9")).get().body());
        } else {
            html = RequestUtils.sendGet("https://www.youtube.com").get();
        }
        java.util.regex.Matcher matcher = Pattern.compile("visitorData\":\"([\\w%-]+)\"").matcher(html);

        // ⚠️ RUECKFALL. Sind die Login-Cookies abgelaufen, leitet YouTube auf
        // accounts.google.com/CookieMismatch um und der Body ist leer — dann
        // faende sich hier NICHTS, der Warm-Pool bliebe leer und JEDE Sitzung
        // liefe ohne visitorData UND ohne Token (gemessen 2026-07-31: Pool
        // dauerhaft leer, "warm-pool task error: Failed to get visitor data").
        // Also lieber anonym weitermachen als gar nicht.
        if (!matcher.find() && cookies != null) {
            System.out.println("[Piped/Bg] ⚠️ angemeldeter visitorData-Abruf ohne Ergebnis "
                    + "(Cookies abgelaufen? -> CookieMismatch) — falle auf anonym zurueck");
            final String anon = RequestUtils.sendGet("https://www.youtube.com").get();
            matcher = Pattern.compile("visitorData\":\"([\\w%-]+)\"").matcher(anon);
        }

        // Konto-Kennung mitlesen: bei ANGEMELDETEN Sitzungen bindet YouTube den
        // Player-Token daran, nicht an die Besucherkennung (yt-dlp-Wiki:
        // "If logged in, use datasync ID for player tokens"). Format
        // "ID1||ID2" — gebunden wird an ID1.
        final java.util.regex.Matcher dsm =
                Pattern.compile("\"(?:DATASYNC_ID|datasyncId)\"\\s*:\\s*\"([^\"|]+)").matcher(html);
        if (dsm.find()) {
            final String ds = dsm.group(1);
            if (!ds.equals(datasyncId)) {
                datasyncId = ds;
                System.out.println("[Piped/Bg] Konto-Kennung (datasyncId) erkannt: "
                        + ds.substring(0, Math.min(8, ds.length())) + "…");
            }
        }

        if (matcher.find()) {
            final String vd = matcher.group(1);
            if (!visitorLogged) {
                visitorLogged = true;
                System.out.println("[Piped/Bg] visitorData aus " + (cookies != null
                        ? "ANGEMELDETER Sitzung (" + (html.contains("\"LOGGED_IN\":true") ? "logged_in=true" : "Login unklar") + ")"
                        : "anonymer Sitzung") + " geholt");
            }
            return vd;
        }

        throw new RuntimeException("Failed to get visitor data");
    }

    private volatile boolean visitorLogged = false;

    /// Konto-Kennung der angemeldeten Sitzung (leer = anonym).
    private volatile String datasyncId = null;
    public @Nullable String datasyncId() { return datasyncId; }

    /// Login-Cookies als Header-Zeile (Netscape-Format wie yt-dlp sie schreibt).
    public static String loadCookieHeader() {
        String p = System.getenv("YOUTUBE_COOKIES_FILE");
        if (p == null || p.isEmpty()) p = "/app/youtube-cookies.txt";
        final java.io.File f = new java.io.File(p);
        if (!f.exists()) return null;
        final StringBuilder sb = new StringBuilder();
        try (java.io.BufferedReader r = new java.io.BufferedReader(new java.io.FileReader(f))) {
            String line;
            while ((line = r.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;
                final String[] parts = line.split("\t");
                if (parts.length < 7) continue;
                // ⚠️ NUR youtube.com-Cookies. Ein Export aus dem Browser enthaelt
                // ALLE Domains (gemessen: 258 Cookies, davon 25 fuer YouTube).
                // Schickt man alles zusammen, antwortet Google mit 302 auf
                // accounts.google.com/CookieMismatch — wir waren dadurch NIE
                // angemeldet, obwohl die Datei gueltige Login-Cookies enthielt.
                // Mit dem Filter: HTTP 200 und "LOGGED_IN":true.
                if (!parts[0].contains("youtube.com")) continue;
                if (sb.length() > 0) sb.append("; ");
                sb.append(parts[5]).append('=').append(parts[6]);
            }
        } catch (Exception e) {
            return null;
        }
        return sb.length() == 0 ? null : sb.toString();
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

    /// Holt die BotGuard-Aufgabe in UNSEREM Namen (angemeldet) statt sie den
    /// Provider anonym besorgen zu lassen.
    ///
    /// Der bgutil-Container ruft sonst selbst `/att/get` auf — ohne unsere
    /// Cookies (sein Log sagt woertlich "Using challenge from /att/get"). Das
    /// Token beantwortet dann die Frage an einen fremden Besucher. Mit unseren
    /// Cookies stellt YouTube die Aufgabe UNSERER Sitzung (HTTP 200 mit
    /// `challenge` + `bgChallenge`), und der Provider loest genau die.
    ///
    /// ⚠️ Dem Provider das `bgChallenge`-OBJEKT geben, nicht die verschluesselte
    /// Zeichenkette daneben — sonst scheitert er an
    /// "Cannot destructure property privateDoNotAccessOrElseTrustedResourceUrlWrappedValue".
    /// ⚠️ Das bricht NICHT den 60-Sekunden-Cap bei Made-for-Kids (sechs Ansaetze
    /// gemessen, alle wirkungslos) — es ist die korrekte Identitaet, kein
    /// Freifahrtschein. Kill-Switch: POTOKEN_OWN_CHALLENGE=0.
    private @Nullable com.fasterxml.jackson.databind.JsonNode fetchOwnChallenge(String visitorData) {
        if ("0".equals(env("POTOKEN_OWN_CHALLENGE"))) return null;
        try {
            final var client = mapper.createObjectNode();
            client.put("clientName", "WEB").put("clientVersion", WEB_CLIENT_VERSION)
                    .put("visitorData", visitorData).put("hl", "de").put("gl", "DE");
            final var ctx = mapper.createObjectNode();
            ctx.set("client", client);
            final var body = mapper.createObjectNode();
            body.put("engagementType", "ENGAGEMENT_TYPE_UNBOUND");
            body.set("context", ctx);

            final java.util.Map<String, String> headers = new java.util.HashMap<>(java.util.Map.of(
                    "Content-Type", "application/json",
                    "User-Agent", me.kavin.piped.consts.Constants.USER_AGENT,
                    "X-Goog-Visitor-Id", visitorData,
                    "X-Youtube-Client-Name", "1",
                    "X-Youtube-Client-Version", WEB_CLIENT_VERSION,
                    "Origin", "https://www.youtube.com"));
            final String cookies = loadCookieHeader();
            if (cookies != null) headers.put("Cookie", cookies);

            final var resp = ReqwestUtils.fetch(
                    "https://www.youtube.com/youtubei/v1/att/get?prettyPrint=false",
                    "POST", mapper.writeValueAsBytes(body), headers).get(20, TimeUnit.SECONDS);
            if (resp.status() / 100 != 2) {
                System.out.println("[Piped/Bg] att/get HTTP " + resp.status() + " -> ohne eigene Aufgabe");
                return null;
            }
            final var json = mapper.readTree(resp.body());
            final var bg = json.get("bgChallenge");
            if (bg == null || bg.isNull()) {
                System.out.println("[Piped/Bg] att/get ohne bgChallenge -> ohne eigene Aufgabe");
                return null;
            }
            return bg;
        } catch (Exception e) {
            System.out.println("[Piped/Bg] eigene Aufgabe nicht holbar (" + e.getMessage() + ")");
            return null;
        }
    }

    /// Client-Version fuer att/get und den Sitzungskontext.
    private static final String WEB_CLIENT_VERSION = "2.20260122.01.00";

    private PoTokenResult createWebClientPoToken() throws Exception {
        String visitorDate = getWebVisitorData();
        // ⚠️ ANGEMELDET wird an die KONTO-Kennung gebunden, nicht an die
        // Besucherkennung. Genau das macht ein eingeloggter Browser — und genau
        // diese Paarung fehlte uns: wir haben angemeldet gestreamt, aber einen
        // Token vorgezeigt, der auf einen anonymen Besucher ausgestellt war.
        // Der Server duldet so etwas nur ~1–2 MB ("Attestierung erforderlich",
        // STREAM_PROTECTION_STATUS 2→3) — das sind die 40–60 s, nach denen
        // Kids-Videos abbrechen. Kill-Switch: POTOKEN_BIND_DATASYNC=0.
        final String binding = (datasyncId != null
                && !"0".equals(env("POTOKEN_BIND_DATASYNC")))
                ? datasyncId : visitorDate;
        System.out.println("[Piped/Bg] /get_pot POST content_binding=" 
                + (binding == datasyncId ? "datasyncId" : "visitorData")
                + " length=" + binding.length());
        // Brainicism's bgutil-pot-server: POST /get_pot mit content_binding (volle visitorData ok)
        final var potBody = mapper.createObjectNode().put("content_binding", binding);
        final var eigeneAufgabe = fetchOwnChallenge(visitorDate);
        if (eigeneAufgabe != null) {
            potBody.set("challenge", eigeneAufgabe);
            final var client = mapper.createObjectNode();
            client.put("clientName", "WEB").put("clientVersion", WEB_CLIENT_VERSION)
                    .put("visitorData", visitorDate).put("hl", "de").put("gl", "DE");
            final var ctx = mapper.createObjectNode();
            ctx.set("client", client);
            potBody.set("innertube_context", ctx);
            System.out.println("[Piped/Bg] praege mit EIGENER Aufgabe (angemeldete Sitzung)");
        }
        String poToken = ReqwestUtils.fetch(bgHelperUrl + "/get_pot", "POST",
                mapper.writeValueAsBytes(potBody), Map.of(
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
        // EXPERIMENT 2026-06-20: isFamilySafe videos are forced onto the WebEmbed
        // path, whose streaming URLs 403 because we send them pot-less
        // (getPoTokenPooled -> streamingDataPoToken=null). The player pot stays
        // visitorData-bound; mint a CONTENT-BOUND (videoId) pot for the streaming
        // URLs and see whether googlevideo accepts it. Falls back to today's
        // behaviour (no streaming pot) when the content-bound mint fails.
        try {
            final long t0 = System.currentTimeMillis();
            PoTokenResult base = getPoTokenPooled();
            if (base == null) return null;
            final long t1 = System.currentTimeMillis();
            String streamingPot = mintContentBoundPoToken(videoId);
            System.out.println("[Bg-timing] " + videoId + " pooled="
                    + (t1 - t0) + "ms contentBound="
                    + (System.currentTimeMillis() - t1) + "ms");
            System.out.println("[Piped/Bg] webEmbed content-bound streaming pot: "
                    + (streamingPot != null
                        ? streamingPot.substring(0, Math.min(16, streamingPot.length())) + "..."
                        : "NULL (fallback to pot-less)"));
            return new PoTokenResult(base.visitorData, base.playerRequestPoToken, streamingPot);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /// Mint a po_token bound to the videoId (content binding) rather than
    /// visitorData — for the WebEmbed streaming URLs. Returns null on failure.
    /// ⚠️ Die Aufgabe MUSS mit — sonst schneidet SABR bei ~60 s ab.
    ///
    /// Belegt in der Referenz-Implementierung (LuanRT/googlevideo), gleich
    /// zweifach mit exakt unserem Symptom:
    ///   * Issue #38, vom Autor: „requires the web client to use content bound
    ///     PO tokens — changing the binding to videoId should fix it".
    ///   * Issue #45: „caused when the used potoken is WRONG. Using potoken
    ///     generation with innertube.getAttestationChallenge(
    ///     'ENGAGEMENT_TYPE_UNBOUND') makes the stream no longer cut off after
    ///     60 seconds" — von zwei weiteren Nutzern bestaetigt.
    ///
    /// Beides zusammen: videoId-gebunden UND aus der selbst geholten
    /// Attestierungs-Aufgabe gepraegt. Wir hatten bisher nur die Bindung; die
    /// Aufgabe steckte allein im Pool-Token (createWebClientPoToken), nicht
    /// hier. Kill-Switch: POTOKEN_OWN_CHALLENGE=0.
    private @Nullable String mintContentBoundPoToken(String videoId) {
        try {
            final var rumpf = mapper.createObjectNode().put("content_binding", videoId);
            try {
                final String vd = getWebVisitorData();
                final long tA = System.currentTimeMillis();
                final var aufgabe = fetchOwnChallenge(vd);
                System.out.println("[Bg-timing] Aufgabe holen "
                        + (System.currentTimeMillis() - tA) + "ms");
                if (aufgabe != null) {
                    rumpf.set("challenge", aufgabe);
                    final var client = mapper.createObjectNode();
                    client.put("clientName", "WEB").put("clientVersion", WEB_CLIENT_VERSION)
                            .put("visitorData", vd).put("hl", "de").put("gl", "DE");
                    final var ctx = mapper.createObjectNode();
                    ctx.set("client", client);
                    rumpf.set("innertube_context", ctx);
                    rumpf.put("bypass_cache", true);
                }
            } catch (Exception e) {
                System.out.println("[Piped/Bg] content-bound ohne eigene Aufgabe (" + e.getMessage() + ")");
            }
            final long tB = System.currentTimeMillis();
            return ReqwestUtils.fetch(bgHelperUrl + "/get_pot", "POST",
                    mapper.writeValueAsBytes(rumpf),
                    Map.of("Content-Type", "application/json"))
                .thenApply(response -> {
                    System.out.println("[Bg-timing] get_pot "
                            + (System.currentTimeMillis() - tB) + "ms");
                    try {
                        return mapper.readTree(new String(response.body())).get("poToken").asText();
                    } catch (Exception e) {
                        return null;
                    }
                }).join();
        } catch (Exception e) {
            System.out.println("[Piped/Bg] content-bound pot mint failed: " + e.getMessage());
            return null;
        }
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
