package me.kavin.piped.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/// Persistentes Resolve-Gedächtnis.
///
/// Was ein Video braucht, um aufzulösen, ist eine EIGENSCHAFT des Videos bzw.
/// seines Kanals — kein Zufall pro Anfrage. Bisher lagen diese Merker
/// (`KNOWN_AUDIO_ZERO`, `KNOWN_THROTTLED`) nur im Arbeitsspeicher und waren nach
/// JEDEM Backend-Neustart weg. Und Neustarts sind häufig: der stündliche
/// Cookie-Refresh startet den Container neu, sobald sich ein Auth-Cookie dreht.
/// Danach zahlte jedes Kids-Video wieder die volle Kaskade (4–5 s statt ~1,5 s).
///
/// Ablage im bereits gemounteten `sabr-cache`-Volume (überlebt
/// `docker compose up --force-recreate`); Schreiben atomar über eine
/// Temp-Datei, damit ein Absturz mitten im Speichern keine kaputte Datei
/// hinterlässt.
public final class ResolveMemo {

    private static final Path DIR = Paths.get(
            System.getenv().getOrDefault("SABR_CACHE_DIR", "/app/sabr-cache"));
    private static final Path FILE = DIR.resolve("_resolve-memo.json");

    /// Drosselung ist transient (Minuten) — kurze TTL, sonst schicken wir uns
    /// selbst dauerhaft auf den langsamen Weg.
    public static final long THROTTLE_TTL_MS = 10 * 60_000L;
    /// „Android liefert keinen Ton" ist eine stabile Eigenschaft des Uploads —
    /// darf lange gelten, aber nicht ewig (Videos werden neu kodiert).
    public static final long AUDIO_ZERO_TTL_MS = 7L * 24 * 3600_000L;

    /// Kanal-Statistik: wie oft brauchte ein Video DIESES Kanals den langsamen
    /// Weg, wie oft ging der schnelle? Damit profitiert auch das ERSTE Antippen
    /// eines neuen Videos — pro-Video-Merker helfen erst beim zweiten Mal.
    public static final class ChannelStat {
        public long slow, fast, updated;
    }
    private static final ConcurrentHashMap<String, ChannelStat> CHANNELS = new ConcurrentHashMap<>();
    /// Ab so vielen langsamen Resolves gilt ein Kanal als „braucht den langsamen
    /// Weg" — und nur, wenn sie die schnellen klar dominieren.
    private static final int CHANNEL_MIN_SLOW = 3;
    private static final long CHANNEL_TTL_MS = 6 * 3600_000L;
    /// Jeder N-te Resolve ignoriert das Kanal-Urteil und probiert absichtlich den
    /// schnellen Weg. Ohne das könnte sich die Heuristik nie selbst korrigieren,
    /// wenn ein Drosselungsfenster vorbei ist — sie würde uns dauerhaft auf dem
    /// langsamen Pfad festhalten.
    private static final int CHANNEL_PROBE_EVERY = 5;
    private static final java.util.concurrent.atomic.AtomicInteger CHANNEL_CALLS =
            new java.util.concurrent.atomic.AtomicInteger();

    private static final ConcurrentHashMap<String, Long> THROTTLED = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Long> AUDIO_ZERO = new ConcurrentHashMap<>();
    private static volatile boolean loaded = false;
    private static volatile long lastSave = 0;
    private static final long SAVE_MIN_INTERVAL_MS = 5_000;

    /// Eigener Mapper statt Constants.mapper: ResolveMemo soll nicht an der
    /// Konstanten-Klasse haengen (die zieht Config/DB-Initialisierung nach).
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ResolveMemo() {}

    // ── öffentliche API ────────────────────────────────────────────────────

    public static boolean isThrottled(String videoId) { return alive(THROTTLED, videoId); }
    public static boolean isAudioZero(String videoId) { return alive(AUDIO_ZERO, videoId); }

    public static void markThrottled(String videoId) { put(THROTTLED, videoId, THROTTLE_TTL_MS); }
    public static void markAudioZero(String videoId) { put(AUDIO_ZERO, videoId, AUDIO_ZERO_TTL_MS); }

    public static void clearThrottled(String videoId) { remove(THROTTLED, videoId); }
    public static void clearAudioZero(String videoId) { remove(AUDIO_ZERO, videoId); }

    /// Urteil des Kanals: true = neue Videos dieses Kanals gleich auf den
    /// bewährten (langsameren) Weg schicken. Gibt gelegentlich absichtlich false
    /// zurück, damit sich das Urteil selbst überprüft.
    public static boolean channelPrefersSlowPath(String channelId) {
        if (channelId == null || "0".equals(System.getenv("YT_CHANNEL_MEMO"))) return false;
        ensureLoaded();
        final ChannelStat st = CHANNELS.get(channelId);
        if (st == null) return false;
        if (System.currentTimeMillis() - st.updated > CHANNEL_TTL_MS) return false;
        if (st.slow < CHANNEL_MIN_SLOW || st.slow <= st.fast * 2) return false;
        if (CHANNEL_CALLS.incrementAndGet() % CHANNEL_PROBE_EVERY == 0) {
            System.out.println("[ResolveMemo] Kanal " + channelId
                    + ": Selbstpruefung — schneller Weg wird trotz Urteil probiert");
            return false;
        }
        return true;
    }

    /// Ergebnis eines Resolves auf den Kanal buchen.
    public static void recordChannelOutcome(String channelId, boolean slowPathNeeded) {
        if (channelId == null) return;
        ensureLoaded();
        final ChannelStat st = CHANNELS.computeIfAbsent(channelId, k -> new ChannelStat());
        synchronized (st) {
            if (slowPathNeeded) st.slow++; else st.fast++;
            // Beide Zähler beschränken, damit alte Historie ein neues Verhalten
            // nicht ewig überstimmt.
            if (st.slow + st.fast > 40) { st.slow /= 2; st.fast /= 2; }
            st.updated = System.currentTimeMillis();
        }
        saveThrottled();
    }

    public static String stats() {
        ensureLoaded();
        return "throttled=" + THROTTLED.size() + " audioZero=" + AUDIO_ZERO.size()
                + " kanaele=" + CHANNELS.size();
    }

    // ── intern ─────────────────────────────────────────────────────────────

    private static boolean alive(Map<String, Long> m, String videoId) {
        ensureLoaded();
        final Long exp = m.get(videoId);
        if (exp == null) return false;
        if (exp < System.currentTimeMillis()) { m.remove(videoId); return false; }
        return true;
    }

    private static void put(Map<String, Long> m, String videoId, long ttl) {
        ensureLoaded();
        m.put(videoId, System.currentTimeMillis() + ttl);
        saveThrottled();
    }

    private static void remove(Map<String, Long> m, String videoId) {
        ensureLoaded();
        if (m.remove(videoId) != null) saveThrottled();
    }

    private static synchronized void ensureLoaded() {
        if (loaded) return;
        loaded = true;   // auch bei Fehler nur EINMAL versuchen
        try {
            if (!Files.exists(FILE)) return;
            final Map<String, Map<String, Long>> data = MAPPER.readValue(
                    Files.readAllBytes(FILE),
                    MAPPER.getTypeFactory().constructMapType(
                            HashMap.class,
                            MAPPER.getTypeFactory().constructType(String.class),
                            MAPPER.getTypeFactory().constructMapType(
                                    HashMap.class, String.class, Long.class)));
            final long now = System.currentTimeMillis();
            copyAlive(data.get("throttled"), THROTTLED, now);
            copyAlive(data.get("audioZero"), AUDIO_ZERO, now);
            final Map<String, Long> ch = data.get("channelsSlow");
            final Map<String, Long> cf = data.get("channelsFast");
            final Map<String, Long> cu = data.get("channelsUpdated");
            if (ch != null) ch.forEach((k, v) -> {
                final ChannelStat st = CHANNELS.computeIfAbsent(k, x -> new ChannelStat());
                st.slow = v == null ? 0 : v;
                st.fast = cf != null && cf.get(k) != null ? cf.get(k) : 0;
                st.updated = cu != null && cu.get(k) != null ? cu.get(k) : now;
            });
            System.out.println("[ResolveMemo] geladen: " + stats());
        } catch (Exception e) {
            System.out.println("[ResolveMemo] laden fehlgeschlagen: " + e.getMessage());
        }
    }

    private static void copyAlive(Map<String, Long> src, Map<String, Long> dst, long now) {
        if (src == null) return;
        src.forEach((k, v) -> { if (v != null && v > now) dst.put(k, v); });
    }

    /// Gedrosselt speichern: die Merker ändern sich in Wellen (ein Sturm setzt
    /// viele auf einmal), eine Datei pro Änderung wäre sinnlose I/O.
    private static void saveThrottled() {
        final long now = System.currentTimeMillis();
        if (now - lastSave < SAVE_MIN_INTERVAL_MS) return;
        lastSave = now;
        try {
            Files.createDirectories(DIR);
            final Map<String, Map<String, Long>> data = new HashMap<>();
            data.put("throttled", new HashMap<>(THROTTLED));
            data.put("audioZero", new HashMap<>(AUDIO_ZERO));
            final Map<String, Long> cs = new HashMap<>(), cf = new HashMap<>(), cu = new HashMap<>();
            CHANNELS.forEach((k, st) -> { cs.put(k, st.slow); cf.put(k, st.fast); cu.put(k, st.updated); });
            data.put("channelsSlow", cs);
            data.put("channelsFast", cf);
            data.put("channelsUpdated", cu);
            final Path tmp = DIR.resolve("_resolve-memo.tmp");
            Files.write(tmp, MAPPER.writeValueAsBytes(data));
            Files.move(tmp, FILE, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException | RuntimeException e) {
            System.out.println("[ResolveMemo] speichern fehlgeschlagen: " + e.getMessage());
        }
    }
}
