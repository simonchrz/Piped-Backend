package me.kavin.piped.utils;

import com.fasterxml.jackson.databind.JsonNode;
import me.kavin.piped.consts.Constants;
import me.kavin.piped.utils.obj.PipedStream;
import me.kavin.piped.utils.obj.Streams;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/// Direkte Stream-URLs über den MOBILEN Web-Client (MWEB).
///
/// WARUM: Für Made-for-Kids liefert der WEB-Client keine direkten Segment-URLs
/// mehr — 30 adaptive Formate, 0 davon mit `url`, nur `serverAbrStreamingUrl`.
/// Genau dort greift der 60-Sekunden-Deckel: die SABR-Sitzung läuft auf
/// STREAM_PROTECTION_STATUS 2 und kippt bei ~60 s auf 3. Sämtliche Hebel
/// innerhalb von SABR sind kontrolliert ausgeschlossen (Token, Identität,
/// client_abr_state, Formate, URL-Parameter, Transport, IP-Familie,
/// Sitzungsquelle, TLS-Fingerabdruck, sogar Java gegen Node).
///
/// MWEB liefert für dasselbe Video **32 Formate MIT direkter URL**, darunter
/// itag 137 und 140. Damit lässt sich SABR komplett umgehen.
///
/// Verifiziert 2026-08-01 an einem 46-Minuten-Kids-Video, Byte-Bereich 200 KB
/// VOR Dateiende: itag 137 (608 MB) und 140 (45 MB) antworten mit HTTP 206.
///
/// Drei Zutaten, jede einzeln belegt:
///   1. Player-Call als MWEB, angemeldet (Cookie + SAPISIDHASH) und mit
///      `signatureTimestamp` — ohne den kommt UNPLAYABLE, auch bei normalen Videos.
///   2. `n`-Parameter entschlüsseln (roh → 403, entschlüsselt → 206).
///   3. `&pot=<videoId-gebundener Token>` anhängen — ohne den geben 137/140
///      am Dateiende 403 (itag 18 auch ohne).
public class MwebStreams {

    private static final String MWEB_UA =
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 "
                    + "(KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1";
    private static final String MWEB_VERSION = "2.20260731.00.00";
    private static final Pattern STS = Pattern.compile("\"STS\":\\s*(\\d+)");

    /// Ausgeschaltet über YT_MWEB_DIRECT=0.
    public static boolean aktiv() {
        return !"0".equals(System.getenv("YT_MWEB_DIRECT"));
    }

    /// Holt direkte Streams. Gibt null zurück, wenn nichts Brauchbares kommt —
    /// der Aufrufer bleibt dann beim bisherigen Weg.
    /// ⚠️ Ein Tap holt drei Playlists; ohne Cache laeuft der MWEB-Player-Call
    /// dreimal. Gemessen 2026-08-01: pro Aufruf ~1-2 s.
    /// 60 s TTL — lang genug fuer einen Tap, kurz genug fuer die bekannt
    /// kurzlebigen Kids-URLs.
    private record Treffer(Streams streams, long at) { }
    private static final java.util.concurrent.ConcurrentHashMap<String, Treffer> CACHE =
            new java.util.concurrent.ConcurrentHashMap<>();
    private static final long TTL_MS = 60_000L;

    public static Streams hole(String videoId) {
        final Treffer t = CACHE.get(videoId);
        if (t != null && System.currentTimeMillis() - t.at() < TTL_MS) return t.streams();
        final Streams frisch = holeFrisch(videoId);
        if (frisch != null) CACHE.put(videoId, new Treffer(frisch, System.currentTimeMillis()));
        return frisch;
    }

    private static Streams holeFrisch(String videoId) {
        if (!aktiv()) return null;
        try {
            final JsonNode player = playerCall(videoId);
            final String status = player.path("playabilityStatus").path("status").asText();
            final JsonNode sd = player.path("streamingData");
            final var alle = new ArrayList<JsonNode>();
            sd.path("adaptiveFormats").forEach(alle::add);
            sd.path("formats").forEach(alle::add);

            final List<JsonNode> mitUrl = alle.stream()
                    .filter(f -> f.hasNonNull("url") && !f.get("url").asText().isEmpty())
                    .toList();
            if (mitUrl.isEmpty()) {
                System.out.println("[MWEB] " + videoId + " status=" + status
                        + " — keine direkten URLs, kein Gewinn");
                return null;
            }

            final String pot = potToken(videoId);
            final Streams s = new Streams();
            s.videoStreams = new ArrayList<>();
            s.audioStreams = new ArrayList<>();
            for (JsonNode f : mitUrl) {
                final PipedStream ps = baue(f, pot);
                if (ps == null) continue;
                final String mime = f.path("mimeType").asText("");
                if (mime.startsWith("audio")) s.audioStreams.add(ps);
                else s.videoStreams.add(ps);
            }
            System.out.println("[MWEB] " + videoId + " status=" + status
                    + " direkte URLs: " + mitUrl.size()
                    + " (Video " + s.videoStreams.size() + " / Audio " + s.audioStreams.size() + ")"
                    + (pot != null ? " mit pot" : " OHNE pot"));
            return s.videoStreams.isEmpty() && s.audioStreams.isEmpty() ? null : s;
        } catch (Exception e) {
            System.out.println("[MWEB] " + videoId + " fehlgeschlagen: " + e);
            return null;
        }
    }

    private static PipedStream baue(JsonNode f, String pot) {
        try {
            String url = f.get("url").asText();
            // â ïž NICHT maybeRewriteN() â das ist ein No-Op fuer URLs aus dem
            // NewPipe-Extractor. Unsere MWEB-URLs stammen aus einem EIGENEN
            // Player-Call, ihr n ist noch verschluesselt. Mit rohem n: 403.
            url = NSigClient.rewriteNUnprocessed(url);
            // ⚠️ Ohne pot antworten 137/140 am Dateiende mit 403.
            if (pot != null && !pot.isEmpty() && !url.contains("&pot="))
                url = url + "&pot=" + pot;

            final String mime = f.path("mimeType").asText("");
            final boolean videoOnly = mime.startsWith("video");
            final PipedStream ps = new PipedStream(
                    f.path("itag").asInt(), url,
                    mime.contains("mp4") ? "MPEG_4" : "WEBM",
                    f.path("qualityLabel").asText(f.path("audioQuality").asText("")),
                    mime, videoOnly, f.path("contentLength").asLong(0));
            ps.bitrate = f.path("bitrate").asInt(0);
            ps.width = f.path("width").asInt(0);
            ps.height = f.path("height").asInt(0);
            ps.fps = f.path("fps").asInt(0);
            ps.initStart = (int) f.path("initRange").path("start").asLong(0);
            ps.initEnd = (int) f.path("initRange").path("end").asLong(0);
            ps.indexStart = (int) f.path("indexRange").path("start").asLong(0);
            ps.indexEnd = (int) f.path("indexRange").path("end").asLong(0);
            return ps;
        } catch (Exception e) {
            return null;
        }
    }

    /// videoId-gebundener GVS-Token — genau die Bindung, die im Test
    /// funktioniert hat (ohne pot geben 137/140 am Dateiende 403).
    private static String potToken(String videoId) {
        try {
            final BgPoTokenProvider p = BgPoTokenProvider.instance();
            return p == null ? null : p.sabrContentBoundPoToken(videoId);
        } catch (Throwable e) {
            System.out.println("[MWEB] kein pot fuer " + videoId + ": " + e.getMessage());
            return null;
        }
    }

    private static JsonNode playerCall(String videoId) throws Exception {
        final int sts = signatureTimestamp(videoId);
        final Map<String, Object> client = new HashMap<>(Map.of(
                "clientName", "MWEB", "clientVersion", MWEB_VERSION,
                "hl", "de", "gl", "DE"));
        final Map<String, Object> req = new HashMap<>(Map.of(
                "context", Map.of("client", client),
                "videoId", videoId, "contentCheckOk", true, "racyCheckOk", true,
                "playbackContext", Map.of("contentPlaybackContext",
                        Map.of("signatureTimestamp", sts,
                                "html5Preference", "HTML5_PREF_WANTS"))));
        final var r = rocks.kavin.reqwest4j.ReqwestUtils.fetch(
                "https://www.youtube.com/youtubei/v1/player?prettyPrint=false",
                "POST", Constants.mapper.writeValueAsBytes(req),
                kopfzeilen()).get(25, TimeUnit.SECONDS);
        if (r.status() / 100 != 2)
            throw new IllegalStateException("MWEB-Player HTTP " + r.status());
        return Constants.mapper.readTree(r.body());
    }

    /// ⚠️ Ohne signatureTimestamp antwortet der Player-Call mit UNPLAYABLE.
    private static int signatureTimestamp(String videoId) throws Exception {
        final var r = rocks.kavin.reqwest4j.ReqwestUtils.fetch(
                "https://www.youtube.com/watch?v=" + videoId, "GET", null,
                Map.of("User-Agent", MWEB_UA)).get(25, TimeUnit.SECONDS);
        final Matcher m = STS.matcher(new String(r.body(), StandardCharsets.UTF_8));
        return m.find() ? Integer.parseInt(m.group(1)) : 0;
    }

    /// Angemeldet aufrufen: Cookies allein genügen youtubei nicht, es braucht
    /// zusätzlich die SAPISIDHASH-Signatur.
    private static Map<String, String> kopfzeilen() {
        final Map<String, String> h = new HashMap<>(Map.of(
                "Content-Type", "application/json",
                "Accept-Encoding", "identity",
                "User-Agent", MWEB_UA,
                "Origin", "https://www.youtube.com",
                "Referer", "https://www.youtube.com/"));
        final String cookies = BgPoTokenProvider.loadCookieHeader();
        if (cookies == null || cookies.isEmpty()) return h;
        h.put("Cookie", cookies);
        final String sapisid = keksWert(cookies, "SAPISID") != null
                ? keksWert(cookies, "SAPISID") : keksWert(cookies, "__Secure-3PAPISID");
        if (sapisid != null) {
            try {
                final long ts = System.currentTimeMillis() / 1000L;
                final byte[] d = MessageDigest.getInstance("SHA-1").digest(
                        (ts + " " + sapisid + " https://www.youtube.com")
                                .getBytes(StandardCharsets.UTF_8));
                final StringBuilder hex = new StringBuilder();
                for (byte b : d) hex.append(String.format("%02x", b));
                h.put("Authorization", "SAPISIDHASH " + ts + "_" + hex);
                h.put("X-Origin", "https://www.youtube.com");
                h.put("X-Goog-AuthUser", "0");
            } catch (Exception ignored) { /* dann eben ohne Signatur */ }
        }
        return h;
    }

    private static String keksWert(String header, String name) {
        for (String teil : header.split(";")) {
            final String s = teil.trim();
            if (s.startsWith(name + "=")) return s.substring(name.length() + 1);
        }
        return null;
    }
}
