package me.kavin.piped.utils.sabr;

import com.fasterxml.jackson.databind.JsonNode;
import me.kavin.piped.consts.Constants;
import me.kavin.piped.utils.BgPoTokenProvider;
import org.schabi.newpipe.extractor.services.youtube.PoTokenResult;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/// SABR resolve helper: fetches the ANDROID player response for a video and runs
/// one SabrSession, returning the reassembled fmp4 bytes for the forced audio
/// (140 m4a) + video (137 avc 1080p). The serving layer (SabrCache) calls this.
public final class SabrHandlers {

    private static final String ANDROID_UA =
            "com.google.android.youtube/20.10.38 (Linux; U; Android 14) gzip";

    /// Cheap viability probe for the storm fallback: ONE ANDROID player call.
    /// SABR is viable when it answers with a serverAbrStreamingUrl — the media
    /// transport itself is verified later by the actual download (ensureFile).
    /// Bounded by androidPlayer's own timeouts; any failure = not viable.
    public static boolean sabrViable(String videoId) {
        try {
            return androidPlayer(videoId, null, null)
                    .path("streamingData").path("serverAbrStreamingUrl")
                    .asText(null) != null;
        } catch (Exception e) {
            return false;
        }
    }

    /// Result of one SABR session: which itags the format picker actually chose
    /// (NOT always 140/137 — videos without 1080p avc fall back to the first
    /// matching format), plus the session outcome so SabrCache can decide on a
    /// family-retry (SABR_ERROR/empty) or a later refill (incomplete). Media
    /// bytes are streamed to the caller's Sink, not returned — a full-length
    /// video is ~1 GB and must not live in RAM.
    public record SabrMedia(int audioItag, int videoItag,
                            boolean complete, String stopReason, long segments) {}

    public static SabrMedia runSession(String videoId, SabrSession.Sink sink) throws Exception {
        return runSession(videoId, sink, null);
    }

    /// family: explicit egress family ("v4"/"v6") for BOTH the ANDROID player
    /// resolve and the SABR session — gvs URLs are IP-signed, so a mismatch
    /// between resolve- and stream-family 403s. null = global ACTIVE family.
    public static SabrMedia runSession(String videoId, SabrSession.Sink sink, String family) throws Exception {
        // A pooled visitorData-bound po_token authorizes the gvs streaming session.
        // Without it googlevideo caps the SABR readahead at ~60s (one buffer window)
        // then stops. visitorData goes into the player context; the po_token bytes
        // go into every ABR streamerContext (SabrSession field 2).
        final BgPoTokenProvider bg = BgPoTokenProvider.instance();
        String visitorData = null;
        byte[] poToken = null;
        if (bg != null) {
            final PoTokenResult pot = bg.sabrSessionPoToken();
            if (pot != null) {
                visitorData = pot.visitorData;
                if (pot.playerRequestPoToken != null) poToken = b64(pot.playerRequestPoToken);
            }
        }
        System.out.println("[Sabr] " + videoId + " runSession poToken="
                + (poToken != null ? poToken.length + "B" : "NONE")
                + " visitor=" + (visitorData != null ? "yes" : "no"));

        final JsonNode player = androidPlayer(videoId, visitorData, family);
        final JsonNode sd = player.path("streamingData");
        final String abrUrl = sd.path("serverAbrStreamingUrl").asText(null);
        final String ustB64 = findFirst(player, "videoPlaybackUstreamerConfig");
        if (abrUrl == null || ustB64 == null) {
            throw new IllegalStateException("video " + videoId + " has no SABR streaming url "
                    + "(status=" + player.path("playabilityStatus").path("status").asText() + ")");
        }
        final JsonNode aud = pickFormat(sd, "audio", 140);
        final JsonNode vid = pickFormat(sd, "video", 137);
        final byte[] clientInfo = new ProtoWriter()
                .varintField(16, 3).stringField(17, "20.10.38")
                .stringField(18, "Android").stringField(19, "14")
                .varintField(64, 34).toByteArray();
        final SabrSession.Fmt pa = new SabrSession.Fmt(aud.path("itag").asInt(), aud.path("lastModified").asLong());
        final SabrSession.Fmt pv = new SabrSession.Fmt(vid.path("itag").asInt(), vid.path("lastModified").asLong());
        // maxIterations bumped 500 -> 8000: a full-length video needs one round per
        // buffer window (~5-10s), so ~55min = several hundred rounds. The loop still
        // breaks early on complete()/stuck; 8000 is just a runaway ceiling.
        final SabrSession session = new SabrSession(abrUrl, b64(ustB64), pa, pv, clientInfo, ANDROID_UA, poToken, family);
        final SabrSession.Result res = session.fetchAll(8000, sink);
        long segs = 0;
        for (var info : res.perFormat.values()) {
            final Object v = info.get("segments");
            if (v instanceof Number n) segs += n.longValue();
        }
        return new SabrMedia(aud.path("itag").asInt(), vid.path("itag").asInt(),
                res.complete, res.stopReason, segs);
    }

    private static JsonNode pickFormat(JsonNode sd, String mimePrefix, int preferItag) {
        JsonNode firstMatch = null;
        for (JsonNode f : sd.path("adaptiveFormats")) {
            if (f.path("itag").asInt() == preferItag) return f;
            if (firstMatch == null && f.path("mimeType").asText("").startsWith(mimePrefix)) firstMatch = f;
        }
        return firstMatch;
    }

    private static JsonNode androidPlayer(String videoId, String visitorData, String family) throws Exception {
        final Map<String, Object> client = new HashMap<>(Map.of(
                "clientName", "ANDROID", "clientVersion", "20.10.38",
                "androidSdkVersion", 34, "hl", "en", "gl", "US",
                "osName", "Android", "osVersion", "14", "userAgent", ANDROID_UA));
        if (visitorData != null && !visitorData.isEmpty()) client.put("visitorData", visitorData);
        final String body = Constants.mapper.writeValueAsString(Map.of(
                "context", Map.of("client", client),
                "videoId", videoId, "contentCheckOk", true, "racyCheckOk", true));
        final byte[] resp;
        if (family != null) {
            // Explicit-family attempt: same reqwest4j family-pinned socket the
            // SABR session will use, so the returned serverAbrStreamingUrl is
            // signed for the family we actually stream on. No gzip — reqwest4j
            // returns raw bytes and the JSON is small.
            final var r = rocks.kavin.reqwest4j.ReqwestUtils.fetchWithProxy(
                    "https://www.youtube.com/youtubei/v1/player?prettyPrint=false",
                    "POST", body.getBytes(StandardCharsets.UTF_8),
                    Map.of("Content-Type", "application/json",
                            "Accept-Encoding", "identity",
                            "User-Agent", ANDROID_UA, "X-Youtube-Client-Name", "3",
                            "X-Youtube-Client-Version", "20.10.38"),
                    family).get(20, java.util.concurrent.TimeUnit.SECONDS);
            if (r.status() / 100 != 2)
                throw new IllegalStateException("ANDROID player HTTP " + r.status()
                        + " (egress=" + family + ")");
            resp = r.body();
        } else {
            resp = httpPost(
                    "https://www.youtube.com/youtubei/v1/player?prettyPrint=false",
                    body.getBytes(StandardCharsets.UTF_8), "application/json", true,
                    Map.of("User-Agent", ANDROID_UA, "X-Youtube-Client-Name", "3",
                            "X-Youtube-Client-Version", "20.10.38"));
        }
        return Constants.mapper.readTree(resp);
    }

    private static byte[] httpPost(String url, byte[] body, String contentType,
                                   boolean gunzip, Map<String, String> headers) throws Exception {
        final HttpURLConnection c = (HttpURLConnection) new URL(url).openConnection();
        try {
            c.setRequestMethod("POST");
            c.setDoOutput(true);
            c.setConnectTimeout(10_000);
            c.setReadTimeout(30_000);
            c.setRequestProperty("Content-Type", contentType);
            c.setRequestProperty("Accept-Encoding", gunzip ? "gzip" : "identity");
            if (headers != null) headers.forEach(c::setRequestProperty);
            try (OutputStream os = c.getOutputStream()) {
                os.write(body);
            }
            final String enc = c.getContentEncoding();
            InputStream in = c.getInputStream();
            if ("gzip".equalsIgnoreCase(enc)) in = new GZIPInputStream(in);
            try (InputStream is = in) {
                return is.readAllBytes();
            }
        } finally {
            c.disconnect();
        }
    }

    private static byte[] b64(String s) {
        return Base64.getUrlDecoder().decode(s.replace('+', '-').replace('/', '_'));
    }

    private static String findFirst(JsonNode node, String field) {
        if (node.has(field) && node.get(field).isTextual()) return node.get(field).asText();
        for (JsonNode child : node) {
            final String r = findFirst(child, field);
            if (r != null) return r;
        }
        return null;
    }
}
