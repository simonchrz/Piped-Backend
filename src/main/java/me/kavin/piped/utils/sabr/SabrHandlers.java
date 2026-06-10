package me.kavin.piped.utils.sabr;

import com.fasterxml.jackson.databind.JsonNode;
import me.kavin.piped.consts.Constants;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/// SABR resolve helper: fetches the ANDROID player response for a video and runs
/// one SabrSession, returning the reassembled fmp4 bytes for the forced audio
/// (140 m4a) + video (137 avc 1080p). The serving layer (SabrCache) calls this.
public final class SabrHandlers {

    private static final String ANDROID_UA =
            "com.google.android.youtube/20.10.38 (Linux; U; Android 14) gzip";

    public static Map<Integer, byte[]> runSession(String videoId) throws Exception {
        final JsonNode player = androidPlayer(videoId);
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
        final SabrSession session = new SabrSession(abrUrl, b64(ustB64), pa, pv, clientInfo, ANDROID_UA);
        return session.fetchAll(500).media;
    }

    private static JsonNode pickFormat(JsonNode sd, String mimePrefix, int preferItag) {
        JsonNode firstMatch = null;
        for (JsonNode f : sd.path("adaptiveFormats")) {
            if (f.path("itag").asInt() == preferItag) return f;
            if (firstMatch == null && f.path("mimeType").asText("").startsWith(mimePrefix)) firstMatch = f;
        }
        return firstMatch;
    }

    private static JsonNode androidPlayer(String videoId) throws Exception {
        final String body = Constants.mapper.writeValueAsString(Map.of(
                "context", Map.of("client", Map.of(
                        "clientName", "ANDROID", "clientVersion", "20.10.38",
                        "androidSdkVersion", 34, "hl", "en", "gl", "US",
                        "osName", "Android", "osVersion", "14", "userAgent", ANDROID_UA)),
                "videoId", videoId, "contentCheckOk", true, "racyCheckOk", true));
        final byte[] resp = httpPost(
                "https://www.youtube.com/youtubei/v1/player?prettyPrint=false",
                body.getBytes(StandardCharsets.UTF_8), "application/json", true,
                Map.of("User-Agent", ANDROID_UA, "X-Youtube-Client-Name", "3",
                        "X-Youtube-Client-Version", "20.10.38"));
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
