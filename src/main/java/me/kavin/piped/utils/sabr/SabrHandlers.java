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
    private static final String ANDROID_VR_UA =
            "com.google.android.apps.youtube.vr.oculus/1.62.27 (Linux; U; Android 12L; eureka-user Build/SQ3A.220605.009.A1) gzip";
    private static final String WEB_UA =
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";
    private static final String WEB_EMBEDDED_VERSION = "1.20260122.01.00";

    /// Cheap viability probe for the storm fallback: ONE ANDROID player call.
    /// SABR is viable when it answers with a serverAbrStreamingUrl — the media
    /// transport itself is verified later by the actual download (ensureFile).
    /// Bounded by androidPlayer's own timeouts; any failure = not viable.
    public static boolean sabrViable(String videoId) {
        try {
            return androidPlayer(videoId, null, null, null, false)
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
        return runSession(videoId, sink, null, false);
    }

    /// family: explicit egress family ("v4"/"v6") for BOTH the ANDROID player
    /// resolve and the SABR session — gvs URLs are IP-signed, so a mismatch
    /// between resolve- and stream-family 403s. null = global ACTIVE family.
    /// contentBoundToken: mint a videoId-content-bound po_token for the ABR
    /// streamerContext instead of the pooled visitorData-bound one. Kids
    /// content ignores the visitor-bound token (session stalls at the ~60s
    /// readahead window despite the token being present); content binding is
    /// the same shape the direct-URL path mints per video (pot= param).
    public static SabrMedia runSession(String videoId, SabrSession.Sink sink,
                                       String family, boolean contentBoundToken) throws Exception {
        return runSession(videoId, sink, family, contentBoundToken, false);
    }

    /// clientMode 1 = ANDROID_VR (client 28), 2 = WEB_EMBEDDED_PLAYER (client
    /// 56, the client that verifiably SERVES kids content — and the client the
    /// pooled visitor-bound web po_token was minted FOR; kids gvs appears to
    /// enforce client<->token consistency), else ANDROID. Ladder rungs when the
    /// readahead stalls at one window.
    public static SabrMedia runSession(String videoId, SabrSession.Sink sink,
                                       String family, boolean contentBoundToken,
                                       boolean vrClient) throws Exception {
        return runSession(videoId, sink, family, contentBoundToken, vrClient ? 1 : 0);
    }

    public static SabrMedia runSession(String videoId, SabrSession.Sink sink,
                                       String family, boolean contentBoundToken,
                                       int clientMode) throws Exception {
        return runSession(videoId, sink, family, contentBoundToken, clientMode, false, null);
    }

    /// paced: 1x-Echtzeit-Session (SabrSession.fetchAll paced) für den Kids-
    /// Readahead-Cap; publishHook läuft pro Runde nach dem Disk-Flush.
    public static SabrMedia runSession(String videoId, SabrSession.Sink sink,
                                       String family, boolean contentBoundToken,
                                       int clientMode, boolean paced, Runnable publishHook) throws Exception {
        final boolean vrClient = clientMode == 1;
        final boolean webClient = clientMode == 2;
        // A pooled visitorData-bound po_token authorizes the gvs streaming session.
        // Without it googlevideo caps the SABR readahead at ~60s (one buffer window)
        // then stops. visitorData goes into the player context; the po_token bytes
        // go into every ABR streamerContext (SabrSession field 2).
        final BgPoTokenProvider bg = BgPoTokenProvider.instance();
        String visitorData = null;
        byte[] poToken = null;
        String attestationPoToken = null;
        if (bg != null) {
            final PoTokenResult pot = bg.sabrSessionPoToken();
            if (pot != null) {
                visitorData = pot.visitorData;
                if (pot.playerRequestPoToken != null) poToken = b64(pot.playerRequestPoToken);
            }
            if (contentBoundToken) {
                final String cb = bg.sabrContentBoundPoToken(videoId);
                if (cb != null) { poToken = b64(cb); attestationPoToken = cb; }
                else System.out.println("[Sabr] " + videoId
                        + " content-bound mint failed -> keeping visitor-bound token");
            }
        }
        System.out.println("[Sabr] " + videoId + " runSession poToken="
                + (poToken != null ? poToken.length + "B" : "NONE")
                + (contentBoundToken ? " (content-bound)" : "")
                + " visitor=" + (visitorData != null ? "yes" : "no"));

        // WEB mode always attests the player call with the pooled web token —
        // that pairing is the whole point of the rung.
        if (webClient && attestationPoToken == null && bg != null) {
            final PoTokenResult pot2 = bg.sabrSessionPoToken();
            if (pot2 != null) attestationPoToken = pot2.playerRequestPoToken;
        }
        final JsonNode player = webClient
                ? webEmbedPlayer(videoId, visitorData, family, attestationPoToken)
                : androidPlayer(videoId, visitorData, family, attestationPoToken, vrClient);
        final JsonNode sd = player.path("streamingData");
        final String abrUrl = sd.path("serverAbrStreamingUrl").asText(null);
        final String ustB64 = findFirst(player, "videoPlaybackUstreamerConfig");
        if (abrUrl == null || ustB64 == null) {
            throw new IllegalStateException("video " + videoId + " has no SABR streaming url "
                    + "(status=" + player.path("playabilityStatus").path("status").asText() + ")");
        }
        final JsonNode aud = pickFormat(sd, "audio", 140);
        final JsonNode vid = pickFormat(sd, "video", 137);
        final byte[] clientInfo;
        final String ua;
        if (webClient) {
            clientInfo = new ProtoWriter()
                    .varintField(16, 56).stringField(17, WEB_EMBEDDED_VERSION)
                    .toByteArray();
            ua = WEB_UA;
        } else if (vrClient) {
            clientInfo = new ProtoWriter()
                    .varintField(16, 28).stringField(17, "1.62.27")
                    .stringField(18, "Android").stringField(19, "12L")
                    .varintField(64, 32).toByteArray();
            ua = ANDROID_VR_UA;
        } else {
            clientInfo = new ProtoWriter()
                    .varintField(16, 3).stringField(17, "20.10.38")
                    .stringField(18, "Android").stringField(19, "14")
                    .varintField(64, 34).toByteArray();
            ua = ANDROID_UA;
        }
        final SabrSession.Fmt pa = new SabrSession.Fmt(aud.path("itag").asInt(), aud.path("lastModified").asLong());
        final SabrSession.Fmt pv = new SabrSession.Fmt(vid.path("itag").asInt(), vid.path("lastModified").asLong());
        // maxIterations bumped 500 -> 8000: a full-length video needs one round per
        // buffer window (~5-10s), so ~55min = several hundred rounds. The loop still
        // breaks early on complete()/stuck; 8000 is just a runaway ceiling.
        final SabrSession session = new SabrSession(abrUrl, b64(ustB64), pa, pv, clientInfo, ua, poToken, family);
        // Re-Attest-Hook: bei STREAM_PROTECTION_STATUS=3 einen FRISCHEN
        // content-bound po_token minten und die Session damit fortsetzen
        // (s. SabrSession — das war die vermeintliche Kids-Readahead-Sperre).
        // Vollstaendige Session-Erneuerung bei prot=3: NEUER Player-Call (gleicher
        // Client/Egress/visitorData) → frische abrUrl + ustreamerConfig + frischer
        // Attestierungs-Token. Nur einen Token nachzureichen genuegt nicht.
        final String visitorForRenewal = visitorData;
        session.setSessionRefresher(() -> {
            try {
                String attest = null;
                if (bg != null) {
                    final PoTokenResult p2 = bg.sabrSessionPoToken();
                    if (p2 != null) attest = p2.playerRequestPoToken;
                }
                final JsonNode p = webClient
                        ? webEmbedPlayer(videoId, visitorForRenewal, family, attest)
                        : androidPlayer(videoId, visitorForRenewal, family, attest, vrClient);
                final JsonNode sd2 = p.path("streamingData");
                final String url2 = sd2.path("serverAbrStreamingUrl").asText(null);
                final String ust2 = findFirst(p, "videoPlaybackUstreamerConfig");
                if (ust2 == null) return null;
                byte[] pot2 = null;
                if (bg != null) {
                    final String cb = bg.sabrContentBoundPoToken(videoId);
                    if (cb != null) pot2 = b64(cb);
                }
                return new SabrSession.Renewal(url2, b64(ust2), pot2);
            } catch (Exception e) {
                System.out.println("[Sabr] " + videoId + " Session-Erneuerung fehlgeschlagen: " + e.getMessage());
                return null;
            }
        });

        final String reattestBinding = System.getenv("YT_SABR_REATTEST_BINDING");
        final String sessionVisitor = visitorData;
        if (bg != null) session.setTokenRefresher(() -> {
            try {
                // Bindung waehlbar: "visitor" = an dieselbe visitorData wie der
                // Player-Call (Session-Identitaet), sonst an die videoId.
                final String cb = "visitor".equals(reattestBinding) && sessionVisitor != null
                        ? bg.sabrPoTokenForBinding(sessionVisitor)
                        : bg.sabrContentBoundPoToken(videoId);
                return cb != null ? b64(cb) : null;
            } catch (Exception e) {
                System.out.println("[Sabr] " + videoId + " Re-Attest-Mint fehlgeschlagen: " + e.getMessage());
                return null;
            }
        });
        final SabrSession.Result res = session.fetchAll(8000, sink, paced, publishHook);
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

    private static JsonNode webEmbedPlayer(String videoId, String visitorData, String family,
                                           String attestationPoToken) throws Exception {
        final Map<String, Object> client = new HashMap<>(Map.of(
                "clientName", "WEB_EMBEDDED_PLAYER", "clientVersion", WEB_EMBEDDED_VERSION,
                "clientScreen", "EMBED", "hl", "en", "gl", "US", "userAgent", WEB_UA));
        if (visitorData != null && !visitorData.isEmpty()) client.put("visitorData", visitorData);
        final Map<String, Object> req = new HashMap<>(Map.of(
                "context", Map.of("client", client,
                        "thirdParty", Map.of("embedUrl", "https://www.youtube.com/")),
                "videoId", videoId, "contentCheckOk", true, "racyCheckOk", true));
        if (attestationPoToken != null)
            req.put("serviceIntegrityDimensions", Map.of("poToken", attestationPoToken));
        final String body = Constants.mapper.writeValueAsString(req);
        final var r = rocks.kavin.reqwest4j.ReqwestUtils.fetchWithProxy(
                "https://www.youtube.com/youtubei/v1/player?prettyPrint=false",
                "POST", body.getBytes(StandardCharsets.UTF_8),
                Map.of("Content-Type", "application/json",
                        "Accept-Encoding", "identity",
                        "User-Agent", WEB_UA,
                        "X-Youtube-Client-Name", "56",
                        "X-Youtube-Client-Version", WEB_EMBEDDED_VERSION,
                        "Origin", "https://www.youtube.com",
                        "Referer", "https://www.youtube.com/"),
                family != null ? family : me.kavin.piped.utils.EgressManager.activeEgress())
                .get(20, java.util.concurrent.TimeUnit.SECONDS);
        if (r.status() / 100 != 2)
            throw new IllegalStateException("WEB_EMBEDDED player HTTP " + r.status());
        return Constants.mapper.readTree(r.body());
    }

    private static JsonNode androidPlayer(String videoId, String visitorData, String family,
                                          String attestationPoToken, boolean vrClient) throws Exception {
        final Map<String, Object> client = vrClient
                ? new HashMap<>(Map.of(
                        "clientName", "ANDROID_VR", "clientVersion", "1.62.27",
                        "androidSdkVersion", 32, "hl", "en", "gl", "US",
                        "osName", "Android", "osVersion", "12L", "userAgent", ANDROID_VR_UA))
                : new HashMap<>(Map.of(
                        "clientName", "ANDROID", "clientVersion", "20.10.38",
                        "androidSdkVersion", 34, "hl", "en", "gl", "US",
                        "osName", "Android", "osVersion", "14", "userAgent", ANDROID_UA));
        if (visitorData != null && !visitorData.isEmpty()) client.put("visitorData", visitorData);
        final Map<String, Object> req = new HashMap<>(Map.of(
                "context", Map.of("client", client),
                "videoId", videoId, "contentCheckOk", true, "racyCheckOk", true));
        // Attestation in the PLAYER request (serviceIntegrityDimensions): for
        // kids content the ustreamerConfig/abrUrl the player hands out limits
        // SABR readahead to one window unless the player call itself carried a
        // po_token — the streamerContext token alone is ignored there.
        if (attestationPoToken != null)
            req.put("serviceIntegrityDimensions", Map.of("poToken", attestationPoToken));
        final String body = Constants.mapper.writeValueAsString(req);
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
                            "User-Agent", vrClient ? ANDROID_VR_UA : ANDROID_UA,
                            "X-Youtube-Client-Name", vrClient ? "28" : "3",
                            "X-Youtube-Client-Version", vrClient ? "1.62.27" : "20.10.38"),
                    family).get(20, java.util.concurrent.TimeUnit.SECONDS);
            if (r.status() / 100 != 2)
                throw new IllegalStateException("ANDROID player HTTP " + r.status()
                        + " (egress=" + family + ")");
            resp = r.body();
        } else {
            resp = httpPost(
                    "https://www.youtube.com/youtubei/v1/player?prettyPrint=false",
                    body.getBytes(StandardCharsets.UTF_8), "application/json", true,
                    Map.of("User-Agent", vrClient ? ANDROID_VR_UA : ANDROID_UA,
                            "X-Youtube-Client-Name", vrClient ? "28" : "3",
                            "X-Youtube-Client-Version", vrClient ? "1.62.27" : "20.10.38"));
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
