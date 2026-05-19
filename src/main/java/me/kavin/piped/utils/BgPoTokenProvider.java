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

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    private String getWebVisitorData() throws Exception {
        var html = RequestUtils.sendGet("https://www.youtube.com").get();
        var matcher = Pattern.compile("visitorData\":\"([\\w%-]+)\"").matcher(html);

        if (matcher.find()) {
            return matcher.group(1);
        }

        throw new RuntimeException("Failed to get visitor data");
    }

    private final Queue<PoTokenResult> validPoTokens = new ConcurrentLinkedQueue<>();

    private PoTokenResult getPoTokenPooled() throws Exception {
        PoTokenResult poToken = validPoTokens.poll();

        if (poToken == null) {
            poToken = createWebClientPoToken();
        }

        // if still null, return null
        if (poToken == null) {
            return null;
        }

        // timer to insert back into queue after 10 + random seconds
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
