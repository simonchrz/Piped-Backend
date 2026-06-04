package me.kavin.piped.utils;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.schabi.newpipe.extractor.downloader.Downloader;
import org.schabi.newpipe.extractor.downloader.Request;
import org.schabi.newpipe.extractor.downloader.Response;
import org.schabi.newpipe.extractor.exceptions.ReCaptchaException;
import rocks.kavin.reqwest4j.ReqwestUtils;

import java.io.IOException;
import java.net.HttpCookie;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DownloaderImpl extends Downloader {

    private static HttpCookie saved_cookie;
    private static long cookie_received;
    private static final Object cookie_lock = new Object();

    // Custom: YouTube cookies loaded from /app/youtube-cookies.txt at startup
    private static final String YOUTUBE_COOKIES = loadYoutubeCookies();

    private static String loadYoutubeCookies() {
        String path = System.getenv("YOUTUBE_COOKIES_FILE");
        if (path == null || path.isEmpty()) path = "/app/youtube-cookies.txt";
        java.io.File f = new java.io.File(path);
        if (!f.exists()) {
            System.out.println("[Piped] No youtube-cookies.txt at " + path);
            return null;
        }
        StringBuilder sb = new StringBuilder();
        try (java.io.BufferedReader r = new java.io.BufferedReader(new java.io.FileReader(f))) {
            String line;
            while ((line = r.readLine()) != null) {
                if (line.startsWith("#") || line.isBlank()) continue;
                String[] parts = line.split("\\t");
                if (parts.length >= 7) {
                    if (sb.length() > 0) sb.append("; ");
                    sb.append(parts[5]).append("=").append(parts[6]);
                }
            }
        } catch (java.io.IOException e) {
            System.err.println("[Piped] Failed to load youtube-cookies.txt: " + e.getMessage());
            return null;
        }
        if (sb.length() == 0) return null;
        System.out.println("[Piped] Loaded YouTube cookies (" + sb.length() + " bytes)");
        return sb.toString();
    }

    /**
     * Executes a request with HTTP/2.
     */
    @Override
    public Response execute(Request request) throws IOException, ReCaptchaException {

        // TODO: HTTP/3 aka QUIC
        var bytes = request.dataToSend();
        Map<String, String> headers = new Object2ObjectOpenHashMap<>();

        if (saved_cookie != null && !saved_cookie.hasExpired())
            headers.put("Cookie", saved_cookie.getName() + "=" + saved_cookie.getValue());

        // Custom: attach YouTube cookies to requests targeting YouTube hosts.
        // EXCEPT the public base.js page scrapes (iframe_api / embed / watch HTML):
        // with account cookies YouTube 302-redirects those to a consent/account
        // flow (0-byte body), so YoutubeJavaScriptExtractor can't find the base.js
        // URL and the WebEmbed resolve dies ("...didn't provide base player's URL").
        // These pages are public and must be fetched anonymously; cookies belong on
        // the segment (googlevideo) + innertube API requests, not the page scrapes.
        if (YOUTUBE_COOKIES != null) {
            String url = request.url();
            final boolean baseJsPageScrape = url.contains("/iframe_api")
                    || url.contains("/embed/")
                    || url.contains("/watch");
            if (!baseJsPageScrape
                    && (url.contains("youtube.com") || url.contains("googlevideo.com")
                        || url.contains("ytimg.com"))) {
                String existing = headers.get("Cookie");
                headers.put("Cookie", existing != null ? existing + "; " + YOUTUBE_COOKIES : YOUTUBE_COOKIES);
            }
        }

        request.headers().forEach((name, values) -> values.forEach(value -> headers.put(name, value)));

        // Custom: override User-Agent to recent Chrome for YouTube requests
        // â YouTube prueft Cookie/UA-Konsistenz, Firefox-UA mit Chrome-Cookies = invalid.
        if (YOUTUBE_COOKIES != null) {
            String urlForUa = request.url();
            if (urlForUa.contains("youtube.com") || urlForUa.contains("googlevideo.com") || urlForUa.contains("ytimg.com")) {
                headers.put("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36");
            }
        }

        var future = ReqwestUtils.fetch(request.url(), request.httpMethod(), bytes, headers);

        // Recaptcha solver code
        // Commented out, as it hasn't been ported to reqwest4j yet
        // Also, this was last seen a long time back

//        future.thenAcceptAsync(resp -> {
//            if (resp.status() == 429) {
//                synchronized (cookie_lock) {
//
//                    if (saved_cookie != null && saved_cookie.hasExpired()
//                            || (System.currentTimeMillis() - cookie_received > TimeUnit.MINUTES.toMillis(30)))
//                        saved_cookie = null;
//
//                    String redir_url = String.valueOf(resp.finalUrl());
//
//                    if (saved_cookie == null && redir_url.startsWith("https://www.google.com/sorry")) {
//
//                        var formBuilder = new FormBody.Builder();
//                        String sitekey = null, data_s = null;
//
//                        for (Element el : Jsoup.parse(new String(resp.body())).selectFirst("form").children()) {
//                            String name;
//                            if (!(name = el.tagName()).equals("script")) {
//                                if (name.equals("input"))
//                                    formBuilder.add(el.attr("name"), el.attr("value"));
//                                else if (name.equals("div") && el.attr("id").equals("recaptcha")) {
//                                    sitekey = el.attr("data-sitekey");
//                                    data_s = el.attr("data-s");
//                                }
//                            }
//                        }
//
//                        if (StringUtils.isEmpty(sitekey) || StringUtils.isEmpty(data_s))
//                            ExceptionHandler.handle(new ReCaptchaException("Could not get recaptcha", redir_url));
//
//                        SolvedCaptcha solved = null;
//
//                        try {
//                            solved = CaptchaSolver.solve(redir_url, sitekey, data_s);
//                        } catch (JsonParserException | InterruptedException | IOException e) {
//                            e.printStackTrace();
//                        }
//
//                        formBuilder.add("g-recaptcha-response", solved.getRecaptchaResponse());
//
//                        var formReqBuilder = new okhttp3.Request.Builder()
//                                .url("https://www.google.com/sorry/index")
//                                .header("User-Agent", Constants.USER_AGENT)
//                                .post(formBuilder.build());
//
//                        okhttp3.Response formResponse;
//                        try {
//                            formResponse = Constants.h2_no_redir_client.newCall(formReqBuilder.build()).execute();
//                        } catch (IOException e) {
//                            throw new RuntimeException(e);
//                        }
//
//                        saved_cookie = HttpCookie.parse(URLUtils.silentDecode(StringUtils
//                                        .substringAfter(formResponse.headers().get("Location"), "google_abuse=")))
//                                .get(0);
//                        cookie_received = System.currentTimeMillis();
//                    }
//                }
//            }
//        }, Multithreading.getCachedExecutor());

        var responseFuture = future.thenApplyAsync(resp -> {
            Map<String, List<String>> headerMap = resp.headers().entrySet().stream()
                    .collect(Object2ObjectOpenHashMap::new, (m, e) -> m.put(e.getKey(), List.of(e.getValue())), Map::putAll);

            return new Response(resp.status(), null, headerMap, new String(resp.body()),
                    resp.finalUrl());
        }, Multithreading.getCachedExecutor());

        try {
            return responseFuture.get(10, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new IOException(e);
        }
    }
}
