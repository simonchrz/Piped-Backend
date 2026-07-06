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
    // SAPISID for the TVHTML5 (TV) client's SAPISIDHASH Authorization header.
    // The TV client requires real account auth (not just the Cookie header) or
    // YouTube bot-walls it (LOGIN_REQUIRED). WEB_EMBEDDED is cookie-only, so this
    // is scoped to TVHTML5 (Cobalt UA) requests below.
    private static final String SAPISID = cookieVal(YOUTUBE_COOKIES, "SAPISID");
    private static final String SAPISID_1P = cookieVal(YOUTUBE_COOKIES, "__Secure-1PAPISID");
    private static final String SAPISID_3P = cookieVal(YOUTUBE_COOKIES, "__Secure-3PAPISID");

    private static String cookieVal(String cookieStr, String want) {
        if (cookieStr == null) return null;
        for (String c : cookieStr.split("; ")) {
            int eq = c.indexOf('=');
            if (eq <= 0) continue;
            if (c.substring(0, eq).equals(want)) return c.substring(eq + 1);
        }
        return null;
    }

    private static String sha1Hex(String in) {
        try {
            byte[] dig = java.security.MessageDigest.getInstance("SHA-1")
                    .digest(in.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(dig.length * 2);
            for (byte b : dig) hex.append(Character.forDigit((b >> 4) & 0xF, 16))
                                  .append(Character.forDigit(b & 0xF, 16));
            return hex.toString();
        } catch (Exception e) { return null; }
    }

    /// Combined SID Authorization header, replicating yt-dlp's
    /// _get_sid_authorization_header: one space-joined header with a
    /// SAPISIDHASH / SAPISID1PHASH / SAPISID3PHASH part per available cookie,
    /// each "<scheme> <ts>_<sha1(ts + ' ' + sid + ' ' + origin)>". The TV client
    /// bot-wall only clears with all available parts, not SAPISIDHASH alone.
    private static String sapisidHashAuth(String userSessionId) {
        long ts = System.currentTimeMillis() / 1000L;
        String origin = "https://www.youtube.com";
        // yt-dlp folds the user session id into each hash as the "u" additional
        // part: hash over "<usid> <ts> <sid> <origin>" and a trailing "_u".
        String prefix = userSessionId != null ? userSessionId + " " : "";
        String suffix = userSessionId != null ? "_u" : "";
        StringBuilder out = new StringBuilder();
        String[][] schemes = {
            {"SAPISIDHASH", SAPISID},
            {"SAPISID1PHASH", SAPISID_1P},
            {"SAPISID3PHASH", SAPISID_3P},
        };
        for (String[] sc : schemes) {
            String sid = sc[1];
            if (sid == null) continue;
            String h = sha1Hex(prefix + ts + " " + sid + " " + origin);
            if (h == null) continue;
            if (out.length() > 0) out.append(' ');
            out.append(sc[0]).append(' ').append(ts).append('_').append(h).append(suffix);
        }
        return out.length() == 0 ? null : out.toString();
    }

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
            // The TVHTML5 (TV/Cobalt) client sets its own Cobalt User-Agent and
            // YouTube validates clientName<->UA consistency: forcing Chrome here
            // turns a TVHTML5 request into a mismatch -> LOGIN_REQUIRED bot-wall.
            // So preserve an already-set Cobalt UA; force Chrome only otherwise.
            String existingUa = headers.get("User-Agent");
            boolean isCobaltUa = existingUa != null && existingUa.contains("Cobalt");
            if (!isCobaltUa
                    && (urlForUa.contains("youtube.com") || urlForUa.contains("googlevideo.com") || urlForUa.contains("ytimg.com"))) {
                headers.put("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36");
            }
            // TVHTML5 (Cobalt UA) needs SAPISIDHASH account auth, not just the
            // Cookie header, to clear the TV-client bot-wall. Scoped here so the
            // cookie-only WEB_EMBEDDED auth posture is unchanged.
            if (isCobaltUa && urlForUa.contains("youtube.com")
                    && !headers.containsKey("Authorization")) {
                // user session id is handed over by the TV helper via this
                // private header; consume + strip it so it never leaves the box.
                String userSessionId = headers.remove("X-Yt-Auth-Session");
                String auth = sapisidHashAuth(userSessionId);
                if (auth != null) {
                    headers.put("Authorization", auth);
                    headers.put("X-Origin", "https://www.youtube.com");
                }
            }
        }

        var future = ReqwestUtils.fetchWithProxy(request.url(), request.httpMethod(), bytes, headers, EgressManager.activeEgress());

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

        Response response;
        try {
            response = responseFuture.get(10, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new IOException(e);
        }

        // Custom: truncated-body guard for HTML page scrapes. The transport
        // can deliver a partial body as if complete (observed 2026-07-06:
        // embed pages cut near 32 KB losing the ytcfg fields past that
        // offset — same silent-truncation class as the Mac source cache).
        // A body that STARTS as an HTML document but doesn't END with
        // </html> is incomplete: refetch once. JSON/JS responses never
        // match the prefix, so innertube/base.js are untouched. On a
        // second short read the response is returned as-is — consumers
        // (e.g. WebEmbedModern's field check) keep their own backstops.
        if (isTruncatedHtml(response)) {
            System.out.println("[Downloader] truncated HTML from " + request.url()
                    + " (size=" + response.responseBody().length() + ") -> refetch");
            try {
                Thread.sleep(500);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IOException(ie);
            }
            var retryFuture = ReqwestUtils.fetchWithProxy(request.url(), request.httpMethod(),
                            bytes, headers, EgressManager.activeEgress())
                    .thenApplyAsync(resp -> {
                        Map<String, List<String>> headerMap = resp.headers().entrySet().stream()
                                .collect(Object2ObjectOpenHashMap::new, (m, e) -> m.put(e.getKey(), List.of(e.getValue())), Map::putAll);
                        return new Response(resp.status(), null, headerMap, new String(resp.body()),
                                resp.finalUrl());
                    }, Multithreading.getCachedExecutor());
            try {
                response = retryFuture.get(10, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new IOException(e);
            }
            if (isTruncatedHtml(response)) {
                System.out.println("[Downloader] STILL truncated after refetch: " + request.url()
                        + " (size=" + response.responseBody().length() + ")");
            }
        }
        return response;
    }

    private static boolean isTruncatedHtml(Response response) {
        if (response.responseCode() < 200 || response.responseCode() >= 300)
            return false;
        String body = response.responseBody();
        if (body == null || body.isEmpty())
            return false;
        String head = body.substring(0, Math.min(body.length(), 256)).stripLeading().toLowerCase();
        if (!(head.startsWith("<!doctype html") || head.startsWith("<html")))
            return false;
        int tailFrom = Math.max(0, body.length() - 256);
        return !body.substring(tailFrom).stripTrailing().toLowerCase().contains("</html>");
    }
}
