package me.kavin.piped.utils;

import me.kavin.piped.consts.Constants;
import okhttp3.Request;
import okhttp3.Response;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static me.kavin.piped.consts.Constants.mapper;

/// Cold-tap UX (2026-06-27): when a resolve fails with NPE's generic
/// "Could not get any stream", the video is usually not a backend bug but
/// genuinely unplayable for anonymous viewers — members-only, private,
/// removed, geo-blocked, or age-gated. NPE's ANDROID/VR clients just return
/// zero streams in those cases without a clear reason. This probes the web
/// watch page's playabilityStatus and surfaces YouTube's OWN (localized)
/// reason text, so the app can show "Nur für Kanalmitglieder" instead of a
/// bare httpStatus(500).
public class YoutubeUnplayable {

    private static final String UA =
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            + "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

    // "playabilityStatus":{"status":"UNPLAYABLE","reason":"...."
    private static final Pattern STATUS =
            Pattern.compile("\"playabilityStatus\"\\s*:\\s*\\{\\s*\"status\"\\s*:\\s*\"([A-Z_]+)\"");
    // direct string reason right after the status
    private static final Pattern REASON_STR =
            Pattern.compile("\"status\"\\s*:\\s*\"[A-Z_]+\"\\s*,\\s*\"reason\"\\s*:\\s*\"([^\"]+)\"");
    // object reason: {"simpleText":"..."} or {"runs":[{"text":"..."}
    private static final Pattern REASON_SIMPLE =
            Pattern.compile("\"reason\"\\s*:\\s*\\{\\s*\"simpleText\"\\s*:\\s*\"([^\"]+)\"");
    private static final Pattern REASON_RUNS =
            Pattern.compile("\"reason\"\\s*:\\s*\\{\\s*\"runs\"\\s*:\\s*\\[\\s*\\{\\s*\"text\"\\s*:\\s*\"([^\"]+)\"");

    /// Returns YouTube's reason for an unplayable video, or null if the web
    /// page reports it as playable (= a genuine extraction failure, keep the
    /// 500) or the probe itself fails.
    /// Login-Cookies, NUR die fuer youtube.com. ⚠️ Ein Browser-Export enthaelt
    /// ALLE Domains; schickt man alles zusammen, antwortet Google mit 302 auf
    /// accounts.google.com/CookieMismatch und man ist wieder anonym.
    private static String youtubeCookieHeader() {
        String path = System.getenv("YOUTUBE_COOKIES_FILE");
        if (path == null || path.isEmpty()) path = "/app/youtube-cookies.txt";
        final java.io.File f = new java.io.File(path);
        if (!f.exists()) return null;
        final StringBuilder sb = new StringBuilder();
        try (java.io.BufferedReader r = new java.io.BufferedReader(new java.io.FileReader(f))) {
            String line;
            while ((line = r.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;
                final String[] parts = line.split("\\t");
                if (parts.length < 7 || !parts[0].contains("youtube.com")) continue;
                if (sb.length() > 0) sb.append("; ");
                sb.append(parts[5]).append('=').append(parts[6]);
            }
        } catch (Exception e) {
            return null;
        }
        return sb.length() == 0 ? null : sb.toString();
    }

    public static String probeReason(String videoId) {
        try {
            // ⚠️ MIT unseren Login-Cookies proben. Anonym sieht die Watch-Seite
            // die Bot-Wall („Melde dich an, damit wir sehen, dass du kein Bot
            // bist") — und dieser Grund wurde als Urteil „nicht abspielbar" an
            // die App gereicht, obwohl unser ANGEMELDETER Pfad das Video sehr
            // wohl bekommt (2026-07-31 an qBFvRSXjaEI: /sabr lieferte 206 in
            // 4 ms, waehrend die Variante 403 {"unplayable":true} zurueckgab).
            // Ein Grund, den nur ein anonymer Besucher sieht, taugt nicht als
            // Aussage ueber UNSERE Abspielbarkeit.
            final Request.Builder rb = new Request.Builder()
                    .url("https://www.youtube.com/watch?v=" + videoId + "&hl=de")
                    .header("User-Agent", UA)
                    .header("Accept-Language", "de,en;q=0.8");
            final String cookieHeader = youtubeCookieHeader();
            if (cookieHeader != null) rb.header("Cookie", cookieHeader);
            Request req = rb.build();
            try (Response resp = Constants.h2client.newCall(req).execute()) {
                if (!resp.isSuccessful() || resp.body() == null)
                    return null;
                String html = new String(resp.body().bytes(), java.nio.charset.StandardCharsets.UTF_8);
                Matcher st = STATUS.matcher(html);
                if (!st.find())
                    return null;
                String status = st.group(1);
                if ("OK".equals(status))
                    return null; // web says playable -> not a membership/availability block

                String reason = firstGroup(REASON_STR, html);
                if (reason == null) reason = firstGroup(REASON_SIMPLE, html);
                if (reason == null) reason = firstGroup(REASON_RUNS, html);
                if (reason != null && !reason.isBlank())
                    return jsonUnescape(reason);

                return switch (status) {
                    case "LOGIN_REQUIRED" -> "Dieses Video erfordert eine Anmeldung (alters- oder mitgliederbeschränkt).";
                    case "UNPLAYABLE" -> "Dieses Video ist nicht abspielbar.";
                    case "ERROR" -> "Dieses Video ist nicht verfügbar.";
                    case "CONTENT_CHECK_REQUIRED" -> "Dieses Video erfordert eine Bestätigung (sensibler Inhalt).";
                    default -> "Dieses Video ist nicht abspielbar (" + status + ").";
                };
            }
        } catch (Exception ignored) {
            return null;
        }
    }

    private static String firstGroup(Pattern p, String s) {
        Matcher m = p.matcher(s);
        return m.find() ? m.group(1) : null;
    }

    /// Decode JSON unicode/control escapes by re-parsing as a JSON string. Safe: the
    /// capture groups never contain an unescaped double-quote.
    private static String jsonUnescape(String raw) {
        try {
            return mapper.readValue("\"" + raw + "\"", String.class);
        } catch (Exception e) {
            return raw;
        }
    }
}
