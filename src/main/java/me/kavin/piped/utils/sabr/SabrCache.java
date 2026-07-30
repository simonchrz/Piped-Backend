package me.kavin.piped.utils.sabr;

import io.activej.http.HttpHeaderValue;
import io.activej.http.HttpHeaders;
import io.activej.http.HttpResponse;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/// SABR serving layer (Baustein 4). Backs /sabr/<videoId>/<itag>: on first hit
/// for a videoId it runs ONE SabrSession (download-once), writes the reassembled
/// fmp4 per itag (audio 140 + video 137) to the cache dir, then range-serves the
/// file. The HLS/sidx layer can point at these byte-range-addressable files
/// unchanged. Concurrent audio+video requests for the same video share the one
/// download (per-videoId lock).
public final class SabrCache {

    private static final Path DIR = ensureDir();
    private static final long MAX_CACHE_BYTES = 8L * 1024 * 1024 * 1024; // 8 GB
    private static final ConcurrentHashMap<String, Object> LOCKS = new ConcurrentHashMap<>();

    // ── storm-fallback marks ────────────────────────────────────────────────
    // videoIds whose direct googlevideo URLs are 403-storming (WebEmbed AND
    // TVHTML5 dead); synth-hls serves them via /sabr instead — stage 4 of the
    // resolve chain (StreamHandlers). TTL'd so a video returns to the normal
    // /yt-proxy path once the transient storm has passed.
    private static final ConcurrentHashMap<String, Long> STORM_MARKS = new ConcurrentHashMap<>();
    // 30 -> 10 min (2026-07-24): the googlevideo throttle on a video is
    // TRANSIENT and whole-URL — it comes and goes over minutes (verified: the
    // same video 403s everywhere, then serves 206 everywhere, then 403s again).
    // A 30-min mark held the capped SABR path far into recovered windows (yt-dlp
    // played the video fully while we kept SABR-ing). The self-healing drop on a
    // healthy resolve (StreamHandlers.clearStorm) handles the common case; this
    // shorter TTL bounds the worst case when no fresh resolve happens.
    private static final long STORM_TTL_MS = 10 * 60_000L;

    public static void markStorm(String videoId) {
        STORM_MARKS.put(videoId, System.currentTimeMillis() + STORM_TTL_MS);
    }

    public static boolean isStormMarked(String videoId) {
        final Long exp = STORM_MARKS.get(videoId);
        if (exp == null) return false;
        if (exp < System.currentTimeMillis()) {
            STORM_MARKS.remove(videoId);
            return false;
        }
        return true;
    }

    /// Drop a video's storm mark (StreamHandlers calls this when a fresh resolve
    /// produced healthy direct URLs without falling to SABR — the transient
    /// throttle window has recovered, so synth-hls should stop serving /sabr and
    /// return to the direct /yt-proxy URLs on its next poll).
    public static void clearStorm(String videoId) {
        STORM_MARKS.remove(videoId);
    }

    /// Ensures the video has been SABR-downloaded (once, per-videoId lock) and
    /// returns the cached file for the itag, or null if unavailable. Lets the
    /// synth-hls layer read the file directly (box scan) without an HTTP hop.
    public static Path ensureFile(String videoId, int itag) throws Exception {
        final Path file = DIR.resolve(safe(videoId) + "_" + itag + ".bin");
        if (!Files.exists(file)) {
            synchronized (LOCKS.computeIfAbsent(videoId, k -> new Object())) {
                // anyFileFor guard: if the session already ran but produced
                // DIFFERENT itags (video without 1080p avc), a request for the
                // absent itag must not re-trigger the whole download forever.
                if (!Files.exists(file) && !anyFileFor(videoId)) {
                    download(videoId);
                }
            }
        }
        return Files.exists(file) ? file : null;
    }

    /// The itags the SABR session ACTUALLY picked for this video, as
    /// {audio, video}. Ensures the download ran (once); reads the manifest
    /// download() writes next to the media files. Falls back to {140, 137}
    /// (the preferred picks) for pre-manifest cache entries.
    public static int[] itagsFor(String videoId) throws Exception {
        final Path manifest = DIR.resolve(safe(videoId) + ".itags");
        if (!Files.exists(manifest)) {
            synchronized (LOCKS.computeIfAbsent(videoId, k -> new Object())) {
                if (!Files.exists(manifest) && !anyFileFor(videoId)) {
                    download(videoId);
                }
            }
        }
        if (Files.exists(manifest)) {
            final String[] parts = Files.readString(manifest).trim().split("\\s+");
            if (parts.length == 2) {
                return new int[]{Integer.parseInt(parts[0]), Integer.parseInt(parts[1])};
            }
        }
        return new int[]{140, 137};
    }

    /// Liegt die Mediendatei schon auf Platte? Dann ist ein /sabr-Abruf reines
    /// Range-Lesen (kein YouTube-Resolve) und braucht KEINEN Resolve-Slot.
    /// ⚠️ Load-bearing (2026-07-25): die Playlist-Builder (/synth-hls) halten
    /// während `ensureFile` einen der nur zwei Slots — bei einem kalten SABR-Video
    /// belegen Video- + Audio-Playlist beide, und die anschliessenden Segment-
    /// Abrufe bekamen keinen mehr → 503 → AVPlayer -16849 mitten im Start.
    public static boolean isCached(String videoId, int itag) {
        final Path f = DIR.resolve(safe(videoId) + "_" + itag + ".bin");
        try {
            return Files.exists(f) && Files.size(f) > 0;
        } catch (IOException e) {
            return false;
        }
    }

    /// Liegt ueberhaupt SABR-Material zu diesem Video auf Platte? Erlaubt der
    /// synth-hls-Schicht, bei gesperrtem Resolve trotzdem aus dem Cache zu
    /// bedienen, statt „nicht abspielbar" zu zeigen.
    public static boolean hasCache(String videoId) {
        return anyFileFor(videoId);
    }

    /// Welche itags liegen tatsaechlich auf Platte? Nur fuer Diagnose.
    public static String cachedItags(String videoId) {
        final List<String> out = new ArrayList<>();
        try (var s = Files.newDirectoryStream(DIR, safe(videoId) + "_*.bin")) {
            for (Path p : s) {
                final String n = p.getFileName().toString();
                out.add(n.substring(n.lastIndexOf('_') + 1).replace(".bin", ""));
            }
        } catch (IOException ignored) {}
        return out.toString();
    }

    private static boolean anyFileFor(String videoId) {
        try (var s = Files.newDirectoryStream(DIR, safe(videoId) + "_*.bin")) {
            return s.iterator().hasNext();
        } catch (IOException e) {
            return false;
        }
    }

    public static HttpResponse handle(String videoId, int itag, String range, boolean head) throws Exception {
        final Path file = ensureFile(videoId, itag);
        if (file == null) {
            return HttpResponse.ofCode(502).withBody("sabr: no media for itag".getBytes());
        }
        try {
            Files.setLastModifiedTime(file, java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis()));
        } catch (IOException ignored) {}
        return serveFile(file, itag, range, head);
    }

    private static void download(String videoId) throws Exception {
        download(videoId, false);
    }

    // ── kids readahead-cap heal ─────────────────────────────────────────────
    // Videos whose burst SABR sessions hit the made-for-kids server-side
    // readahead cap (all token shapes, 2026-07-23 matrix). The async refill goes
    // straight to a PACED 1x session for these (burst rungs would just re-cap);
    // EXHAUSTED marks videos where even pacing yielded nothing new — the
    // playlist layer then closes the truncated playlist with ENDLIST so the
    // player cleanly plays the partial cache instead of erroring on a
    // never-growing live playlist (AVPlayer -12646).
    private static final ConcurrentHashMap<String, Long> CAPPED_MARKS = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Long> EXHAUSTED = new ConcurrentHashMap<>();
    private static final long CAP_MARK_TTL_MS = 30 * 60_000L;

    public static boolean isRefillExhausted(String videoId) {
        final Long exp = EXHAUSTED.get(videoId);
        if (exp == null) return false;
        if (exp < System.currentTimeMillis()) { EXHAUSTED.remove(videoId); return false; }
        return true;
    }

    /// Playlist-Layer: Teil-Cache als VOD BEENDEN statt EVENT-wachsend? True ab
    /// dem Kids-Cap-Verdict (nicht erst nach dem Paced-Fehlschlag) — der Tap
    /// während des Sturms spielt dann sofort sauber die vorhandenen Segmente,
    /// statt an einer (fast sicher) nie wachsenden Live-Playlist zu sterben.
    /// Wächst der Cache doch (Paced-Erfolg), liefert der nächste Playlist-Build
    /// automatisch mehr Segmente.
    public static boolean isPartialTerminal(String videoId) {
        return isCapMarked(videoId) || isRefillExhausted(videoId);
    }

    private static boolean isCapMarked(String videoId) {
        final Long exp = CAPPED_MARKS.get(videoId);
        if (exp == null) return false;
        if (exp < System.currentTimeMillis()) { CAPPED_MARKS.remove(videoId); return false; }
        return true;
    }

    /// Paced 1x-Refill: standardmässig AUS (`YT_SABR_PACED=1` schaltet ihn an).
    /// Die Hypothese „der Kids-Cap haengt am zu schnell vorlaufenden player_time"
    /// ist FALSIFIZIERT — jede gemessene paced Session (2026-07-24/25) endete mit
    /// „no net gain". Der Lauf kostet aber echtes Geld: er laeuft minutenlang,
    /// haelt den Per-Video-Lock, kopiert pro Runde die ganze Cache-Datei und
    /// konkurriert mit den Auslieferungs-Requests um die nur zwei Resolve-Slots.
    /// Code + Schalter bleiben als dokumentiertes Negativ-Ergebnis stehen.
    private static boolean pacedEnabled() {
        return "1".equals(System.getenv("YT_SABR_PACED"));
    }

    /// Client-Stufe der SABR-Leiter. DEFAULT = 3 (WEB) seit 2026-07-30.
    ///
    /// Begruendung (belegt, nicht vermutet): fuer den WEB-Client liefert YouTube
    /// zu Made-for-Kids-Videos GAR KEINE direkten Segment-URLs mehr (gemessen:
    /// 30 adaptiveFormats, 0 davon mit `url`, nur `serverAbrStreamingUrl`) — der
    /// Browser spielt sie ausschliesslich ueber SABR. Unsere Direkt-URLs kommen
    /// aus ANDROID_VR/WebEmbed, und genau die sterben im Drosselfenster mit 403.
    /// Der ANDROID-SABR-Pfad wiederum laeuft nach ~67 s in die Attestierung
    /// (STREAM_PROTECTION_STATUS 2->3, ~13 MB). Der WEB-Pfad laeuft seit den
    /// Fixes „Policy-Taktung" + „ehrliche buffered_ranges" durch: erster
    /// KOMPLETT-Download eines Kids-Videos (735/735 Segmente, 662 MB, prot=1).
    /// `YT_SABR_CLIENT=0` stellt den alten ANDROID-Default wieder her.
    private static final int DEFAULT_CLIENT = defaultClient();

    private static int defaultClient() {
        final String v = System.getenv("YT_SABR_CLIENT");
        if (v == null || v.isBlank()) return 3;
        try { return Integer.parseInt(v.trim()); }
        catch (NumberFormatException e) { return 3; }
    }

    /// WEB-Sperr-Memo (analog `KNOWN_THROTTLED` im Direktpfad). Wenn der
    /// WEB-Rung auf BEIDEN Egress-Familien hart 403t, ist das googlevideos
    /// Tagesfenster-Zustand und gilt fuer ALLE Videos — gemessen 2026-07-30:
    /// dasselbe Kontrollvideo lief 25 min vorher ueber WEB komplett durch.
    /// Ohne Memo kostet jeder Tap in einem geschlossenen Fenster zwei
    /// vergebliche Sessions inklusive Player-Call und Resolve-Slot (die
    /// Slot-Oekonomie hat nur zwei Plaetze — s. 503-Schleife). Selbstheilend
    /// per TTL. Kill-Switch `YT_SABR_WEB_MEMO=0`.
    private static final AtomicLong WEB_BLOCKED_UNTIL = new AtomicLong(0);
    private static final long WEB_MEMO_TTL_MS = 10 * 60 * 1000L;

    private static boolean webBlocked() {
        return !"0".equals(System.getenv("YT_SABR_WEB_MEMO"))
                && WEB_BLOCKED_UNTIL.get() > System.currentTimeMillis();
    }

    /// EINE Client-Stufe ueber beide Egress-Familien: Versuch auf der aktiven
    /// Familie, bei Fehlschlag/0 Segmenten einmal auf der anderen (resolve +
    /// Session sind familien-gepinnt, gvs-URLs sind IP-signiert). Es gewinnt der
    /// Versuch mit mehr Segmenten; keep-larger-publish sorgt dafuer, dass ein
    /// schwaecherer Versuch einen besseren Cache nie ueberschreibt.
    private static SabrHandlers.SabrMedia attemptBothFamilies(String videoId, String fam1, int clientMode) {
        SabrHandlers.SabrMedia r = attempt(videoId, fam1, false, clientMode);
        if (r != null && !(!r.complete()
                && ("SABR_ERROR".equals(r.stopReason()) || r.segments() == 0)))
            return r;
        final String fam2 = me.kavin.piped.utils.EgressManager.otherFamily();
        if (fam2.equals(fam1)) return r;
        System.out.println("[SabrCache] " + videoId + " attempt on " + fam1
                + (r == null ? " threw" : " " + r.stopReason() + " (segs=" + r.segments() + ")")
                + " -> family-retry on " + fam2);
        final SabrHandlers.SabrMedia r2 = attempt(videoId, fam2, false, clientMode);
        if (r2 != null && (r == null || r2.segments() > r.segments())) {
            System.out.println("[SabrCache] " + videoId + " family-retry " + fam2
                    + " won (segs=" + r2.segments() + " complete=" + r2.complete() + ")");
            return r2;
        }
        return r;
    }

    private static void download(String videoId, boolean allowPacedRequested) throws Exception {
        final boolean allowPaced = allowPacedRequested && pacedEnabled();
        // Known capped (kids): burst rungs are wasted requests — refill goes
        // straight to the paced 1x session; the sync warm path keeps serving
        // the existing partial cache untouched.
        if (isCapMarked(videoId)) {
            if (!allowPaced) return;
            final String fam = me.kavin.piped.utils.EgressManager.activeEgress();
            System.out.println("[SabrCache] " + videoId + " cap-marked -> paced 1x refill on " + fam);
            final long before = cachedBytes(videoId);
            final SabrHandlers.SabrMedia rp = attempt(videoId, fam, true, DEFAULT_CLIENT, true);
            finishPaced(videoId, rp, before);
            return;
        }
        // Stufe 1: der Default-Client (WEB) ueber beide Egress-Familien —
        // uebersprungen, solange das WEB-Memo steht (s. WEB_BLOCKED_UNTIL).
        final String fam1 = me.kavin.piped.utils.EgressManager.activeEgress();
        final boolean skipWeb = DEFAULT_CLIENT != 0 && webBlocked();
        if (skipWeb)
            System.out.println("[SabrCache] " + videoId
                    + " WEB-Memo aktiv -> direkt ANDROID (spart 2 Fehlversuche)");
        int usedClient = skipWeb ? 0 : DEFAULT_CLIENT;
        SabrHandlers.SabrMedia result = attemptBothFamilies(videoId, fam1, usedClient);
        // Stufe 2: Client-Rueckfall auf ANDROID. Es gab Fenster (2026-07-30
        // abends), in denen der WEB-Pfad auf BEIDEN Familien hart 403te, waehrend
        // ANDROID noch seine ~13 MB holte. Ein Teil-Cache ist besser als keiner,
        // also die alte Stufe nachziehen, wenn WEB gar nichts gebracht hat.
        if (!skipWeb && DEFAULT_CLIENT != 0 && (result == null || result.segments() == 0)) {
            // Beide Familien ohne ein einziges Segment = Fenster zu, nicht
            // video-spezifisch -> Memo setzen, damit die naechsten Taps direkt
            // auf ANDROID gehen.
            WEB_BLOCKED_UNTIL.set(System.currentTimeMillis() + WEB_MEMO_TTL_MS);
            System.out.println("[SabrCache] " + videoId + " client=" + DEFAULT_CLIENT
                    + " brachte nichts -> Rueckfall auf ANDROID (WEB-Memo 10min gesetzt)");
            final SabrHandlers.SabrMedia rA = attemptBothFamilies(videoId, fam1, 0);
            if (rA != null && (result == null || rA.segments() > result.segments())) {
                result = rA;
                usedClient = 0;
                System.out.println("[SabrCache] " + videoId + " ANDROID-Rueckfall gewann (segs="
                        + rA.segments() + " complete=" + rA.complete() + ")");
            }
        }
        if (result != null && !result.complete() && result.stopReason() != null
                && result.stopReason().startsWith("stuck")) {
            // Readahead-cap signature: data flowed, then the server stopped
            // sending despite advancing player_time — the visitorData-bound
            // po_token wasn't accepted for this video (made-for-kids does
            // this). Retry with a videoId-CONTENT-BOUND token, same family.
            System.out.println("[SabrCache] " + videoId + " capped at segs="
                    + result.segments() + " -> content-bound-token retry");
            final SabrHandlers.SabrMedia r3 = attempt(videoId, fam1, true, usedClient);
            if (r3 != null && r3.segments() > result.segments()) {
                result = r3;
                System.out.println("[SabrCache] " + videoId + " content-bound retry won"
                        + " (segs=" + r3.segments() + " complete=" + r3.complete() + ")");
            } else {
                // Burst rungs exhausted. 2026-07-23 experiment matrix on made-
                // for-kids content: content-bound streamerContext token, player-
                // request attestation, ANDROID_VR (UNPLAYABLE for kids) and
                // WEB_EMBEDDED (no serverAbrStreamingUrl at all) ALL leave the
                // one-window cap. 2026-07-24 hypothesis: the cap is relative to
                // player_time AND the server rejects player_time that outruns
                // wall-clock — so a PACED 1x session (real player emulation) is
                // the remaining rung; runs only in the async refill (takes
                // ~video duration), never in the sync warm path.
                System.out.println("[SabrCache] " + videoId
                        + " capped on both token shapes (kids-content signature)"
                        + (allowPaced ? " -> paced 1x refill" : " -> partial cache, paced refill pending"));
                CAPPED_MARKS.put(videoId, System.currentTimeMillis() + CAP_MARK_TTL_MS);
                if (allowPaced) {
                    final long before = cachedBytes(videoId);
                    final SabrHandlers.SabrMedia rp = attempt(videoId, fam1, true, usedClient, true);
                    if (rp != null && rp.segments() > result.segments()) result = rp;
                    finishPaced(videoId, rp, before);
                }
            }
        }
        if (result == null)
            throw new IllegalStateException("sabr: both family attempts failed for " + videoId);
        // Manifest with the ACTUAL picked itags so the serving layer doesn't
        // have to guess (videos without 1080p avc don't yield 137).
        Files.writeString(DIR.resolve(safe(videoId) + ".itags"),
                result.audioItag() + " " + result.videoItag());
        maybeEvict();
    }

    /// One SABR download attempt on an explicit egress family: streams each
    /// format to a fresh .part (Sink), then publishes .part -> .bin — but only
    /// when the .part is LARGER than any existing .bin, so a storm-crippled
    /// attempt (or refill) can't overwrite a better earlier cache. Returns the
    /// session result, or null when the session threw before finishing.
    private static SabrHandlers.SabrMedia attempt(String videoId, String family,
                                                  boolean contentBoundToken, int clientMode) {
        return attempt(videoId, family, contentBoundToken, clientMode, false);
    }

    private static SabrHandlers.SabrMedia attempt(String videoId, String family,
                                                  boolean contentBoundToken, int clientMode,
                                                  boolean paced) {
        final Map<Integer, Path> parts = new ConcurrentHashMap<>();
        final Map<Integer, OutputStream> opened = new ConcurrentHashMap<>();
        final SabrSession.Sink sink = itag -> {
            final Path part = DIR.resolve(safe(videoId) + "_" + itag + ".part");
            Files.deleteIfExists(part);
            final OutputStream os = new BufferedOutputStream(Files.newOutputStream(part), 1 << 20);
            parts.put(itag, part);
            opened.put(itag, os);
            return os;
        };
        // Paced sessions run ~video-length; publish the growing .part into the
        // served .bin once per round so the EVENT playlist keeps growing under
        // the player (that growth IS the point of pacing).
        final Runnable publishHook = !paced ? null : () -> {
            for (Map.Entry<Integer, Path> e : parts.entrySet())
                publishIfLarger(videoId, e.getKey(), e.getValue(), false);
        };
        // Protokoll-Probe (2026-07-25): erzwingt einen Client-Modus fuer die
        // Session, um die Client<->Token-Konsistenz zu testen (0=ANDROID,
        // 1=ANDROID_VR, 2=WEB_EMBEDDED). Unset = normales Verhalten.
        final String probeClient = System.getenv("YT_SABR_PROBE_CLIENT");
        final int effClientMode = probeClient != null
                ? Integer.parseInt(probeClient.trim()) : clientMode;
        if (probeClient != null)
            System.out.println("[Sabr] PROBE clientMode=" + effClientMode + " (erzwungen)");
        // Token-Bindung erzwingen. Der Browser benutzt fuer GVS einen
        // VIDEO-gebundenen Token (yt-dlp-Wiki: "Most PO Tokens (such as for web
        // GVS/Player) are bound to the video ID"), wir im ersten Versuch einen
        // visitorData-gebundenen. Client UND Bindung muessen zusammenpassen —
        // deshalb getrennt schaltbar, sonst testet man immer nur eine Haelfte.
        final String probeBind = System.getenv("YT_SABR_PROBE_CONTENT_BOUND");
        final boolean effContentBound = probeBind != null
                ? "1".equals(probeBind.trim()) : contentBoundToken;
        if (probeBind != null)
            System.out.println("[Sabr] PROBE contentBound=" + effContentBound + " (erzwungen)");
        SabrHandlers.SabrMedia result = null;
        try {
            result = SabrHandlers.runSession(videoId, sink, family, effContentBound, effClientMode, paced, publishHook);
        } catch (Exception e) {
            System.out.println("[SabrCache] " + videoId + " attempt(" + family + ") threw: " + e.getMessage());
        } finally {
            // the session closes the streams it was handed; this is defensive for
            // the error path (runSession throws before the session's finally runs).
            for (OutputStream os : opened.values()) { try { os.close(); } catch (IOException ignored) {} }
        }
        for (Map.Entry<Integer, Path> e : parts.entrySet())
            publishIfLarger(videoId, e.getKey(), e.getValue(), true);
        return result;
    }

    /// keep-larger publish .part -> .bin. move=true consumes the part (terminal
    /// publish); move=false copies (incremental — the session keeps appending to
    /// the part) via temp + ATOMIC_MOVE, so concurrent range-readers never see a
    /// half-written bin (rename keeps old-inode readers intact on POSIX).
    private static void publishIfLarger(String videoId, int itag, Path part, boolean move) {
        final Path bin = DIR.resolve(safe(videoId) + "_" + itag + ".bin");
        try {
            final long partSize = Files.exists(part) ? Files.size(part) : 0;
            final long binSize = Files.exists(bin) ? Files.size(bin) : 0;
            if (partSize > binSize) {
                if (move) {
                    Files.move(part, bin, StandardCopyOption.REPLACE_EXISTING);
                } else {
                    final Path tmp = DIR.resolve(safe(videoId) + "_" + itag + ".pub");
                    Files.copy(part, tmp, StandardCopyOption.REPLACE_EXISTING);
                    Files.move(tmp, bin, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                }
            } else if (move) {
                Files.deleteIfExists(part);
            }
        } catch (IOException ignored) {}
    }

    /// Summe der servierten .bin-Bytes eines Videos — Netto-Gewinn-Messung für
    /// den Paced-Verdict (Session-Segmentzähler zählt auch re-downloads).
    private static long cachedBytes(String videoId) {
        long n = 0;
        try (var s = Files.list(DIR)) {
            for (Path p : s.filter(f -> f.getFileName().toString().startsWith(safe(videoId) + "_")
                    && f.getFileName().toString().endsWith(".bin")).toList()) {
                try { n += Files.size(p); } catch (IOException ignored) {}
            }
        } catch (IOException ignored) {}
        return n;
    }

    /// Verdict nach einem Paced-Refill: complete -> Marks löschen (geheilt);
    /// kein Netto-Byte-Gewinn -> EXHAUSTED (Playlist-Layer schließt die
    /// Teil-Playlist mit ENDLIST); Teil-Fortschritt -> Marks behalten, ein
    /// späterer Refill paced weiter.
    private static void finishPaced(String videoId, SabrHandlers.SabrMedia r, long beforeBytes) {
        final long after = cachedBytes(videoId);
        if (r != null && r.complete()) {
            CAPPED_MARKS.remove(videoId);
            EXHAUSTED.remove(videoId);
            System.out.println("[SabrCache] " + videoId + " paced refill COMPLETE (" + after + "B)");
        } else if (after <= beforeBytes) {
            EXHAUSTED.put(videoId, System.currentTimeMillis() + CAP_MARK_TTL_MS);
            System.out.println("[SabrCache] " + videoId + " paced refill no net gain ("
                    + beforeBytes + "B -> " + after + "B) -> EXHAUSTED, partial ENDLIST");
        } else {
            System.out.println("[SabrCache] " + videoId + " paced refill partial gain ("
                    + beforeBytes + "B -> " + after + "B), marks kept");
        }
    }

    // ── refill (incomplete cache heal) ──────────────────────────────────────
    // The synth-hls playlist layer calls requestRefill when the sidx promises
    // more bytes than the cache file holds (= a storm truncated the download).
    // Async + per-video cooldown so playlist polls don't stack sessions; the
    // keep-larger publish in attempt() makes refills monotonic.
    private static final ConcurrentHashMap<String, Long> REFILL_LAST = new ConcurrentHashMap<>();
    private static final long REFILL_COOLDOWN_MS = 5 * 60_000L;

    private static final Set<String> REFILL_ACTIVE = ConcurrentHashMap.newKeySet();

    public static void requestRefill(String videoId) {
        if (isRefillExhausted(videoId)) return;          // paced verdict: nichts zu holen
        // Cap-markiert + paced aus = es gaebe nur eine weitere Burst-Session, die
        // (gemessen) nichts holt. Spart Slots/IO auf genau den Videos, die die
        // Kinder am haeufigsten antippen.
        if (isCapMarked(videoId) && !pacedEnabled()) return;
        if (REFILL_ACTIVE.contains(videoId)) return;     // Session läuft bereits
        final long now = System.currentTimeMillis();
        final boolean[] go = {false};
        REFILL_LAST.compute(videoId, (k, prev) -> {
            if (prev != null && now - prev < REFILL_COOLDOWN_MS) return prev;
            go[0] = true;
            return now;
        });
        if (!go[0]) return;
        if (!REFILL_ACTIVE.add(videoId)) return;
        Thread.ofVirtual().name("sabr-refill-" + videoId).start(() -> {
            synchronized (LOCKS.computeIfAbsent(videoId, k -> new Object())) {
                try {
                    System.out.println("[SabrCache] " + videoId + " refill (incomplete cache)");
                    download(videoId, true);
                } catch (Exception e) {
                    System.out.println("[SabrCache] " + videoId + " refill failed: " + e.getMessage());
                } finally {
                    REFILL_ACTIVE.remove(videoId);
                }
            }
        });
    }

    private static HttpResponse serveFile(Path file, int itag, String range, boolean head) throws IOException {
        final long total = Files.size(file);
        final String ct = itag == 140 ? "audio/mp4" : "video/mp4";
        final boolean noRange = range == null || range.isEmpty();
        if (noRange) {
            final HttpResponse r = HttpResponse.ofCode(200)
                    .withHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValue.of(ct))
                    .withHeader(HttpHeaders.CONTENT_LENGTH, HttpHeaderValue.of(String.valueOf(total)))
                    .withHeader(HttpHeaders.ACCEPT_RANGES, HttpHeaderValue.of("bytes"));
            return head ? r : r.withBody(Files.readAllBytes(file));
        }
        final long[] se = parseRange(range, total);
        if (se == null) return HttpResponse.ofCode(416);
        final long start = se[0];
        final long end = se[1];
        final HttpResponse r = HttpResponse.ofCode(206)
                .withHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValue.of(ct))
                .withHeader(HttpHeaders.CONTENT_RANGE, HttpHeaderValue.of("bytes " + start + "-" + end + "/" + total))
                .withHeader(HttpHeaders.CONTENT_LENGTH, HttpHeaderValue.of(String.valueOf(end - start + 1)))
                .withHeader(HttpHeaders.ACCEPT_RANGES, HttpHeaderValue.of("bytes"));
        return head ? r : r.withBody(readRange(file, start, end));
    }

    private static long[] parseRange(String r, long total) {
        if (r == null || !r.startsWith("bytes=")) return null;
        try {
            final String body = r.substring(6);
            final int dash = body.indexOf('-');
            if (dash < 0) return null;
            final String s = body.substring(0, dash).trim();
            final String e = body.substring(dash + 1).trim();
            long start = s.isEmpty() ? 0 : Long.parseLong(s);
            long end = e.isEmpty() ? total - 1 : Long.parseLong(e);
            if (end >= total) end = total - 1;
            if (start < 0 || start > end) return null;
            return new long[]{start, end};
        } catch (Exception ex) {
            return null;
        }
    }

    private static byte[] readRange(Path file, long start, long end) throws IOException {
        final int len = (int) (end - start + 1);
        final byte[] buf = new byte[len];
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {
            raf.seek(start);
            raf.readFully(buf);
        }
        return buf;
    }

    private static long lastEvict = 0;
    private static synchronized void maybeEvict() {
        final long now = System.currentTimeMillis();
        if (now - lastEvict < 30_000) return;
        lastEvict = now;
        try {
            long total = 0;
            final List<Path> files = new ArrayList<>();
            try (var stream = Files.list(DIR)) {
                for (Path p : (Iterable<Path>) stream::iterator) {
                    if (p.getFileName().toString().endsWith(".bin")) files.add(p);
                }
            }
            for (Path p : files) total += Files.size(p);
            if (total <= MAX_CACHE_BYTES) return;
            files.sort((a, b) -> {
                try {
                    return Long.compare(Files.getLastModifiedTime(a).toMillis(),
                            Files.getLastModifiedTime(b).toMillis());
                } catch (IOException ex) {
                    return 0;
                }
            });
            for (Path p : files) {
                if (total <= MAX_CACHE_BYTES * 8 / 10) break;
                try {
                    final long sz = Files.size(p);
                    Files.delete(p);
                    total -= sz;
                } catch (IOException ignored) {}
            }
        } catch (IOException ignored) {}
    }

    private static String safe(String s) {
        return s.replaceAll("[^A-Za-z0-9._-]", "_");
    }

    private static Path ensureDir() {
        final String env = System.getenv("SABR_CACHE_DIR");
        final Path dir = Paths.get(env != null && !env.isEmpty() ? env : "/app/sabr-cache");
        try {
            Files.createDirectories(dir);
            try (var stream = Files.list(dir)) {
                for (Path p : (Iterable<Path>) stream::iterator) {
                    final String n = p.getFileName().toString();
                    if (n.endsWith(".tmp") || n.endsWith(".part")) {   // orphaned partial downloads
                        try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("[SabrCache] dir setup failed: " + e.getMessage());
        }
        return dir;
    }
}
