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

    /// Streifen-Modus: Segmente an ihre sidx-Position schreiben statt fortlaufend
    /// (s. SparseStore). Voraussetzung dafuer, dass der Player an BELIEBIGE
    /// Stellen springen kann. Default AUS, bis verifiziert.
    public static final boolean SPARSE = "1".equals(System.getenv("SABR_SPARSE"));

    static Path dir() { return DIR; }
    static String safeId(String videoId) { return safe(videoId); }
    /// Cache-Deckel. Der Wert stammt aus der Zeit der 13-MB-Teilcaches; seit dem
    /// WEB-Pfad kostet EIN Kids-Video ~724 MB, 8 GB sind also nur noch ~11
    /// Videos. Stellbar per `SABR_CACHE_MAX_GB` — die eigentliche Sicherung ist
    /// aber MIN_FREE_BYTES (freier Plattenplatz), s. maybeEvict.
    private static final long MAX_CACHE_BYTES = envGb("SABR_CACHE_MAX_GB", 8);
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
    /// Laeuft fuer dieses Video gerade ein Download? Verhindert, dass Audio- und
    /// Video-Abruf zwei Sessions starten, und sagt der Wartelogik, ob sich noch
    /// etwas tut.
    private static final Set<String> DOWNLOAD_ACTIVE = ConcurrentHashMap.newKeySet();

    /// Wie viele SABR-Downloads gleichzeitig laufen duerfen.
    ///
    /// ⚠️ Load-bearing seit dem WEB-Pfad. Vorher endete jede Kids-Session nach
    /// ~13 MB, ein Schwall paralleler Sessions war also harmlos. Jetzt laedt
    /// JEDE Session das KOMPLETTE Video (~700 MB) — und die App laedt beim
    /// Scrollen mehrere Videos vor. Gemessen 2026-07-31: vier gleichzeitige
    /// Voll-Downloads, Load 8,0 auf vier Kernen, und der echte Tap des Nutzers
    /// scheiterte davor am Resolve („exceeded 12s budget") → HTTP 500 in der App.
    /// Ein Download nach dem anderen: die Warteschlange kostet nichts, weil
    /// ensureFile ohnehin nur auf den ANFANG wartet und danach ausliefert.
    /// `SABR_MAX_PARALLEL_DOWNLOADS` stellt den Wert um.
    private static final java.util.concurrent.Semaphore DOWNLOAD_SLOTS =
            new java.util.concurrent.Semaphore(maxParallelDownloads(), true);

    // ── Bedarfsgetriebener Download ────────────────────────────────────────
    // Ein Voll-Download „auf Verdacht" ist falsch: AVPlayer sagt uns per
    // Range-Requests laufend, wo er steht, und wenn das Video weggeklickt wird,
    // hoeren die Anfragen auf. Vorher lud jede Session stur bis zum Ende — beim
    // Scroll-Prefetch der App liefen dadurch VIER Voll-Downloads gleichzeitig
    // (Load 8,0 auf vier Kernen, 2026-07-31), und der echte Tap scheiterte davor
    // am Resolve-Budget (HTTP 500 in der App). Jetzt gilt:
    //   • Vorlauf halten: LEAD_BYTES ueber dem hoechsten angeforderten Offset,
    //     danach pausiert die Session (kostet nichts).
    //   • Aufhoeren, wenn IDLE_STOP_MS lang niemand mehr angefordert hat —
    //     also z.B. beim Wegklicken oder bei einem nie angesehenen Prefetch.
    // Der Teil-Cache bleibt in beiden Faellen erhalten; ein spaeterer Abruf
    // setzt fort, weil `.part`/`.bin` per keep-larger weitergefuehrt werden.
    private static final Map<String, Long> DEMAND_OFFSET = new ConcurrentHashMap<>();
    private static final Map<String, Long> LAST_REQUEST = new ConcurrentHashMap<>();

    /// Wieviel Vorlauf vor der Abspielposition gehalten wird. 48 MB sind bei
    /// 1080p rund 3–4 Minuten — genug, dass Puffern und Vorspulen sich nicht
    /// anfuehlen wie Nachladen, und weit weniger als ein ganzes Video.
    private static final long LEAD_BYTES = envMb("SABR_LEAD_MB", 48);
    /// So lange ohne Anforderung -> Sitzung beenden.
    private static final long IDLE_STOP_MS = 90_000;

    private static long envMb(String name, long defMb) {
        final String v = System.getenv(name);
        long mb = defMb;
        if (v != null && !v.isBlank()) {
            try { mb = Long.parseLong(v.trim()); } catch (NumberFormatException ignored) {}
        }
        return mb * 1024L * 1024;
    }

    static {
        SabrHandlers.SESSION_STOP = vid -> () -> {
            final long last = LAST_REQUEST.getOrDefault(vid, 0L);
            return last > 0 && System.currentTimeMillis() - last > IDLE_STOP_MS;
        };
        SabrHandlers.SESSION_PAUSE = vid -> () -> {
            // Streifen-Modus: es gibt keine .part mehr — der Vorlauf wird an der
            // Anwesenheitskarte gemessen, ab dem Segment, das der Player gerade
            // braucht.
            if (SPARSE) {
                final int[] itags;
                try { itags = itagsForCached(vid); } catch (Exception e) { return false; }
                if (itags == null) return false;
                final int vItag = itags[1];
                if (!SparseStore.ensureOffsets(vid, vItag)) return false;
                final long demand = DEMAND_OFFSET.getOrDefault(safe(vid) + "_" + vItag, 0L);
                int fromSeq = SparseStore.segmentAt(vid, vItag, demand);
                if (fromSeq < 1) fromSeq = 1;
                return SparseStore.leadSatisfied(vid, vItag, fromSeq, LEAD_BYTES);
            }
            // Der Vorlauf haengt an der VIDEO-Spur — sie macht ~95% der Bytes
            // aus, und Audio laeuft im selben Zug mit.
            // ⚠️ NICHT „jede Spur braucht LEAD_BYTES": die Audiospur ist oft
            // KLEINER als der Vorlauf (3,4 MB Datei vs. 6 MB Soll), damit waere
            // die Bedingung nie erfuellt und es wuerde wieder alles geladen —
            // genau so gemessen 2026-07-31 beim ersten Versuch.
            try (var s = Files.newDirectoryStream(DIR, safe(vid) + "_*.part")) {
                for (Path p : s) {
                    final String n = p.getFileName().toString();
                    final int itag;
                    try {
                        itag = Integer.parseInt(n.substring(n.lastIndexOf('_') + 1).replace(".part", ""));
                    } catch (NumberFormatException e) { continue; }
                    if (isAudioItag(itag)) continue;
                    final long have = Files.size(p);
                    final long want = DEMAND_OFFSET.getOrDefault(safe(vid) + "_" + itag, 0L) + LEAD_BYTES;
                    return have >= want;
                }
                return false;   // noch keine Videospur -> weiterladen
            } catch (IOException e) {
                return false;
            }
        };
    }

    /// Bis zu welchem Byte will der Client? Ohne Range-Header (oder bei offenem
    /// Ende) zaehlt der Start plus ein Fenster — mehr braucht er in dieser
    /// Antwort ohnehin nicht (s. MAX_RESPONSE_BYTES).
    private static long requestedEnd(String range) {
        if (range == null || !range.startsWith("bytes=")) return MAX_RESPONSE_BYTES;
        try {
            final String body = range.substring(6);
            final int dash = body.indexOf('-');
            if (dash < 0) return MAX_RESPONSE_BYTES;
            final String s = body.substring(0, dash).trim();
            final String e = body.substring(dash + 1).trim();
            final long start = s.isEmpty() ? 0 : Long.parseLong(s);
            return e.isEmpty() ? start + MAX_RESPONSE_BYTES : Long.parseLong(e);
        } catch (Exception ex) {
            return MAX_RESPONSE_BYTES;
        }
    }

    // ── Fortsetz-Zustand ───────────────────────────────────────────────────
    // Was in der .bin steckt, beschreibt eine winzige Nebendatei
    // `<id>_<itag>.state`: bis zu welchem Segment sie reicht, wieviel Zeit das
    // ist, wie gross das Video insgesamt ist — und die Byte-Laenge, bei der das
    // galt. Nur wenn diese Laenge noch zur .bin passt, wird fortgesetzt; sonst
    // ist der Zustand veraltet und wir fangen sauber von vorn an. Damit setzt
    // eine wieder anfahrende Session ab Segment N+1 fort, statt alles noch
    // einmal zu holen.
    private static Path statePath(String videoId, int itag) {
        return DIR.resolve(safe(videoId) + "_" + itag + ".state");
    }

    private static void writeState(String videoId, int itag, long binLen, int lastSeq,
                                   long bufferedMs, int totalSegments, long totalDurationMs, long lmt) {
        try {
            Files.writeString(statePath(videoId, itag), binLen + " " + lastSeq + " " + bufferedMs
                    + " " + totalSegments + " " + totalDurationMs + " " + lmt);
        } catch (IOException ignored) {}
    }

    /// Fortsetz-Zustand aller Spuren eines Videos, sofern er zur .bin passt.
    private static Map<Integer, SabrSession.Resume> readResume(String videoId) {
        final Map<Integer, SabrSession.Resume> out = new java.util.LinkedHashMap<>();
        try (var s = Files.newDirectoryStream(DIR, safe(videoId) + "_*.state")) {
            for (Path p : s) {
                final String n = p.getFileName().toString();
                final int itag;
                try {
                    itag = Integer.parseInt(n.substring(n.lastIndexOf('_') + 1).replace(".state", ""));
                } catch (NumberFormatException e) { continue; }
                final String[] f = Files.readString(p).trim().split("\\s+");
                // 6 Felder inkl. lmt. Aeltere 5-Feld-Dateien haben keinen Encode
                // vermerkt — die werden bewusst NICHT fortgesetzt (s. Resume).
                if (f.length != 6) continue;
                final Path bin = DIR.resolve(safe(videoId) + "_" + itag + ".bin");
                if (!Files.exists(bin) || Files.size(bin) != Long.parseLong(f[0])) continue;  // veraltet
                final int lastSeq = Integer.parseInt(f[1]);
                final int totalSegs = Integer.parseInt(f[3]);
                // Schon vollstaendig -> nichts fortzusetzen. Ohne diese Bremse
                // wuerde eine Fortsetzung die letzten Segmente ein zweites Mal
                // anhaengen und die Datei zerstoeren.
                if (totalSegs > 0 && lastSeq >= totalSegs) continue;
                out.put(itag, new SabrSession.Resume(lastSeq, Long.parseLong(f[2]),
                        totalSegs, Long.parseLong(f[4]), Long.parseLong(f[5])));
            }
        } catch (Exception ignored) {}
        return out;
    }

    /// itags NUR aus dem, was schon auf Platte liegt — ohne Download anzustossen
    /// (die Pausen-Pruefung laeuft mitten in einer Sitzung).
    private static int[] itagsForCached(String videoId) {
        final int[] derived = deriveItags(videoId);
        return derived != null ? derived : new int[]{140, 137};
    }

    /// Fortsetz-Zustand aus der Anwesenheitskarte (Streifen-Modus).
    private static Map<Integer, SabrSession.Resume> readResumeFromMap(String videoId) {
        final Map<Integer, SabrSession.Resume> out = new java.util.LinkedHashMap<>();
        final int[] itags = deriveItags(videoId);
        if (itags == null) return out;
        for (int itag : itags) {
            if (!SparseStore.ensureOffsets(videoId, itag)) continue;
            final int lastSeq = SparseStore.contiguousFromStart(videoId, itag);
            final int total = SparseStore.cachedTotal(videoId, itag);
            final long lmt = SparseStore.cachedLmt(videoId, itag);
            if (lastSeq <= 0 || total <= 0 || lmt <= 0 || lastSeq >= total) continue;
            // Dauer schaetzen wir nicht — sie steckt in FORMAT_INIT; hier zaehlt
            // nur, ab WO weitergemacht wird.
            out.put(itag, new SabrSession.Resume(lastSeq, 0, total, 0, lmt));
        }
        return out;
    }

    /// Wann hat zuletzt jemand WIRKLICH Segmente abgerufen (= zugesehen)?
    ///
    /// ⚠️ Vom Playlist-Aufbau zu unterscheiden: der passiert auch beim Vorladen
    /// im Feed, ganz ohne Zuschauer. Wer nur die Uhr am Playlist-Aufbau
    /// aufzieht, laesst Hintergrund-Downloads fuer Videos laufen, die niemand
    /// ansieht — das kostet Bandbreite, Platte und vor allem Ruf bei YouTube
    /// (und der ist angesichts der 403-Wand das knappste Gut).
    private static final Map<String, Long> LAST_PLAYBACK = new ConcurrentHashMap<>();
    private static final long PLAYBACK_RECENT_MS = 30 * 60_000L;

    private static boolean watchedRecently(String videoId) {
        final Long t = LAST_PLAYBACK.get(videoId);
        return t != null && System.currentTimeMillis() - t < PLAYBACK_RECENT_MS;
    }

    /// Anforderung des Players vermerken — Grundlage der Bedarfssteuerung.
    private static void noteRequest(String videoId, int itag, long endOffset) {
        LAST_REQUEST.put(videoId, System.currentTimeMillis());
        LAST_PLAYBACK.put(videoId, System.currentTimeMillis());
        DEMAND_OFFSET.merge(safe(videoId) + "_" + itag, endOffset, Math::max);
    }

    private static int maxParallelDownloads() {
        final String v = System.getenv("SABR_MAX_PARALLEL_DOWNLOADS");
        if (v != null && !v.isBlank()) {
            try { return Math.max(1, Integer.parseInt(v.trim())); }
            catch (NumberFormatException ignored) {}
        }
        // ⚠️ NICHT 1: seit der Bedarfssteuerung laedt eine Session nur ihren
        // Vorlauf und PAUSIERT dann — dabei haelt sie ihren Platz weiter. Mit
        // nur einem Platz muesste ein zweiter Tap bis zum Idle-Stop (90 s)
        // warten. Drei Plaetze deckeln den Ansturm (3 × ~48 MB Vorlauf), ohne
        // echte Taps auszubremsen; pausierte Sessions kosten nichts.
        return 3;
    }

    /// Ab wann ist genug da, um zu antworten? Der Playlist-Layer baut eine
    /// EVENT-Playlist aus dem, was auf Platte liegt, und pollt nach — er braucht
    /// nur den Anfang. Video-Segmente sind ~1–9 MB, Audio-Segmente ~160 KB.
    private static long earlyServeBytes(int itag) {
        return isAudioItag(itag) ? 128L * 1024 : 1024L * 1024;
    }

    private static boolean isAudioItag(int itag) {
        return itag == 139 || itag == 140 || itag == 141
                || itag == 249 || itag == 250 || itag == 251;
    }

    /// Wie lange der erste Abruf hoechstens auf den Anfang wartet. Danach
    /// antwortet er mit dem, was da ist (oder 502) — der Download laeuft im
    /// Hintergrund weiter.
    private static final long EARLY_WAIT_MS = 30_000;

    /// Startet den Download im HINTERGRUND (einmal pro Video) und wartet nur,
    /// bis der Anfang serviert werden kann.
    ///
    /// ⚠️ Vorher lief `download()` synchron in diesem Aufruf. Das war richtig,
    /// solange eine Kids-Session nach ~13 MB endete; seit der WEB-Pfad KOMPLETTE
    /// Videos holt, blockierte der erste Tap minutenlang (gemessen 161 s fuer
    /// 1 KB!) — und der /sabr-Router haelt waehrenddessen einen der nur ZWEI
    /// Resolve-Slots (ServerLauncher: acquire vor handle, release im finally).
    /// Zwei kalte Kids-Taps haetten damit alle Resolves lahmgelegt: exakt die
    /// dokumentierte 503-Schleife (-1008 / -16849 in der App).
    public static Path ensureFile(String videoId, int itag) throws Exception {
        final Path file = DIR.resolve(safe(videoId) + "_" + itag + ".bin");
        if (Files.exists(file) && Files.size(file) >= earlyServeBytes(itag)) return file;
        // Auch der Playlist-Bau zaehlt als Lebenszeichen (sonst liefe die
        // frische Session in ihren Idle-Stop, bevor der Player das erste
        // Segment anfordert).
        LAST_REQUEST.put(videoId, System.currentTimeMillis());
        startDownload(videoId);
        final long deadline = System.currentTimeMillis() + EARLY_WAIT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (Files.exists(file) && Files.size(file) >= earlyServeBytes(itag)) break;
            // Session fertig oder gescheitert -> nicht weiter warten.
            if (!DOWNLOAD_ACTIVE.contains(videoId)) break;
            Thread.sleep(200);
        }
        return Files.exists(file) ? file : null;
    }

    /// Einmaliger Hintergrund-Download pro Video. Der per-Video-Lock bleibt die
    /// Serialisierung; DOWNLOAD_ACTIVE verhindert, dass Audio- und Video-Abruf
    /// zwei Threads in denselben Lock schicken.
    private static void startDownload(String videoId) {
        startDownload(videoId, false);
    }

    /// resume=true: fortsetzen, obwohl schon Dateien da sind — der Player will
    /// Bytes, die wir (noch) nicht haben. Ohne das bliebe eine pausierte oder
    /// idle beendete Session liegen, bis die Playlist-Schicht zufaellig einen
    /// Refill ausloest.
    private static void startDownload(String videoId, boolean resume) {
        startDownload(videoId, resume, false);
    }

    /// demand=true: der Player WARTET auf diese Bytes (Sprung). Dann darf weder
    /// die Cap-Markierung noch der anyFileFor-Guard bremsen — sonst kommt beim
    /// Vorspulen nie etwas an (2026-07-31 gemessen: Sprung-Anforderung lief in
    /// die frische Cap-Markierung, Session startete gar nicht -> 503).
    private static void startDownload(String videoId, boolean resume, boolean demand) {
        if (!resume && anyFileFor(videoId) && !DOWNLOAD_ACTIVE.contains(videoId)) return;
        if (!DOWNLOAD_ACTIVE.add(videoId)) return;
        Thread.ofVirtual().name("sabr-dl-" + videoId).start(() -> {
            boolean acquired = false;
            try {
                // Warten, bis ein Download-Platz frei ist. Wartende Videos sind
                // unkritisch: der Abruf haengt nicht daran (ensureFile wartet nur
                // auf den Anfang und laeuft sonst in seinen Timeout), und ein
                // Prefetch darf ruhig hinten anstehen.
                DOWNLOAD_SLOTS.acquire();
                acquired = true;
                synchronized (LOCKS.computeIfAbsent(videoId, k -> new Object())) {
                    // anyFileFor guard: if the session already ran but produced
                    // DIFFERENT itags (video without 1080p avc), a request for the
                    // absent itag must not re-trigger the whole download forever.
                    if (resume || !anyFileFor(videoId)) download(videoId, false, demand);
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                System.out.println("[SabrCache] " + videoId + " download failed: " + e.getMessage());
            } finally {
                if (acquired) DOWNLOAD_SLOTS.release();
                DOWNLOAD_ACTIVE.remove(videoId);
            }
        });
    }

    /// The itags the SABR session ACTUALLY picked for this video, as
    /// {audio, video}. Ensures the download ran (once); reads the manifest
    /// download() writes next to the media files. Falls back to {140, 137}
    /// (the preferred picks) for pre-manifest cache entries.
    public static int[] itagsFor(String videoId) throws Exception {
        final Path manifest = DIR.resolve(safe(videoId) + ".itags");
        if (!Files.exists(manifest)) {
            // Das Manifest schreibt download() erst am ENDE. Seit der Download
            // im Hintergrund laeuft (s. ensureFile) darf hier nicht mehr
            // minutenlang darauf gewartet werden — die tatsaechlich gewaehlten
            // itags stehen aber schon in den Dateinamen, sobald die Session die
            // ersten Bytes geschrieben hat.
            startDownload(videoId);
            final long deadline = System.currentTimeMillis() + EARLY_WAIT_MS;
            while (System.currentTimeMillis() < deadline
                    && !Files.exists(manifest) && !anyFileFor(videoId)
                    && DOWNLOAD_ACTIVE.contains(videoId)) {
                Thread.sleep(200);
            }
            if (!Files.exists(manifest)) {
                final int[] derived = deriveItags(videoId);
                if (derived != null) return derived;
            }
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

    /// itags aus den bereits geschriebenen Cache-Dateien ableiten — Ersatz fuer
    /// das Manifest, solange die Session noch laeuft. null, wenn (noch) kein
    /// Paar aus Audio und Video da ist.
    private static int[] deriveItags(String videoId) {
        int audio = -1, video = -1;
        try (var s = Files.newDirectoryStream(DIR, safe(videoId) + "_*.bin")) {
            for (Path p : s) {
                final String n = p.getFileName().toString();
                try {
                    final int it = Integer.parseInt(
                            n.substring(n.lastIndexOf('_') + 1).replace(".bin", ""));
                    if (isAudioItag(it)) audio = it; else video = it;
                } catch (NumberFormatException ignored) {}
            }
        } catch (IOException ignored) {}
        return (audio > 0 && video > 0) ? new int[]{audio, video} : null;
    }

    private static boolean anyFileFor(String videoId) {
        try (var s = Files.newDirectoryStream(DIR, safe(videoId) + "_*.bin")) {
            return s.iterator().hasNext();
        } catch (IOException e) {
            return false;
        }
    }

    /// Videos, bei denen ein Nachfordern gerade NICHT klappt (Drosselfenster).
    /// Solange die Marke steht, bietet die Playlist nur das an, was wirklich da
    /// ist — sonst springt der Player in einen Bereich, den wir nicht liefern
    /// koennen, und bleibt im Standbild haengen (2026-07-31 in der App: Video
    /// forderte Byte 418MB von 868MB an, Session cappte bei 3 Segmenten, jede
    /// Antwort 503). Lieber die erste Minute spielen als gar nichts.
    private static final Map<String, Long> SEEK_UNAVAILABLE = new ConcurrentHashMap<>();
    private static final long SEEK_UNAVAILABLE_TTL_MS = 10 * 60_000L;

    /// Liefert die Quelle fuer dieses Video gerade zuverlaessig? NUR dann bietet
    /// die Playlist alle Segmente an.
    ///
    /// ⚠️ Beweislast umgedreht (2026-07-31, zweimal in der App aufgeschlagen):
    /// Eine VOD-Playlist wird NICHT neu geladen. Wenn wir beim ERSTEN Bau alle
    /// Segmente anbieten und die Drosselung erst danach auffaellt, ist der
    /// Player auf diese Playlist festgenagelt, fordert die Mitte an und haengt
    /// im Standbild — die spaeter korrigierte Playlist sieht er nie. Also:
    /// im Zweifel NUR den vorhandenen Anfang anbieten (dann spielt wenigstens
    /// die erste Minute) und alles erst, wenn eine Sitzung nachweislich sauber
    /// geliefert hat.
    private static final Map<String, Long> SEEK_PROVEN = new ConcurrentHashMap<>();
    private static final long SEEK_PROVEN_TTL_MS = 10 * 60_000L;

    /// Ergebnis einer Sitzung bewerten: sauber beendet = Quelle liefert.
    private static void noteSessionOutcome(String videoId, SabrHandlers.SabrMedia r) {
        final boolean healthy = r != null && (r.complete()
                || "idle".equals(r.stopReason()) || "maxIterations".equals(r.stopReason()));
        if (healthy) SEEK_PROVEN.put(videoId, System.currentTimeMillis() + SEEK_PROVEN_TTL_MS);
        else SEEK_PROVEN.remove(videoId);
    }

    /// Welche Token-Bindung hat fuer DIESES Video zuletzt geliefert?
    ///
    /// ⚠️ Die Leiter beginnt sonst immer mit dem visitor-bound Token — bei
    /// Made-for-Kids liefert aber regelmaessig erst die content-bound Stufe.
    /// Gemessen 2026-07-31 an einem Sprung: 12:16:25,9 Sprung erkannt,
    /// 12:16:38,5 erste Stufe aufgegeben, danach erst die richtige — von 18,6 s
    /// Wartezeit gingen ~11 s an eine Stufe, die fuer dieses Video nie liefert.
    /// Mit dem Merker faengt der naechste Versuch gleich richtig an.
    private static final Map<String, Boolean> GOOD_BINDING = new ConcurrentHashMap<>();

    /// Waechst der Cache gerade? Dann liefert die Quelle JETZT — der staerkste
    /// verfuegbare Beleg, dass wir fehlende Stellen nachfordern koennen.
    private static final Map<String, Long> LAST_GROWTH = new ConcurrentHashMap<>();
    private static final Map<String, Integer> LAST_SEQ_SEEN = new ConcurrentHashMap<>();
    private static final long GROWTH_FRESH_MS = 60_000;

    private static void noteGrowth(String videoId, int itag, int lastSeq) {
        final String k = videoId + "_" + itag;
        final Integer prev = LAST_SEQ_SEEN.put(k, lastSeq);
        if (prev == null || lastSeq > prev) LAST_GROWTH.put(videoId, System.currentTimeMillis());
    }

    private static boolean growingNow(String videoId) {
        final Long t = LAST_GROWTH.get(videoId);
        return t != null && System.currentTimeMillis() - t < GROWTH_FRESH_MS;
    }

    public static boolean seekable(String videoId) {
        if (seekUnavailable(videoId)) return false;
        // Ein laufender, liefernder Download ist Beleg genug. Ohne das bekaeme
        // ein Video, das GERADE geladen wird, nur eine EVENT-Playlist ohne
        // ENDLIST — und die zeigt AVPlayer als Livestream: KEINE Gesamtdauer,
        // KEIN Springen (so in der App gemeldet, waehrend der Cache von 96 auf
        // 100 Segmente wuchs).
        if (growingNow(videoId)) return true;
        final Long exp = SEEK_PROVEN.get(videoId);
        if (exp == null) return false;
        if (exp < System.currentTimeMillis()) { SEEK_PROVEN.remove(videoId); return false; }
        return true;
    }

    public static boolean seekUnavailable(String videoId) {
        final Long exp = SEEK_UNAVAILABLE.get(videoId);
        if (exp == null) return false;
        if (exp < System.currentTimeMillis()) { SEEK_UNAVAILABLE.remove(videoId); return false; }
        return true;
    }

    /// Sprungziel je Video: die Segmentnummer, die der Player gerade braucht.
    private static final Map<String, Integer> SEEK_SEQ = new ConcurrentHashMap<>();
    /// Wie lange ein Abruf auf nachgeforderte Bytes wartet.
    private static final long SEEK_WAIT_MS = 20_000;

    static {
        SabrHandlers.SESSION_SEEK = vid -> () -> SEEK_SEQ.getOrDefault(vid, -1);
    }

    /// Streifen-Modus: fehlende Segmente eines angefragten Bereichs NACHFORDERN
    /// statt 416 zu antworten. Das ist der Kern des freien Springens — der
    /// Player darf jede Stelle anfragen, wir holen sie dann. Gibt true zurueck,
    /// wenn alle Bytes des Bereichs vorliegen.
    private static boolean ensureRange(String videoId, int itag, long start, long end) throws Exception {
        if (!SparseStore.ensureOffsets(videoId, itag)) return false;
        final List<Integer> segs = SparseStore.segmentsForRange(videoId, itag, start, end);
        // Kein Segment betroffen = der Bereich liegt im Init/sidx-Kopf. Der ist
        // da, sobald es Offsets gibt (die stammen ja aus ihm) — also liefern.
        // ⚠️ Das als Fehler zu werten war der erste Bug hier: AVPlayer holt als
        // ALLERERSTES genau diesen Kopf (EXT-X-MAP) und bekam ein 503.
        if (segs.isEmpty()) return true;
        List<Integer> missing = SparseStore.missing(videoId, itag, segs);
        if (missing.isEmpty()) return true;
        System.out.println("[Sparse] " + videoId + "/" + itag + " Bereich " + start + "-" + end
                + ": " + missing.size() + " Segment(e) fehlen, ab " + missing.get(0) + " nachfordern");
        SEEK_SEQ.put(videoId, missing.get(0));
        startDownload(videoId, true, true);
        final long deadline = System.currentTimeMillis() + SEEK_WAIT_MS;
        while (System.currentTimeMillis() < deadline) {
            Thread.sleep(250);
            missing = SparseStore.missing(videoId, itag, segs);
            if (missing.isEmpty()) {
                SEEK_SEQ.remove(videoId);
                SEEK_PROVEN.put(videoId, System.currentTimeMillis() + SEEK_PROVEN_TTL_MS);
                return true;
            }
            SEEK_SEQ.put(videoId, missing.get(0));
        }
        SEEK_UNAVAILABLE.put(videoId, System.currentTimeMillis() + SEEK_UNAVAILABLE_TTL_MS);
        return false;
    }

    public static HttpResponse handle(String videoId, int itag, String range, boolean head) throws Exception {
        // Der Player sagt mit JEDEM Range-Request, wie weit er ist — das ist die
        // Eingabe der Bedarfssteuerung (s. noteRequest / SESSION_PAUSE).
        final long wantEnd = requestedEnd(range);
        noteRequest(videoId, itag, wantEnd);
        if (SPARSE) return handleSparse(videoId, itag, range, head, wantEnd);
        final Path file = ensureFile(videoId, itag);
        // Der Player will ueber das hinaus, was auf Platte liegt -> Session
        // fortsetzen. Das ist der Gegenpart zur Pause: sie haelt an, sobald der
        // Vorlauf reicht, und hier faehrt sie wieder an. Ohne das bliebe eine
        // idle beendete Session liegen, bis die Playlist-Schicht einen Refill
        // ausloest (Sperrzeit 5 min — viel zu traege, wenn der Player wartet).
        if (file != null && wantEnd >= Files.size(file) - LEAD_BYTES / 2)
            startDownload(videoId, true);
        if (file == null) {
            return HttpResponse.ofCode(502).withBody("sabr: no media for itag".getBytes());
        }
        try {
            Files.setLastModifiedTime(file, java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis()));
        } catch (IOException ignored) {}
        return serveFile(file, itag, range, head);
    }

    /// Auslieferung im Streifen-Modus: der Player darf JEDE Stelle anfragen.
    /// Fehlt etwas, wird es nachgefordert (ensureRange) statt das Video mit 416
    /// zu verwerfen. Kommt es nicht rechtzeitig, antworten wir mit 503 —
    /// AVPlayer wiederholt das, waehrend die Sitzung weiterlaeuft.
    private static HttpResponse handleSparse(String videoId, int itag, String range,
                                             boolean head, long wantEnd) throws Exception {
        final Path file = ensureFile(videoId, itag);
        if (file == null) return HttpResponse.ofCode(502).withBody("sabr: no media for itag".getBytes());
        SparseStore.ensureOffsets(videoId, itag);
        final long total = SparseStore.totalLength(videoId, itag);
        if (total <= 0) return serveFile(file, itag, range, head);   // sidx noch nicht da
        final long[] se = range == null || range.isEmpty()
                ? new long[]{0, Math.min(total - 1, MAX_RESPONSE_BYTES - 1)}
                : parseRange(range, total);
        if (se == null) return HttpResponse.ofCode(416);
        final long start = se[0];
        final long end = Math.min(se[1], start + MAX_RESPONSE_BYTES - 1);
        if (!head && !ensureRange(videoId, itag, start, end)) {
            System.out.println("[Sparse] " + videoId + "/" + itag + " Bereich " + start + "-" + end
                    + " nicht rechtzeitig da -> 503");
            return HttpResponse.ofCode(503);
        }
        return serveFile(file, itag, range, head, total);
    }

    private static void download(String videoId) throws Exception {
        download(videoId, false, false);
    }

    private static void download(String videoId, boolean allowPacedRequested) throws Exception {
        download(videoId, allowPacedRequested, false);
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

    private static void download(String videoId, boolean allowPacedRequested, boolean demand) throws Exception {
        final boolean allowPaced = allowPacedRequested && pacedEnabled();
        // Known capped (kids): burst rungs are wasted requests — refill goes
        // straight to the paced 1x session; the sync warm path keeps serving
        // the existing partial cache untouched.
        // ⚠️ Nur abkuerzen, wenn tatsaechlich ein Teil-Cache da ist, den die
        // Playlist-Schicht ausliefern kann. Ohne Cache hiesse die Abkuerzung
        // „30 Minuten lang gar nichts" (502 statt Video) — genau das passiert,
        // wenn die Eviction die Dateien geraeumt hat oder eine Session ohne ein
        // einziges Segment endete. Dann lieber die Leiter fahren.
        // ⚠️ Cap-Markierung bremst nur den SYNCHRONEN Weg (der Tap serviert dann
        // den Teil-Cache statt zu warten). Ein REFILL darf trotzdem die volle
        // Leiter fahren: die Markierung stammt aus der Zeit, als jeder Weg
        // aussichtslos war — seit der WEB-Pfad im offenen Fenster komplette
        // Kids-Videos holt, ist ein spaeterer Versuch genau das Richtige. So
        // vervollstaendigt sich ein Teil-Video von selbst, sobald das Fenster
        // aufgeht (Sperrzeit + WEB-Memo deckeln die Versuche).
        if (isCapMarked(videoId) && anyFileFor(videoId) && !allowPacedRequested && !demand) return;
        if (isCapMarked(videoId) && anyFileFor(videoId) && pacedEnabled()) {
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
        // ⚠️ Das WEB-Memo spart Fehlversuche beim VORLADEN — aber wenn der Nutzer
        // WARTET (Sprung), muss der beste Weg probiert werden: der WEB-Pfad ist
        // der einzige, der die Attestierungssperre bricht (belegt: komplette
        // Kids-Videos). Ohne diese Ausnahme lief jeder Sprung auf ANDROID und
        // damit in prot=3 (2026-07-31 gemessen: Sprung auf Segment 332 korrekt
        // ausgefuehrt, Server lieferte nichts, weil ANDROID gedeckelt ist).
        final boolean skipWeb = DEFAULT_CLIENT != 0 && webBlocked() && !demand;
        if (demand && webBlocked())
            System.out.println("[SabrCache] " + videoId + " Sprung: WEB-Memo uebergangen");
        if (skipWeb)
            System.out.println("[SabrCache] " + videoId
                    + " WEB-Memo aktiv -> direkt ANDROID (spart 2 Fehlversuche)");
        int usedClient = skipWeb ? 0 : DEFAULT_CLIENT;
        // Mit der Bindung anfangen, die zuletzt geliefert hat (s. GOOD_BINDING).
        final boolean preferContentBound = Boolean.TRUE.equals(GOOD_BINDING.get(videoId));
        SabrHandlers.SabrMedia result = preferContentBound && !skipWeb
                ? attempt(videoId, fam1, true, usedClient)
                : attemptBothFamilies(videoId, fam1, usedClient);
        if (preferContentBound && (result == null || result.segments() == 0))
            result = attemptBothFamilies(videoId, fam1, usedClient);
        if (result != null && result.segments() > 0)
            GOOD_BINDING.put(videoId, preferContentBound);
        // Stufe 2: Client-Rueckfall auf ANDROID. Es gab Fenster (2026-07-30
        // abends), in denen der WEB-Pfad auf BEIDEN Familien hart 403te, waehrend
        // ANDROID noch seine ~13 MB holte. Ein Teil-Cache ist besser als keiner,
        // also die alte Stufe nachziehen, wenn WEB gar nichts gebracht hat.
        // Stufe 1b: WEB mit CONTENT-BOUND Token. Fuer Made-for-Kids ist das die
        // Stufe, die tatsaechlich liefert: mit visitor-bound Token bleibt die
        // WEB-Session bei ~20 Segmenten stehen, mit content-bound holt sie das
        // Video KOMPLETT (2026-07-30: 735/735, 662 MB, 195 Runden). Die
        // content-bound-Stufe weiter unten lief bisher nur im `stuck`-Fall —
        // bei 403/0 Segmenten sprangen wir direkt auf ANDROID und liessen die
        // wirksamste Stufe aus.
        if (!skipWeb && DEFAULT_CLIENT != 0 && (result == null || result.segments() == 0)) {
            System.out.println("[SabrCache] " + videoId
                    + " WEB visitor-bound brachte nichts -> WEB content-bound");
            final SabrHandlers.SabrMedia rc = attempt(videoId, fam1, true, DEFAULT_CLIENT);
            if (rc != null && (result == null || rc.segments() > result.segments())) {
                result = rc;
                GOOD_BINDING.put(videoId, true);   // diese Bindung liefert hier
                System.out.println("[SabrCache] " + videoId + " WEB content-bound gewann (segs="
                        + rc.segments() + " complete=" + rc.complete() + ")");
            }
        }
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
                GOOD_BINDING.put(videoId, true);   // diese Bindung liefert hier
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
        noteSessionOutcome(videoId, result);
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
        // Fortlaufende Positionen je Spur (nur im Streifen-Modus): das naechste
        // zu schreibende Byte, weil die .part hier ein reiner Strom ist.
        final Map<Integer, Long> streamPos = new ConcurrentHashMap<>();
        final Map<Integer, java.util.TreeMap<Integer, byte[]>> pending = new ConcurrentHashMap<>();
        final Map<Integer, Integer> nextSeq = new ConcurrentHashMap<>();
        final SabrSession.Sink sink = new SabrSession.Sink() {
            @Override public void writeInit(int itag, long lmt, byte[] data) throws IOException {
                if (SPARSE) { SparseStore.writeInit(videoId, itag, lmt, data); return; }
                openStream(itag).write(data);
                streamPos.merge(itag, (long) data.length, Long::sum);
            }
            @Override public void writeSegment(int itag, int seq, byte[] data) throws IOException {
                if (SPARSE) { SparseStore.writeSegment(videoId, itag, seq, data); return; }
                final java.util.TreeMap<Integer, byte[]> stash =
                        pending.computeIfAbsent(itag, k -> new java.util.TreeMap<>());
                stash.put(seq, data);
                Integer next = nextSeq.get(itag);
                if (next == null) { next = stash.firstKey(); nextSeq.put(itag, next); }
                final OutputStream os = openStream(itag);
                while (stash.containsKey(next)) {
                    final byte[] d = stash.remove(next);
                    os.write(d);
                    streamPos.merge(itag, (long) d.length, Long::sum);
                    next = next + 1;
                }
                nextSeq.put(itag, next);
            }
            /// Klassischer Weg: fortlaufend in eine .part schreiben. ⚠️ Die
            /// Session sortiert NICHT mehr (das braucht der Streifen-Modus
            /// gerade nicht), deshalb haelt dieser Pfad die Reihenfolge selbst
            /// ein: was nicht direkt anschliesst, wartet — sonst entstuende bei
            /// einer Luecke eine Datei mit vertauschten Segmenten.
            private OutputStream openStream(int itag) throws IOException {
                OutputStream os = opened.get(itag);
                if (os != null) return os;
                final Path part = DIR.resolve(safe(videoId) + "_" + itag + ".part");
                Files.deleteIfExists(part);
                os = new BufferedOutputStream(Files.newOutputStream(part), 1 << 20);
                parts.put(itag, part);
                opened.put(itag, os);
                return os;
            }
        };
        // INKREMENTELL VEROEFFENTLICHEN — pro Runde, nicht erst am Ende.
        // Seit der WEB-Pfad komplette Videos holt, dauert eine Kids-Session
        // Minuten (gemessen: 161 s / 197 Runden). Ohne diesen Hook entsteht die
        // .bin erst danach: der erste Tap wartet die ganze Zeit und haelt dabei
        // einen der nur ZWEI Resolve-Slots. Mit dem Hook waechst die .bin
        // waehrend des Downloads, die EVENT-Playlist listet die vorhandenen
        // Segmente und der Player startet nach Sekunden.
        //
        // ⚠️ NICHT die ganze Datei pro Runde kopieren (das macht der paced-Weg):
        // bei 662 MB × ~200 Runden waeren das zig GB Schreiblast auf der
        // SD-Karte. publishTail haengt nur die neuen Bytes an -> insgesamt 2×
        // Dateigroesse. Voraussetzung fuer das Anhaengen ist, dass die .bin von
        // DIESEM Versuch stammt; existiert schon eine aus einem frueheren
        // Versuch, bleibt es beim keep-larger-Publish am Ende (sonst wuerden
        // Bytes zweier Sessions ineinander laufen).
        // Fortsetzen: passt der gespeicherte Zustand zur .bin, macht die Session
        // ab Segment N+1 weiter und die .part enthaelt NUR den neuen Schwanz —
        // der wird an die vorhandene .bin angehaengt. baseLen haelt fest, wo die
        // .bin beim Start stand (Pruefgroesse fuers Anhaengen).
        // ⚠️ Im Streifen-Modus steht der Stand in der KARTE (.map), nicht in der
        // alten .state-Datei — sonst startet jede Sitzung wieder bei Segment 1,
        // laedt Vorhandenes ein zweites Mal und ignoriert den Sprungwunsch
        // (2026-07-31 beobachtet: Karte wuchs stur 1..456, kein einziger
        // "Sprung auf"-Eintrag, Sprungabrufe liefen in den Timeout).
        final Map<Integer, SabrSession.Resume> resume =
                SPARSE ? readResumeFromMap(videoId) : readResume(videoId);
        final Map<Integer, Long> baseLen = new ConcurrentHashMap<>();
        for (var e : resume.entrySet()) {
            try {
                baseLen.put(e.getKey(), Files.size(DIR.resolve(safe(videoId) + "_" + e.getKey() + ".bin")));
            } catch (IOException ignored) {}
        }
        final Map<Integer, long[]> progress = new ConcurrentHashMap<>();
        // Welche Spuren wurden WIRKLICH fortgesetzt? Nur bei denen darf der neue
        // Schwanz angehaengt werden; bei allen anderen enthaelt die .part das
        // Video ab Segment 1 (dann gilt keep-larger wie bisher).
        final Set<Integer> resumedItags = ConcurrentHashMap.newKeySet();
        final SabrSession.ProgressSink progressSink = new SabrSession.ProgressSink() {
            @Override public void report(int itag, int lastSeq, long bufMs, int totalSegs,
                                         long totalDurMs, long lmt) {
                progress.put(itag, new long[]{lastSeq, bufMs, totalSegs, totalDurMs, lmt});
            }
            @Override public void resumeApplied(int itag, boolean applied) {
                if (applied) resumedItags.add(itag); else resumedItags.remove(itag);
            }
        };
        final Map<Integer, Long> publishedLen = new ConcurrentHashMap<>();
        // Anhaengen ist erlaubt, wenn noch nichts da ist ODER wir nachweislich
        // an unseren eigenen Fortsetz-Zustand anknuepfen.
        final boolean canAppend = !anyFileFor(videoId) || !resume.isEmpty();
        final Runnable publishHook = () -> {
            for (Map.Entry<Integer, Path> e : parts.entrySet()) {
                final int itag = e.getKey();
                // Anhaengen nur, wenn es nichts Fremdes gibt (frisches Video)
                // oder wir nachweislich an unseren eigenen Stand anknuepfen.
                final long base = baseLen.getOrDefault(itag, 0L);
                if (canAppend && (base == 0 || resumedItags.contains(itag))) {
                    publishTail(videoId, itag, e.getValue(), publishedLen, base);
                    // Fortsetzpunkt mitschreiben — er muss zur .bin passen, sonst
                    // wird er beim naechsten Start verworfen (s. readResume).
                    final long[] pr = progress.get(itag);
                    if (pr != null && pr[0] > 0) {
                        try {
                            final long binLen = Files.size(DIR.resolve(safe(videoId) + "_" + itag + ".bin"));
                            writeState(videoId, itag, binLen, (int) pr[0], pr[1], (int) pr[2], pr[3], pr[4]);
                        } catch (IOException ignored) {}
                    }
                } else {
                    publishIfLarger(videoId, itag, e.getValue(), false);
                }
            }
            // WAEHREND des Downloads aufraeumen, nicht erst danach: ein Video
            // waechst inzwischen auf ~700 MB (und belegt bis zum Schluss doppelt,
            // weil .part und .bin nebeneinander liegen). Bis zum Session-Ende zu
            // warten hiesse, den Deckel um Gigabytes zu ueberfahren. maybeEvict
            // drosselt sich selbst auf einen Lauf pro 30 s und ruehrt das gerade
            // laufende Video nicht an.
            // Streifen-Modus: Anwesenheitskarte festschreiben, damit ein Neustart
            // weiss, was schon da ist.
            if (SPARSE) {
                for (var pe : progress.entrySet()) {
                    final long[] pr = pe.getValue();
                    SparseStore.persist(videoId, pe.getKey(), pr[4], (int) pr[2]);
                    noteGrowth(videoId, pe.getKey(), (int) pr[0]);
                }
            }
            maybeEvict();
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
            result = SabrHandlers.runSession(videoId, sink, family, effContentBound, effClientMode,
                    paced, publishHook, resume, progressSink);
        } catch (Exception e) {
            System.out.println("[SabrCache] " + videoId + " attempt(" + family + ") threw: " + e.getMessage());
        } finally {
            // the session closes the streams it was handed; this is defensive for
            // the error path (runSession throws before the session's finally runs).
            for (OutputStream os : opened.values()) { try { os.close(); } catch (IOException ignored) {} }
        }
        // Schlussveroeffentlichung. ⚠️ Beim FORTSETZEN enthaelt die .part nur den
        // neuen Schwanz und ist damit KLEINER als die .bin — der keep-larger-Weg
        // wuerde sie verwerfen und die letzte Runde ginge verloren (die Session
        // bricht bei `complete`/`stuck` VOR dem naechsten Publish-Hook ab).
        // Also in dem Fall anhaengen statt vergleichen.
        for (Map.Entry<Integer, Path> e : parts.entrySet()) {
            final int itag = e.getKey();
            final long base = baseLen.getOrDefault(itag, 0L);
            if (canAppend && base > 0 && resumedItags.contains(itag)) {
                publishTail(videoId, itag, e.getValue(), publishedLen, base);
                try { Files.deleteIfExists(e.getValue()); } catch (IOException ignored) {}
            } else {
                publishIfLarger(videoId, itag, e.getValue(), true);
            }
            // Fortsetzpunkt final festhalten, passend zur fertigen .bin.
            final long[] pr = progress.get(itag);
            if (pr != null && pr[0] > 0) {
                try {
                    final long binLen = Files.size(DIR.resolve(safe(videoId) + "_" + itag + ".bin"));
                    writeState(videoId, itag, binLen, (int) pr[0], pr[1], (int) pr[2], pr[3], pr[4]);
                } catch (IOException ignored) {}
            } else if (base > 0 && resumedItags.contains(itag)) {
                // Fortgesetzt, aber KEIN einziges neues Segment: der Server
                // erkennt unseren Stand nicht an (anderer Encode, abgelaufene
                // Sitzung, Drosselfenster). Zustand wegwerfen, damit die naechste
                // Runde sauber von vorn beginnt statt es ewig zu wiederholen.
                try { Files.deleteIfExists(statePath(videoId, itag)); } catch (IOException ignored) {}
                System.out.println("[SabrCache] " + videoId + "/" + itag
                        + " Fortsetzen brachte nichts -> Zustand verworfen");
            }
        }
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

    /// Inkrementelles Veroeffentlichen OHNE Voll-Kopie: haengt nur die seit dem
    /// letzten Aufruf dazugekommenen Bytes der .part an die .bin an. Die Session
    /// flusht vor dem Hook, es werden also nur ganze Segmente sichtbar; Leser
    /// sehen jederzeit ein gueltiges PRAEFIX der Enddatei (Anhaengen aendert
    /// bereits gelesene Offsets nicht).
    private static void publishTail(String videoId, int itag, Path part,
                                    Map<Integer, Long> published, long baseLen) {
        final Path bin = DIR.resolve(safe(videoId) + "_" + itag + ".bin");
        try {
            final long partSize = Files.exists(part) ? Files.size(part) : 0;
            final long done = published.getOrDefault(itag, 0L);
            if (partSize <= done) return;
            final long binSize = Files.exists(bin) ? Files.size(bin) : 0;
            // Die .bin muss genau dort stehen, wo wir sie verlassen haben:
            // Ausgangslaenge (0 bei frischem Video, sonst der Fortsetzpunkt)
            // plus das, was diese Sitzung schon angehaengt hat. Passt das nicht,
            // schreibt jemand anderes hinein — dann NICHT anhaengen, sonst
            // mischen sich zwei Sessions. Der terminale keep-larger-Publish
            // raeumt das am Ende korrekt auf.
            if (binSize != baseLen + done) return;
            try (RandomAccessFile in = new RandomAccessFile(part.toFile(), "r");
                 OutputStream out = Files.newOutputStream(bin,
                         java.nio.file.StandardOpenOption.CREATE,
                         java.nio.file.StandardOpenOption.APPEND)) {
                in.seek(done);
                final byte[] buf = new byte[1 << 20];
                long remaining = partSize - done;
                while (remaining > 0) {
                    final int n = in.read(buf, 0, (int) Math.min(buf.length, remaining));
                    if (n <= 0) break;
                    out.write(buf, 0, n);
                    remaining -= n;
                }
            }
            published.put(itag, partSize);
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
        // Nur nachladen, wenn dieses Video kuerzlich WIRKLICH gespielt wurde.
        // Ein Playlist-Aufbau allein (Vorladen im Feed) reicht nicht — sonst
        // laufen Hintergrund-Downloads fuer Videos, die niemand ansieht.
        if (!watchedRecently(videoId)) return;
        if (isRefillExhausted(videoId)) return;          // paced verdict: nichts zu holen
        // ⚠️ FRUEHER hier: cap-markiert + paced aus -> abbrechen. Das galt, solange
        // JEDER Weg an der Attestierung scheiterte. Seit der WEB-Pfad im offenen
        // Fenster komplette Kids-Videos holt, ist der spaetere Versuch der
        // einzige Weg, wie ein bei ~60 s abgeschnittenes Video je vollstaendig
        // wird. Gedeckelt bleibt es durch REFILL_COOLDOWN_MS und das WEB-Memo.
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

    /// Obergrenze fuer EINE Antwort. Der Handler ist blockierend
    /// (`AsyncServlet.ofBlocking`) und baut den Body als byte[] — ohne Deckel
    /// zieht ein einziger Abruf die ganze Datei in den Heap. Das war folgenlos,
    /// solange SABR nur 13–20 MB Teilcaches erzeugte; seit der WEB-Pfad
    /// KOMPLETTE Videos holt (gemessen: 662 MB), killt genau ein solcher Abruf
    /// den 2-GiB-Container (OutOfMemoryError + 713-MB-Heapdump, 2026-07-30).
    /// 16 MB liegen weit ueber jedem echten Segment-Range (~1–9 MB laut den
    /// FORMAT_INIT-Daten), der Auslieferungspfad merkt den Deckel also nicht.
    private static final long MAX_RESPONSE_BYTES = 16L * 1024 * 1024;

    private static HttpResponse serveFile(Path file, int itag, String range, boolean head) throws IOException {
        return serveFile(file, itag, range, head, Files.size(file));
    }

    /// total wird im Streifen-Modus aus dem sidx genommen, NICHT aus der
    /// Dateigroesse: die Datei hat Loecher, ihre Groesse sagt nichts darueber,
    /// wie lang das Video ist.
    private static HttpResponse serveFile(Path file, int itag, String range, boolean head, long total)
            throws IOException {
        final String ct = itag == 140 ? "audio/mp4" : "video/mp4";
        final boolean noRange = range == null || range.isEmpty();
        if (noRange && (head || total <= MAX_RESPONSE_BYTES)) {
            final HttpResponse r = HttpResponse.ofCode(200)
                    .withHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValue.of(ct))
                    .withHeader(HttpHeaders.CONTENT_LENGTH, HttpHeaderValue.of(String.valueOf(total)))
                    .withHeader(HttpHeaders.ACCEPT_RANGES, HttpHeaderValue.of("bytes"));
            return head ? r : r.withBody(Files.readAllBytes(file));
        }
        // Range-los auf einer grossen Datei: als 206 mit dem ersten Fenster
        // beantworten statt den Heap zu sprengen. Die Auslieferung laeuft
        // ohnehin ueber EXT-X-BYTERANGE, also immer mit Range; range-los
        // kommen praktisch nur Werkzeuge (curl) — die sehen jetzt eine
        // Teilantwort statt eines toten Backends.
        final long[] se = noRange ? new long[]{0, total - 1} : parseRange(range, total);
        if (se == null) return HttpResponse.ofCode(416);
        final long start = se[0];
        // Mehr als MAX_RESPONSE_BYTES am Stueck wird gekuerzt — der Client holt
        // den Rest per Folge-Range (zulaessig: der Server darf weniger liefern
        // als angefragt, solange Content-Range das ausweist).
        final long end = Math.min(se[1], start + MAX_RESPONSE_BYTES - 1);
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

    /// Untergrenze fuer den FREIEN Plattenplatz. Der Cache-Deckel allein genuegt
    /// nicht mehr: die SD-Karte teilt sich das Dateisystem mit allem anderen auf
    /// dem Pi, und seit der WEB-Pfad komplette Videos holt, kostet EIN Video
    /// ~724 MB — waehrend des Downloads sogar doppelt, weil `.part` und die
    /// wachsende `.bin` nebeneinander liegen. `SABR_CACHE_MIN_FREE_GB` stellt
    /// den Wert um.
    private static final long MIN_FREE_BYTES = envGb("SABR_CACHE_MIN_FREE_GB", 3);

    private static long envGb(String name, long defGb) {
        final String v = System.getenv(name);
        long gb = defGb;
        if (v != null && !v.isBlank()) {
            try { gb = Long.parseLong(v.trim()); } catch (NumberFormatException ignored) {}
        }
        return gb * 1024L * 1024 * 1024;
    }

    private static long lastEvict = 0;

    /// LRU-Eviction ueber GANZE VIDEOS.
    ///
    /// ⚠️ Vorher wurden einzelne `.bin` geloescht. Bei 13-MB-Fragmenten war das
    /// egal, jetzt nicht mehr: faellt die Video-Datei und die Audio-Datei bleibt,
    /// ist `anyFileFor` weiter true — der `ensureFile`-Guard verhindert dann
    /// jeden Neu-Download, und das Video ist dauerhaft halb. Ausserdem blieben
    /// die `.itags`-Manifeste ewig liegen. Jetzt fliegt pro Runde ein komplettes
    /// Video (alle `_*.bin` + Manifest) raus, aeltestes zuerst — und niemals
    /// eines, das gerade heruntergeladen wird.
    private static synchronized void maybeEvict() {
        final long now = System.currentTimeMillis();
        if (now - lastEvict < 30_000) return;
        lastEvict = now;
        try {
            final Map<String, List<Path>> byVideo = new java.util.HashMap<>();
            final Map<String, Long> sizeOf = new java.util.HashMap<>();
            final Map<String, Long> touchedAt = new java.util.HashMap<>();
            long total = 0;
            try (var stream = Files.list(DIR)) {
                for (Path p : (Iterable<Path>) stream::iterator) {
                    final String n = p.getFileName().toString();
                    final String id;
                    if (n.endsWith(".bin")) id = n.substring(0, n.lastIndexOf('_'));
                    else if (n.endsWith(".state")) id = n.substring(0, n.lastIndexOf('_'));
                    else if (n.endsWith(".itags")) id = n.substring(0, n.length() - 6);
                    else continue;
                    final long sz = Files.size(p);
                    total += sz;
                    byVideo.computeIfAbsent(id, k -> new ArrayList<>()).add(p);
                    sizeOf.merge(id, sz, Long::sum);
                    touchedAt.merge(id, Files.getLastModifiedTime(p).toMillis(), Math::max);
                }
            }
            final long free = Files.getFileStore(DIR).getUsableSpace();
            if (total <= MAX_CACHE_BYTES && free >= MIN_FREE_BYTES) return;
            // Ziel: unter 80% des Deckels UND ueber der Freiplatz-Grenze.
            final long targetTotal = MAX_CACHE_BYTES * 8 / 10;
            final List<String> lru = new ArrayList<>(byVideo.keySet());
            lru.sort((a, b) -> Long.compare(touchedAt.getOrDefault(a, 0L), touchedAt.getOrDefault(b, 0L)));
            // Dateinamen tragen die dateisichere Form der videoId — dagegen
            // pruefen, nicht gegen die rohe (bei YouTube-IDs identisch, aber
            // darauf soll sich das hier nicht verlassen).
            final Set<String> activeSafe = new java.util.HashSet<>();
            for (String v : DOWNLOAD_ACTIVE) activeSafe.add(safe(v));
            long freed = 0;
            for (String id : lru) {
                if (total - freed <= targetTotal && free + freed >= MIN_FREE_BYTES) break;
                if (activeSafe.contains(id)) continue;   // laeuft gerade
                for (Path p : byVideo.get(id)) {
                    try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                }
                freed += sizeOf.getOrDefault(id, 0L);
                System.out.println("[SabrCache] evict " + id + " ("
                        + (sizeOf.getOrDefault(id, 0L) >> 20) + "MB)");
            }
            if (freed > 0)
                System.out.println("[SabrCache] evict gesamt " + (freed >> 20) + "MB, Cache jetzt "
                        + ((total - freed) >> 20) + "MB, frei "
                        + ((free + freed) >> 30) + "GB");
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
