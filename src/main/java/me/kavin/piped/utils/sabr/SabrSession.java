package me.kavin.piped.utils.sabr;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/// SABR streaming session (Baustein 3). Drives the VideoPlaybackAbrRequest loop:
/// each request reports buffered_ranges + player_time, the server streams the
/// next UMP media segments; we write them per format (init + segments by seq)
/// to a caller-provided Sink as they arrive. See SABR-SPEC.md.
///
/// Segments are streamed to DISK progressively (Sink) instead of accumulated in
/// RAM — a full-length video's video track is ~1 GB, which would OOM the 2 GB
/// container. Only a tiny out-of-order reorder buffer + the current round's HTTP
/// response live in memory at once.
public final class SabrSession {

    /// Per-itag byte sink. open(itag) is called once, lazily, the first time we
    /// have bytes to write for that itag; the returned stream is written in
    /// fmp4 order (init first, then segments ascending) and closed by the session.
    /// Ziel der Mediendaten. ⚠️ SEGMENT-ADRESSIERT, nicht als Stream: die Datei
    /// liegt in sidx-Reihenfolge, und jedes Segment hat darin eine feste
    /// Position. Dadurch darf ein Segment auch dann geschrieben werden, wenn
    /// frueheren noch fehlen (Sprung im Video) — die Datei bekommt ein Loch
    /// statt eines Staus. Vorher war es ein fortlaufender Stream, weshalb ein
    /// fehlendes Segment alles Nachfolgende blockierte und Springen unmoeglich
    /// war. Die Byte-Anordnung bleibt exakt dieselbe wie bisher.
    public interface Sink {
        void writeInit(int itag, long lmt, byte[] data) throws IOException;
        void writeSegment(int itag, int seq, byte[] data) throws IOException;
    }

    public static final class Fmt {
        public final int itag;
        public final long lmt;
        public Fmt(int itag, long lmt) { this.itag = itag; this.lmt = lmt; }
    }

    public static final class Result {
        public int iterations;
        public boolean complete;
        public String stopReason = "";
        public final Map<Integer, Map<String, Object>> perFormat = new LinkedHashMap<>();
    }

    private final class FState {
        final Fmt fmt;
        byte[] pendingInit;               // init bytes, held until drainToDisk writes them
        boolean initWritten;
        final TreeMap<Integer, byte[]> buf = new TreeMap<>();  // received, not yet flushed
        final Set<Integer> seen = new HashSet<>();             // dedup + segment count
        /// seq -> {startMs, durationMs} JEDES empfangenen Segments. Grundlage der
        /// ehrlichen buffered_ranges: nur damit lassen sich zusammenhaengende
        /// Laeufe (und damit LOECHER) korrekt an den Server melden.
        final TreeMap<Integer, long[]> segTimes = new TreeMap<>();
        int maxSeq = 0;
        long bytesWritten = 0;
        int totalSegments = -1;
        long totalDurationMs = 0;
        long bufferedMs = 0;
        FState(Fmt f) { fmt = f; }
        long perSegMs() { return totalSegments > 0 ? totalDurationMs / totalSegments : 0; }
        long frontierMs() { return perSegMs() * maxSeq; }
        /// Ende des ERSTEN zusammenhaengenden Laufs — bis hierhin koennte ein
        /// echter Player tatsaechlich spielen. Der Play-Head darf NIE dahinter
        /// vorruecken: ein SABR-Server bedient die Abspielposition und fuellt
        /// keine Loecher HINTER dem Play-Head nach (2026-07-30 gemessen: Loch
        /// bei seg 23, player_time am Frontier bei 2333 s -> der Server
        /// antwortete nur noch 94 B und lieferte seg 23 nie; ein echter Player
        /// wuerde vor dem Loch stehen bleiben bzw. dorthin seeken).
        /// ⚠️ ANDROID-MediaHeader tragen KEINE startMs/durationMs (Felder 11/12
        /// leer — 2026-07-30 gemessen: buffered=0ms, ptMs klemmte auf 0, der
        /// Server schickte in Schleife dieselben 3,1 MB). Fallback auf die
        /// FORMAT_INIT-Schaetzung perSegMs(), wie der alte frontierMs()-Weg.
        long estStartMs(int seq, long raw) { return raw > 0 ? raw : perSegMs() * Math.max(0, seq - 1); }
        long estDurMs(long raw) { return raw > 0 ? raw : perSegMs(); }

        /// Letztes Segment des ersten zusammenhaengenden Laufs — bis hierhin ist
        /// die Datei auf Platte lueckenlos, nur das darf als Fortsetzpunkt
        /// gespeichert werden.
        int contiguousEndSeq() {
            int last = 0, prev = Integer.MIN_VALUE;
            for (int q : segTimes.keySet()) {
                if (prev != Integer.MIN_VALUE && q != prev + 1) break;
                last = q; prev = q;
            }
            return last;
        }

        long contiguousEndMs() {
            long end = 0; int prev = Integer.MIN_VALUE;
            for (var e : segTimes.entrySet()) {
                if (prev != Integer.MIN_VALUE && e.getKey() != prev + 1) break;
                end = estStartMs(e.getKey(), e.getValue()[0]) + estDurMs(e.getValue()[1]);
                prev = e.getKey();
            }
            return end;
        }
        boolean complete() { return totalSegments > 0 && seen.size() >= totalSegments && initWritten; }

        /// Alles Angekommene an SEINE Position schreiben — unabhaengig von der
        /// Reihenfolge. Vorher wurde nur der zusammenhaengende Lauf ab
        /// writeCursor geschrieben; ein fehlendes Segment staute damit alles
        /// Nachfolgende im RAM und machte Springen unmoeglich. Die Zieldatei
        /// bekommt jetzt an fehlenden Stellen ein Loch, die Byte-Anordnung
        /// bleibt aber identisch (sidx-Reihenfolge).
        void drainToDisk() throws IOException {
            if (pendingInit != null && !initWritten) {
                sink.writeInit(fmt.itag, fmt.lmt, pendingInit);
                bytesWritten += pendingInit.length;
                pendingInit = null;
                initWritten = true;
            }
            if (!initWritten) return;                 // ohne Init keine Offsets
            for (var e : buf.entrySet()) {
                sink.writeSegment(fmt.itag, e.getKey(), e.getValue());
                bytesWritten += e.getValue().length;
            }
            buf.clear();
        }

        void flush() { }

        void close() { }
    }

    private static final class Pending {
        int itag = -1;
        long lmt = 0;
        boolean isInit;
        int seq;
        long startMs;
        long durationMs;
        final ByteArrayOutputStream data = new ByteArrayOutputStream();
    }

    private String abrUrl;
    private int requestNo = 0;
    private byte[] ustreamerConfig;   // bei Re-Attest aus neuem Player-Call ersetzt
    private final byte[] clientInfo;
    private final String userAgent;
    private final Fmt prefAudio;
    private final Fmt prefVideo;
    /// Alle vom Player angebotenen Formate. Der echte Web-Player nennt im
    /// Mitschnitt 2026-07-25 VIER Audio- und ZWOELF Video-Kandidaten (Felder
    /// 16/17), nicht je einen. Bei uns war es je EINER — und der wurde bei
    /// doppelt vorkommendem itag (140 zweimal mit verschiedenem lmt) auch noch
    /// geraten. Passt das format_id (itag+lmt) nicht, faengt der Server nichts an.
    private java.util.List<Fmt> allAudio = new java.util.ArrayList<>();
    private java.util.List<Fmt> allVideo = new java.util.ArrayList<>();
    public void setFormatCandidates(java.util.List<Fmt> a, java.util.List<Fmt> v) {
        if (a != null) allAudio = a;
        if (v != null) allVideo = v;
    }
    /// Laeuft diese Session als reiner WEB-Client (clientMode 3)? Entscheidet die
    /// DEFAULT-Form des client_abr_state: WEB braucht die Referenz-Form (drei
    /// Felder, s. clientAbrState) — mit der alten Zwei-Feld-Form antwortet
    /// googlevideo dem WEB-Client mit 403 bzw. leeren Rahmen (2026-07-30 beim
    /// Umstellen auf WEB-Default reproduziert). ANDROID laeuft unveraendert mit
    /// der alten Form, die dort nachweislich komplette Downloads liefert.
    private boolean webClient;
    public void setWebClient(boolean w) { this.webClient = w; }

    /// Abbruchbedingung, vor jeder Runde geprueft. Damit kann ein
    /// HINTERGRUND-Download zuruecktreten, sobald der Nutzer ein anderes Video
    /// antippt: ein Voll-Download dauert Minuten, und die Downloads laufen
    /// serialisiert — ohne das Zuruecktreten stuende der echte Tap hinter den
    /// Prefetches der App in der Warteschlange. Das bereits Geholte bleibt
    /// erhalten (die .bin waechst pro Runde), es geht also nichts verloren.
    private java.util.function.BooleanSupplier stopWhen;
    public void setStopWhen(java.util.function.BooleanSupplier s) { this.stopWhen = s; }

    /// „Weit genug voraus" — vor jeder Runde geprueft. Solange wahr, wird NICHT
    /// gefragt, sondern gewartet. Damit laeuft die Session bedarfsgetrieben: sie
    /// holt so viel Vorlauf, wie der Player (per Range-Requests) angefordert
    /// hat, und schweigt danach. Der Server ist damit einverstanden — sein
    /// `max_time_since_last_request_ms` (NEXT_REQUEST_POLICY Feld 3, typisch
    /// 60 s) sagt sogar genau, wie lange wir schweigen duerfen; laenger nicht,
    /// sonst laeuft die Sitzung ab. Deshalb hier ein Heartbeat kurz davor.
    private java.util.function.BooleanSupplier pauseWhen;
    public void setPauseWhen(java.util.function.BooleanSupplier s) { this.pauseWhen = s; }

    /// SPRUNGZIEL. Liefert die Segmentnummer, die der Player JETZT braucht, oder
    /// -1. Damit springt die laufende Sitzung an eine beliebige Stelle des
    /// Videos, statt stur von vorne weiterzuladen — SABR kann das, die
    /// Abspielposition IST die Steuergroesse. Ohne das war Vorspulen auf den
    /// bereits geladenen Anfang beschraenkt.
    private java.util.function.IntSupplier seekTargetSeq;
    public void setSeekTargetSeq(java.util.function.IntSupplier s) { this.seekTargetSeq = s; }

    /// FORTSETZEN AB SEGMENT N. Was schon auf Platte liegt, beschreibt der
    /// Aufrufer hier; die Session tut dann so, als haette sie diese Segmente in
    /// dieser Sitzung geholt: sie meldet sie in `buffered_ranges`, setzt die
    /// Spielzeit ans Ende des Vorhandenen und schreibt erst ab N+1 weiter.
    /// Ohne das begann jede Wiederanfahrt wieder bei Segment 1 — der
    /// keep-larger-Publish rettete zwar den Cache, aber die Bytes wurden ein
    /// zweites Mal geholt.
    /// ⚠️ `lmt` ist Teil der Identitaet: YouTube liefert fuer dasselbe itag je
    /// nach Player-Call VERSCHIEDENE Encodes (gemessen 2026-07-31 an EINEM
    /// Video: 553 / 640 / 656 Segmente). Segmentnummern gelten nur innerhalb
    /// eines Encodes — ohne diesen Abgleich wuerde „weiter ab Segment 15" auf
    /// die falsche Fassung zeigen (der Server antwortet dann mit einem leeren
    /// Rahmen) und beim Anhaengen zwei Encodes in EINER Datei vermischen.
    public static final class Resume {
        public final int lastSeq; public final long bufferedMs;
        public final int totalSegments; public final long totalDurationMs;
        public final long lmt;
        public Resume(int lastSeq, long bufferedMs, int totalSegments, long totalDurationMs, long lmt) {
            this.lastSeq = lastSeq; this.bufferedMs = bufferedMs;
            this.totalSegments = totalSegments; this.totalDurationMs = totalDurationMs;
            this.lmt = lmt;
        }
    }
    private Map<Integer, Resume> resume = Map.of();
    public void setResume(Map<Integer, Resume> r) { if (r != null && !r.isEmpty()) resume = r; }

    /// Fortschrittsmeldung pro Runde — die Cache-Schicht schreibt daraus ihre
    /// `.state`-Datei, damit die naechste Sitzung fortsetzen kann.
    public interface ProgressSink {
        void report(int itag, int lastContiguousSeq, long bufferedMs, int totalSegments,
                    long totalDurationMs, long lmt);
        /// Wurde fuer dieses itag tatsaechlich fortgesetzt? Nur dann darf die
        /// Cache-Schicht den neuen Schwanz an die vorhandene Datei ANHAENGEN —
        /// sonst enthaelt die .part das Video ab Segment 1 und das Anhaengen
        /// wuerde die Datei zerstoeren.
        void resumeApplied(int itag, boolean applied);
    }
    private ProgressSink progressSink;
    public void setProgressSink(ProgressSink p) { this.progressSink = p; }

    /// Zustand aus `resume` in die FStates spiegeln. Die Zeitstempel werden aus
    /// der Gesamtdauer geschaetzt (wie estStartMs/estDurMs bei ANDROID) — dem
    /// Server kommt es auf die Segment-INDIZES an, und die stimmen exakt.
    private void applyResume(Map<Integer, FState> states) {
        for (var e : resume.entrySet()) {
            final int itag = e.getKey();
            final Resume r = e.getValue();
            if (r.lastSeq <= 0 || r.totalSegments <= 0) continue;
            final Fmt fmt = prefAudio != null && prefAudio.itag == itag ? prefAudio
                    : prefVideo != null && prefVideo.itag == itag ? prefVideo : null;
            if (fmt == null) continue;
            // Anderer Encode als der gecachte -> NICHT fortsetzen (s. Resume).
            if (fmt.lmt != r.lmt) {
                System.out.println("[Sabr] " + itag + " KEIN Fortsetzen: anderer Encode"
                        + " (Cache lmt=" + r.lmt + ", jetzt lmt=" + fmt.lmt + ") -> von vorn");
                if (progressSink != null) progressSink.resumeApplied(itag, false);
                continue;
            }
            final FState s = states.computeIfAbsent(itag, k -> new FState(fmt));
            s.totalSegments = r.totalSegments;
            s.totalDurationMs = r.totalDurationMs;
            s.bufferedMs = r.bufferedMs;
            s.maxSeq = r.lastSeq;
            s.initWritten = true;              // Init liegt bereits in der Datei
            final long perSeg = s.perSegMs();
            for (int q = 1; q <= r.lastSeq; q++) {
                s.seen.add(q);
                s.segTimes.put(q, new long[]{perSeg * (q - 1), perSeg});
            }
            System.out.println("[Sabr] " + itag + " fortsetzen ab Segment " + (r.lastSeq + 1)
                    + "/" + r.totalSegments + " (" + r.bufferedMs + "ms gepuffert, lmt=" + r.lmt + ")");
            if (progressSink != null) progressSink.resumeApplied(itag, true);
        }
    }
    private byte[] poToken;         // decoded gvs po_token bytes, or null (mid-session erneuerbar)
    /// Liefert einen FRISCHEN content-bound po_token. Wird aufgerufen, wenn der
    /// Server STREAM_PROTECTION_STATUS=3 („Attestierung erforderlich") meldet.
    public interface TokenRefresher { byte[] fresh(); }
    private TokenRefresher tokenRefresher;
    public void setTokenRefresher(TokenRefresher r) { this.tokenRefresher = r; }

    /// Vollstaendige Session-Erneuerung: NEUER Player-Call → frische abrUrl +
    /// ustreamerConfig + po_token. Nur einen frischen Token nachzureichen genuegt
    /// nachweislich NICHT (2026-07-25: videoId- UND visitorData-gebunden getestet,
    /// beide abgelehnt, Server bleibt bei prot=3). Der Zustand (`states` mit
    /// buffered_range/seen) bleibt erhalten, damit wir dort weitermachen, wo die
    /// alte Session stehen geblieben ist.
    public static final class Renewal {
        public final String abrUrl; public final byte[] ustreamerConfig; public final byte[] poToken;
        public Renewal(String u, byte[] c, byte[] p) { abrUrl = u; ustreamerConfig = c; poToken = p; }
    }
    public interface SessionRefresher { Renewal fresh(); }
    private SessionRefresher sessionRefresher;
    public void setSessionRefresher(SessionRefresher r) { this.sessionRefresher = r; }
    private final String egressFamily; // "v4"/"v6" — gvs URLs are IP-signed, so the
                                       // session MUST egress on the same family the
                                       // player resolve used. null = global ACTIVE.
    private Sink sink;              // set at fetch start

    public SabrSession(String abrUrl, byte[] ustreamerConfig, Fmt prefAudio, Fmt prefVideo,
                       byte[] clientInfo, String userAgent) {
        this(abrUrl, ustreamerConfig, prefAudio, prefVideo, clientInfo, userAgent, null, null);
    }

    public SabrSession(String abrUrl, byte[] ustreamerConfig, Fmt prefAudio, Fmt prefVideo,
                       byte[] clientInfo, String userAgent, byte[] poToken) {
        this(abrUrl, ustreamerConfig, prefAudio, prefVideo, clientInfo, userAgent, poToken, null);
    }

    public SabrSession(String abrUrl, byte[] ustreamerConfig, Fmt prefAudio, Fmt prefVideo,
                       byte[] clientInfo, String userAgent, byte[] poToken, String egressFamily) {
        this.abrUrl = abrUrl;
        this.ustreamerConfig = ustreamerConfig;
        this.prefAudio = prefAudio;
        this.prefVideo = prefVideo;
        this.clientInfo = clientInfo;
        this.userAgent = userAgent;
        this.poToken = poToken;
        this.egressFamily = egressFamily;
    }

    public Result fetchAll(int maxIterations, Sink sink) throws Exception {
        return fetchAll(maxIterations, sink, false, null);
    }

    /// paced=true: real-time 1x playback emulation. Made-for-kids gvs enforces a
    /// server-side readahead cap RELATIVE to the claimed player_time, and it
    /// rejects a player_time that advances faster than wall-clock (the burst mode
    /// below claims ~36s of "playback" within ~10s -> the server stops serving;
    /// 2026-07-24 verdict on top of the 2026-07-23 token-shape matrix). Paced mode
    /// advances player_time no faster than wall-clock (+ a small head start) and
    /// sleeps between rounds — the window slides like a real client and the
    /// download runs at ~1x. publishHook (optional) runs once per round after the
    /// disk flush so the caller can republish the growing file for live serving.
    /// Re-Attest-Versuch: DEFAULT AUS (`YT_SABR_REATTEST=1` schaltet ihn an).
    ///
    /// ── Protokoll-Analyse 2026-07-25: FÜNF Hypothesen getestet, alle widerlegt ──
    /// Der Server meldet `STREAM_PROTECTION_STATUS{1=3, 2=10}` („Attestierung
    /// erforderlich", 10 Wiederholungen erlaubt) und liefert danach nichts mehr,
    /// egal was wir tun:
    ///   1. frischer content-bound po_token (videoId)        -> weiter prot=3
    ///   2. frischer visitor-bound po_token (599B-Form)      -> weiter prot=3
    ///   3. KOMPLETT neuer Player-Call (neue abrUrl +
    ///      ustreamerConfig + Token)                          -> weiter prot=3
    ///   4. dito + playbackCookie fallen gelassen             -> weiter prot=3
    ///   5. Token zusätzlich als `pot=`-Query am ABR-POST     -> weiter prot=3
    /// Der BotGuard-Helfer liefert nachweislich JEDES MAL einen anderen Token
    /// (3x geprüft), Frische ist also nicht das Problem. WEB_EMBEDDED als
    /// Session-Client scheidet aus (liefert gar keine serverAbrStreamingUrl).
    /// Ebenfalls unbehandelt, aber unkritisch: UMP-Typen 47 (playback-start
    /// policy), 49 (bandwidth-sampling hint), 51 (selectable formats).
    /// OFFENE FÄDEN für den nächsten Anlauf: (a) der Token muss evtl. an einer
    /// anderen Stelle der ABR-Anfrage stehen als streamerContext-Feld 2;
    /// (b) unser BotGuard-Helfer erzeugt evtl. eine Token-ART, die der
    /// gvs-STREAMING-Pfad grundsätzlich nicht akzeptiert (für Player-Calls und
    /// Direkt-URL-`pot=` funktioniert sie); (c) ein echter Client-Mitschnitt
    /// (Charles/mitmproxy gegen den YT-Web-Player) würde die Frage in Minuten
    /// klären, statt sie weiter zu erraten.
    /// Der Mechanismus ist belegt (prot 2->3 ist das Stopp-Signal), aber ein
    /// frisch gemünzter content-bound po_token wird NICHT akzeptiert: der Server
    /// bleibt bei prot=3 und sendet nichts (40 Erneuerungen, 0 neue Segmente,
    /// verifiziert 2026-07-25). Vermutete Ursache: die Bindung passt nicht — der
    /// Streaming-Token muss vermutlich an die SESSION (visitorData/ustreamer-
    /// Config des Player-Calls) gebunden sein, nicht nur an die videoId; oder
    /// prot=3 verlangt einen komplett neuen Player-Call statt nur eines Tokens.
    /// Bis das geklärt ist: aus, sonst kostet es pro Session ~40 Mints + ~40
    /// Roundtrips für nichts.
    private static final int MAX_REATTESTS =
            "1".equals(System.getenv("YT_SABR_REATTEST")) ? 40 : 0;
    private static final boolean POT_IN_URL = "1".equals(System.getenv("YT_SABR_POT_IN_URL"));
    private static final boolean TRACE = "1".equals(System.getenv("YT_SABR_TRACE"));
    private final java.util.Map<Integer, Integer> unknownParts = new java.util.TreeMap<>();
    private final java.util.Set<Integer> dumpedTypes = new java.util.HashSet<>();

    /// SABR_CONTEXT_UPDATE (UMP-Typ 57). Struktur aus dem Hexdump 2026-07-25:
    ///   1=type(varint) · 2=scope(varint) · 3=value(bytes) · 4=send_by_default · 5=write_policy
    /// Bei `send_by_default=1` MUSS der Client den Kontext in jeder Folgeanfrage
    /// zurückspiegeln (streamerContext, repeated sabr_contexts) — sonst liefert
    /// der Server gar keine Medien mehr (gemessen: konstant 105B nur mit 57+67).
    private static final int SABR_CONTEXT_UPDATE = 57;
    private final java.util.Map<Integer, byte[]> sabrContexts = new java.util.LinkedHashMap<>();

    private void handleContextUpdate(byte[] payload) {
        int ctype = -1; byte[] value = null; boolean sendByDefault = false;
        try {
            final ProtoReader r = new ProtoReader(payload);
            while (r.hasMore()) {
                final int f = r.readTag();
                if (f == 1 && r.wireType() == 0) ctype = (int) r.readVarint();
                else if (f == 3 && r.wireType() == 2) value = r.readBytes();
                else if (f == 4 && r.wireType() == 0) sendByDefault = r.readVarint() != 0;
                else r.skip();
            }
        } catch (Exception ignored) { return; }
        if (ctype >= 0 && value != null && sendByDefault) {
            // ⚠️ NICHT die rohe SabrContextUpdate spiegeln (2026-07-30 probiert →
            // SABR_ERROR): Feld 5 erwartet `SabrContext {1=type, 2=value}`, der
            // Server sendet aber `SabrContextUpdate {1=type, 2=scope, 3=value,
            // 4=send_by_default, 5=write_policy}`. Die Umsetzung value(3)→(2) ist
            // richtig; genau das macht die Referenz-Implementierung auch.
            final byte[] prev = sabrContexts.put(ctype, value);
            if (prev == null)
                System.out.println("[Sabr] SabrContext übernommen: type=" + ctype
                        + " value=" + value.length + "B");
        }
    }

    /// NEXT_REQUEST_POLICY (UMP-Typ 35): der Server steuert damit die TAKTUNG der
    /// Folgerunden — das ist die „bedarfsgesteuerte Taktung", die der Referenz-
    /// Implementierung ihre Medien beschert und uns fehlte. Proto (LuanRT/
    /// googlevideo, protos/video_streaming/next_request_policy.proto):
    ///   1=target_audio_readahead_ms · 2=target_video_readahead_ms
    ///   3=max_time_since_last_request_ms · 4=backoff_time_ms · 7=playback_cookie
    /// Die Referenz wartet vor JEDER Folgerunde `backoff_time_ms` (SabrStream.
    /// executeWithRetry) — wir lasen bisher NUR das Cookie (Feld 7) und feuerten
    /// sofort weiter. Befund 2026-07-30: sofortiges Feuern quittiert der Server
    /// mit leeren UMP-Rahmen (105 B → 11 B, kein Fehler); mit Rundenabstand
    /// (YT_SABR_GAP_MS-Probe: 4000/2000/1000 ms) lieferte er komplette Videos.
    /// Kill-Switch: YT_SABR_IGNORE_BACKOFF=1 stellt das alte Verhalten her.
    private long policyBackoffMs = 0;
    private long policyTargetAudioMs = 0;
    private long policyTargetVideoMs = 0;
    private long policyMaxIdleMs = 0;

    /// Parst die volle Policy in die policy*-Felder; Rueckgabe = playback_cookie
    /// (Feld 7) oder null, wenn keins enthalten war (dann altes Cookie behalten —
    /// ein fehlendes Feld ist keine Anweisung, das Cookie zu verwerfen).
    private byte[] handleNextRequestPolicy(byte[] p) {
        byte[] ck = null;
        long backoff = 0, ta = 0, tv = 0, maxIdle = 0;
        try {
            final ProtoReader r = new ProtoReader(p);
            while (r.hasMore()) {
                final int f = r.readTag();
                if (r.wireType() == 0) {
                    final long v = r.readVarint();
                    switch (f) {
                        case 1: ta = v; break;
                        case 2: tv = v; break;
                        case 3: maxIdle = v; break;
                        case 4: backoff = v; break;
                        default: break;
                    }
                } else if (f == 7 && r.wireType() == 2) {
                    ck = r.readBytes();
                } else {
                    r.skip();
                }
            }
        } catch (Exception ignored) { return null; }
        final boolean changed = backoff != policyBackoffMs || ta != policyTargetAudioMs
                || tv != policyTargetVideoMs || maxIdle != policyMaxIdleMs;
        policyBackoffMs = backoff;
        policyTargetAudioMs = ta;
        policyTargetVideoMs = tv;
        policyMaxIdleMs = maxIdle;
        if (TRACE && changed)
            System.out.println("[Sabr] NextRequestPolicy backoff=" + backoff
                    + "ms targetA=" + ta + "ms targetV=" + tv
                    + "ms maxIdle=" + maxIdle + "ms cookie=" + (ck != null ? ck.length + "B" : "-"));
        return ck;
    }

    /// STREAM_PROTECTION_STATUS (UMP-Typ 58): Feld 1 = Status der Session-
    /// Attestierung. Bekannte Werte: 1=OK, 2=ausstehend, 3=Attestierung noetig.
    /// ⚠️ Wir haben diesen Typ bisher DEKLARIERT, aber nie ausgewertet — er lief
    /// in den default-Zweig. Wenn der Server hier „Attestierung noetig" meldet und
    /// wir stur weiter dasselbe Paket schicken, hoert er auf, Medien zu senden:
    /// exakt unser Bild „resp=808B new=0" bis zum stuck-Abbruch.
    private static int readProtectionStatus(byte[] payload) {
        int status = -1;
        final StringBuilder dump = TRACE ? new StringBuilder() : null;
        try {
            final ProtoReader r = new ProtoReader(payload);
            while (r.hasMore()) {
                final int f = r.readTag();
                if (r.wireType() == 0) {
                    final long v = r.readVarint();
                    if (f == 1) status = (int) v;
                    if (dump != null) dump.append(' ').append(f).append('=').append(v);
                } else {
                    if (dump != null) dump.append(' ').append(f).append("=<len>");
                    r.skip();
                }
            }
        } catch (Exception ignored) { }
        if (dump != null && dump.length() > 0)
            System.out.println("[Sabr] STREAM_PROTECTION_STATUS Felder:" + dump);
        return status;
    }

    public Result fetchAll(int maxIterations, Sink sink, boolean paced, Runnable publishHook) throws Exception {
        this.sink = sink;
        final Map<Integer, FState> states = new LinkedHashMap<>();
        byte[] playbackCookie = null;
        long playerTimeMs = 0;
        int stuckRounds = 0;
        int reattests = 0;
        final int stuckLimit = paced ? 24 : 3;   // paced: ~2 min quiet before giving up
        final long wallStart = System.currentTimeMillis();
        final Result res = new Result();
        applyResume(states);
        if (!states.isEmpty()) {
            // Ab dem Ende des Vorhandenen weiterfragen, nicht bei 0.
            playerTimeMs = states.values().stream().mapToLong(FState::contiguousEndMs).min().orElse(0);
        }
        String stopReason = "maxIterations";
        System.out.println("[Sabr] session start maxIter=" + maxIterations
                + (paced ? " PACED" : "")
                + " poToken=" + (poToken != null ? poToken.length + "B" : "NONE"));

        try {
            for (int iter = 0; iter < maxIterations; iter++) {
                res.iterations = iter + 1;
                if (stopWhen != null && stopWhen.getAsBoolean()) { stopReason = "idle"; break; }
                // BEDARFSGETRIEBEN: warten, solange genug Vorlauf da ist. Der
                // Player fordert per Range weiter an, das hebt die Bremse wieder
                // auf. Spaetestens nach `max_time_since_last_request_ms` (Policy
                // Feld 3, default hier 45 s) fragen wir trotzdem einmal, damit
                // die Sitzung nicht abläuft.
                if (pauseWhen != null) {
                    final long heartbeat = policyMaxIdleMs > 0
                            ? Math.max(5_000, policyMaxIdleMs - 15_000) : 45_000;
                    final long pauseStart = System.currentTimeMillis();
                    boolean logged = false;
                    while (pauseWhen.getAsBoolean()) {
                        if (stopWhen != null && stopWhen.getAsBoolean()) break;
                        if (System.currentTimeMillis() - pauseStart >= heartbeat) break;
                        if (!logged) {
                            System.out.println("[Sabr] genug Vorlauf — warte auf Player-Bedarf");
                            logged = true;
                        }
                        try { Thread.sleep(1_000); }
                        catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
                    }
                    if (stopWhen != null && stopWhen.getAsBoolean()) { stopReason = "idle"; break; }
                }
                // BEDARFSGESTEUERTE TAKTUNG: vor jeder Folgerunde die vom Server
                // per NEXT_REQUEST_POLICY (Feld 4, backoff_time_ms) angeordnete
                // Wartezeit einhalten — exakt wie die Referenz-Implementierung
                // (SabrStream.executeWithRetry). Beleg 2026-07-30: sofortiges
                // Feuern → leere UMP-Rahmen (105 B → 11 B); mit Rundenabstand
                // (GAP-Probe 1000–4000 ms) → komplette Videos inkl. itag 140.
                // Die Policy bleibt gueltig, bis der Server eine neue schickt.
                // YT_SABR_GAP_MS wirkt zusaetzlich als manueller Mindestabstand
                // (Experiment-Schalter); es gilt das Maximum aus beidem.
                // Sicherheitsdeckel 60 s gegen absurde Server-Werte.
                if (iter > 0) {
                    long waitMs = "1".equals(System.getenv("YT_SABR_IGNORE_BACKOFF"))
                            ? 0 : policyBackoffMs;
                    final String gap = System.getenv("YT_SABR_GAP_MS");
                    if (gap != null && !gap.isEmpty()) {
                        try { waitMs = Math.max(waitMs, Long.parseLong(gap.trim())); }
                        catch (NumberFormatException ignored) { }
                    }
                    if (waitMs > 60_000) {
                        System.out.println("[Sabr] backoff " + waitMs + "ms auf 60s gedeckelt");
                        waitMs = 60_000;
                    }
                    if (waitMs > 0) {
                        try { Thread.sleep(waitMs); }
                        catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
                    }
                }
                // Sprungziel beruecksichtigen: der Player hat Bytes angefordert,
                // die woanders im Video liegen -> Abspielposition dorthin setzen.
                if (seekTargetSeq != null) {
                    final int want = seekTargetSeq.getAsInt();
                    if (want > 0) {
                        long perSeg = 0;
                        for (FState st : states.values())
                            if (st.perSegMs() > 0) { perSeg = st.perSegMs(); break; }
                        if (perSeg > 0) {
                            final long targetMs = perSeg * (want - 1);
                            if (Math.abs(targetMs - playerTimeMs) > perSeg) {
                                System.out.println("[Sabr] Sprung auf Segment " + want
                                        + " (" + targetMs + "ms)");
                                playerTimeMs = targetMs;
                            }
                        }
                    }
                }
                final byte[] resp = post(buildRequest(states, playerTimeMs, playbackCookie));

                final Map<Long, Pending> pend = new HashMap<>();
                final int[] newSegments = {0};
                final byte[][] cookie = {playbackCookie};
                final boolean[] sabrError = {false};
                final boolean[] redirected = {false};
                final int[] protectionStatus = {-1};

                UmpReader.parse(resp, (type, payload) -> {
                    switch (type) {
                        case UmpReader.FORMAT_INIT_METADATA: handleFormatInit(payload, states); break;
                        case UmpReader.MEDIA_HEADER: handleHeader(payload, pend); break;
                        case UmpReader.MEDIA: handleMedia(payload, pend); break;
                        case UmpReader.MEDIA_END: finalizeOne(payload, pend, states, newSegments); break;
                        case UmpReader.NEXT_REQUEST_POLICY: {
                            final byte[] ck = handleNextRequestPolicy(payload);
                            if (ck != null) cookie[0] = ck;
                            break;
                        }
                        case UmpReader.SABR_REDIRECT: { String u = extractRedirect(payload); if (u != null) { abrUrl = u; redirected[0] = true; } break; }
                        case UmpReader.SABR_ERROR: sabrError[0] = true; break;
                        case SABR_CONTEXT_UPDATE: handleContextUpdate(payload); break;
                        case UmpReader.STREAM_PROTECTION_STATUS:
                            protectionStatus[0] = readProtectionStatus(payload); break;
                        default:
                            // DIAGNOSE (YT_SABR_TRACE=1): unbekannte UMP-Typen mitschreiben.
                            // Der Web-Player wertet mehr aus als wir; was wir ignorieren,
                            // kann genau die Anweisung sein, die die Session am Leben haelt.
                            if (TRACE) {
                                unknownParts.merge(type, 1, Integer::sum);
                                // Beim ERSTEN Auftreten die Rohbytes hexdumpen, damit die
                                // Protobuf-Struktur ablesbar ist (Feldnummern/Wire-Types)
                                // statt geraten werden muss.
                                if (dumpedTypes.add(type)) {
                                    final StringBuilder hx = new StringBuilder();
                                    for (int i = 0; i < Math.min(payload.length, 160); i++)
                                        hx.append(String.format("%02x", payload[i]));
                                    System.out.println("[Sabr] UMP-Typ " + type + " len=" + payload.length
                                            + " hex=" + hx);
                                }
                            }
                            break;
                    }
                });
                // sweep any segment whose MEDIA_END we didn't see, then flush this
                // round's contiguous run to disk (outside the parse lambda so IO can
                // throw). buf holds only not-yet-contiguous gaps between rounds.
                for (Map.Entry<Long, Pending> e : pend.entrySet()) store(e.getValue(), states, newSegments);
                for (FState s : states.values()) s.drainToDisk();

                // per-round trace: how far each format has actually pulled + why the
                // server might stop. This is what pins down the SABR cap mechanism.
                if (iter < 8 || newSegments[0] == 0 || sabrError[0] || redirected[0] || (iter % 25 == 0)) {
                    final StringBuilder sb = new StringBuilder();
                    for (FState s : states.values()) {
                        sb.append(' ').append(s.fmt.itag).append(":seg").append(s.seen.size())
                          .append('/').append(s.totalSegments).append(",disk").append(s.bytesWritten >> 20).append("MB");
                        // Loch-Diagnose: erstes fehlendes Segment innerhalb der
                        // empfangenen Folge. Genau dieser Fall blockierte am
                        // 2026-07-30 den writeCursor (RAM-Stau + stuck-Abbruch).
                        int prevSeq = Integer.MIN_VALUE, firstGap = -1;
                        for (int seq : s.segTimes.keySet()) {
                            if (prevSeq != Integer.MIN_VALUE && seq != prevSeq + 1 && firstGap < 0)
                                firstGap = prevSeq + 1;
                            prevSeq = seq;
                        }
                        if (firstGap >= 0) sb.append(",LOCH@").append(firstGap);
                    }
                    System.out.println("[Sabr] iter=" + res.iterations + " resp=" + resp.length + "B new=" + newSegments[0]
                            + " ptMs=" + playerTimeMs + (sabrError[0] ? " SABR_ERROR" : "")
                            + (protectionStatus[0] >= 0 ? " prot=" + protectionStatus[0] : "")
                            + (TRACE && !unknownParts.isEmpty() ? " unhandled=" + unknownParts : "")
                            + (redirected[0] ? " REDIRECT" : "") + sb);
                }

                playbackCookie = cookie[0];

                // ── Re-Attest (2026-07-25) ─────────────────────────────────────
                // DER eigentliche Grund für die vermeintliche „Made-for-Kids-
                // Readahead-Sperre": der Server meldet per STREAM_PROTECTION_STATUS
                // 2 (ausstehend) -> 3 (Attestierung ERFORDERLICH) und stellt das
                // Senden ein. Belegt im Trace: prot=2 solange Medien fliessen, ab
                // der ersten prot=3-Runde nur noch resp≈1KB mit new=0. Wir haben
                // das Feld bisher ignoriert und stur weitergefragt — daher sah es
                // nach einem 60s-Fenster-Cap aus. Ein echter Client mintet dann
                // einen frischen po_token und macht in DERSELBEN Session weiter.
                if (protectionStatus[0] == 3 && reattests < MAX_REATTESTS) {
                    reattests++;
                    if (sessionRefresher != null) {
                        final Renewal rn = sessionRefresher.fresh();
                        if (rn != null && rn.ustreamerConfig != null) {
                            if (rn.abrUrl != null) abrUrl = rn.abrUrl;
                            ustreamerConfig = rn.ustreamerConfig;
                            if (rn.poToken != null) poToken = rn.poToken;
                            // Das playbackCookie identifiziert die ALTE, vom Server als
                            // un-attestiert verworfene Sitzung. Ein echter Client faengt
                            // nach einem Reload eine NEUE an und setzt nur die Position
                            // fort — also Cookie fallen lassen, `states` (buffered_range
                            // + seen) aber behalten, damit wir bei Segment N weitermachen.
                            playbackCookie = null;
                            System.out.println("[Sabr] Session erneuert (#" + reattests
                                    + ", ust=" + rn.ustreamerConfig.length + "B, pot="
                                    + (rn.poToken != null ? rn.poToken.length + "B" : "-")
                                    + ") nach prot=3");
                            continue;
                        }
                        System.out.println("[Sabr] prot=3: Session-Erneuerung fehlgeschlagen");
                    } else if (tokenRefresher != null) {
                        final byte[] fresh = tokenRefresher.fresh();
                        if (fresh != null && fresh.length > 0) {
                            poToken = fresh;
                            System.out.println("[Sabr] Attestierung erneuert (#" + reattests
                                    + ", " + fresh.length + "B) nach prot=3");
                            continue;
                        }
                    }
                }

                if (sabrError[0]) { stopReason = "SABR_ERROR"; break; }
                // ⚠️ NICHT bei leerer states-Map "complete" melden: allMatch() ist auf
                // einer leeren Menge trivial wahr -> die Session brach nach Runde 1
                // mit complete=true und 0 Segmenten ab, obwohl noch gar kein
                // FORMAT_INIT angekommen war.
                if (!states.isEmpty() && states.values().stream().allMatch(FState::complete)) {
                    stopReason = "complete"; break;
                }
                if (newSegments[0] == 0) {
                    if (++stuckRounds > stuckLimit) { stopReason = "stuck(no new segments " + (stuckLimit + 1) + " rounds)"; break; }
                } else {
                    stuckRounds = 0;
                }
                // advance the playback head to the CONTIGUOUS buffered frontier
                // (min across formats, so audio+video march together) to pull the
                // next window. Bewusst NICHT frontierMs() (= maxSeq-basiert): bei
                // einem Loch in der Segmentfolge muss der Play-Head VOR dem Loch
                // stehen bleiben, sonst backfillt der Server es nie (s.
                // contiguousEndMs). Mit ehrlichen buffered_ranges sieht der
                // Server das Loch UND einen Play-Head davor -> er liefert nach,
                // der Lauf verschmilzt, der Frontier rueckt weiter.
                final long frontier = states.values().stream().mapToLong(FState::contiguousEndMs).min().orElse(playerTimeMs);
                if (!paced) {
                    playerTimeMs = frontier;
                    // Pro Runde flushen und veroeffentlichen, damit der Cache
                    // WAEHREND des Downloads waechst: eine komplette Session
                    // dauert Minuten, und ohne das entsteht die servierte Datei
                    // erst am Ende (erster Tap wartete gemessen 161 s).
                    if (publishHook != null) {
                        for (FState s : states.values()) s.flush();
                        // ERST melden, DANN veroeffentlichen: der Hook schreibt
                        // die .state-Datei und braucht dafuer den Stand, der
                        // gleich in der .bin landet.
                        if (progressSink != null) {
                            for (FState s : states.values())
                                progressSink.report(s.fmt.itag, s.contiguousEndSeq(), s.bufferedMs,
                                        s.totalSegments, s.totalDurationMs, s.fmt.lmt);
                        }
                        publishHook.run();
                    }
                } else {
                    // never claim playback ahead of wall-clock (+8s head start)
                    playerTimeMs = Math.min(frontier, (System.currentTimeMillis() - wallStart) + 8_000);
                    for (FState s : states.values()) s.flush();
                    if (publishHook != null) publishHook.run();
                    Thread.sleep(newSegments[0] > 0 ? 1_000 : 5_000);
                }
            }

            res.complete = !states.isEmpty() && states.values().stream().allMatch(FState::complete);
            // ⚠️ ABSCHLUSSMELDUNG. Die Schleife bricht bei `complete`/`stuck` VOR
            // dem naechsten Publish-Hook ab, der Fortsetzpunkt des Aufrufers
            // haengt sonst ein bis zwei Runden zurueck — und eine spaetere
            // Fortsetzung wuerde bereits vorhandene Segmente ein zweites Mal
            // anhaengen (Datei kaputt). Deshalb hier der Endstand.
            if (progressSink != null) {
                for (FState s : states.values())
                    progressSink.report(s.fmt.itag, s.contiguousEndSeq(), s.bufferedMs,
                            s.totalSegments, s.totalDurationMs, s.fmt.lmt);
            }
            for (FState s : states.values()) {
                final Map<String, Object> info = new LinkedHashMap<>();
                info.put("segments", s.seen.size());
                info.put("totalSegments", s.totalSegments);
                info.put("hasInit", s.initWritten);
                info.put("bytes", s.bytesWritten);
                info.put("totalDurationMs", s.totalDurationMs);
                info.put("frontierMs", s.frontierMs());
                res.perFormat.put(s.fmt.itag, info);
                System.out.println("[Sabr] format " + s.fmt.itag + " done: " + s.seen.size() + "/" + s.totalSegments
                        + " segs, " + s.bytesWritten + "B on disk, buffered=" + s.bufferedMs + "ms");
            }
        } finally {
            for (FState s : states.values()) s.close();
        }
        res.stopReason = stopReason;
        System.out.println("[Sabr] session end: complete=" + res.complete + " iters=" + res.iterations
                + " stop=" + stopReason);
        return res;
    }

    // ---- request building ----
    /// Wann die Session begann — fuer elapsed_wall_time_ms (Feld 36).
    private final long sessionStartMs = System.currentTimeMillis();

    /// Transport fuer den ABR-POST (s. post()). Eine Instanz pro JVM; HttpClient
    /// ist thread-sicher und haelt Verbindungen selbst warm.
    private static final java.net.http.HttpClient HTTP = java.net.http.HttpClient.newBuilder()
            .connectTimeout(java.time.Duration.ofSeconds(20))
            .followRedirects(java.net.http.HttpClient.Redirect.NORMAL)
            .build();

    /// Wiedergabe-Nonce dieser Session (s. post()). Pro Session EINMAL erzeugt.
    /// Bewusst lokal statt aus SynthHlsHandlers importiert — die sabr-Schicht
    /// soll nicht auf die Handler-Schicht zeigen. Alphabet identisch (16
    /// URL-sichere Zeichen, wie der Browser sie erzeugt).
    private static final char[] CPN_CHARS =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_".toCharArray();
    private static String freshCpn() {
        final char[] c = new char[16];
        final java.util.concurrent.ThreadLocalRandom r =
                java.util.concurrent.ThreadLocalRandom.current();
        for (int i = 0; i < 16; i++) c[i] = CPN_CHARS[r.nextInt(CPN_CHARS.length)];
        return new String(c);
    }
    private final String cpn = freshCpn();

    /// `client_abr_state` (Feld 1 der ABR-Anfrage).
    ///
    /// Feldnamen aus der Referenz-Implementierung LuanRT/googlevideo
    /// (protos/video_streaming/client_abr_state.proto); die WERTE stammen aus dem
    /// Browser-Mitschnitt des echten Web-Players (2026-07-25).
    ///
    /// Bisher schickten wir ZWEI Felder (~5 B), der Browser ~20 (105 B). Der
    /// Server bekam damit keine Wiedergabe-Absicht: keine Viewport-Groesse, keine
    /// Bandbreitenschaetzung, keinen Sitzungsfortschritt. `YT_SABR_CAS_FULL=1`
    /// schaltet den vollen Zustand ein.
    ///
    /// ⚠️ Feld 40 (`enabled_track_types_bitfield`) schickt der Browser NICHT —
    /// im vollen Modus lassen wir es entsprechend weg. Ebenso Feld 4 der
    /// Aussenanfrage (Spielzeit steckt im client_abr_state).
    /// ⚠️ Nicht rekonstruierbar: Feld 79 `playback_authorization` (18 B, Inhalt
    /// nicht mitgeschnitten) sowie die undokumentierten Felder 71/72/85.
    /// Referenz-Form des client_abr_state: DEFAULT fuer den WEB-Client, aus fuer
    /// die App-Clients. `YT_SABR_CAS_REF=1`/`=0` ueberschreibt beides.
    private boolean useCasRef() {
        final String v = System.getenv("YT_SABR_CAS_REF");
        if (v != null && !v.isBlank()) return "1".equals(v.trim());
        return webClient;
    }

    private byte[] clientAbrState(long playerTimeMs, Map<Integer, FState> states) {
        // REFERENZ-FORM (`YT_SABR_CAS_REF=1`). Direkt aus den Request-Bytes der
        // Referenz-Implementierung abgelesen, die mit UNSEREN Session-Daten
        // nachweislich Medien bekommt (2026-07-30: 1,85 MB Video + 182 KB Audio,
        // wo unsere Form 11 B bekam). Sie schickt im ersten Request GENAU DREI
        // Felder — und `player_time_ms` ist NICHT dabei:
        //   21 sticky_resolution = 1080
        //   34 visibility        = 1        (wir: nie gesetzt)
        //   35 playback_rate     = 1.0f     (wir: nie gesetzt, brauchte fixed32)
        // Unsere bisherige Form (28 + 40) enthaelt dagegen zwei Felder, die die
        // Referenz gar nicht sendet. Ab Runde 2 kommt die Spielzeit dazu.
        // ⚠️ Ausdrueckliche Vollform hat Vorrang — sonst gewinnt fuer WEB immer
        // die Referenz-Form und der Schalter waere wirkungslos (genau so
        // passiert: Log zeigte weiter "form=ref len=18").
        if (!"1".equals(System.getenv("YT_SABR_CAS_FULL")) && useCasRef()) {
            final ProtoWriter w = new ProtoWriter()
                    .varintField(21, 1080)
                    .varintField(34, 1);
            w.floatField(35, 1.0f);
            if (playerTimeMs > 0) w.varintField(28, playerTimeMs);
            return w.toByteArray();
        }
        if (!"1".equals(System.getenv("YT_SABR_CAS_FULL")))
            return new ProtoWriter().varintField(28, playerTimeMs).varintField(40, 7).toByteArray();
        final long wall = Math.max(0, System.currentTimeMillis() - sessionStartMs);
        // Gepufferte Dauer = was wir wirklich schon haben (der Browser meldet hier
        // seinen Pufferstand; eine erfundene Zahl waere genau die Sorte Rauschen,
        // die uns beim Kids-Cap schon einmal fehlgeleitet hat).
        long buffered = 0;
        for (FState s : states.values()) buffered = Math.max(buffered, s.bufferedMs);
        // Werte aus einem FRISCHEN Browser-Mitschnitt (2026-07-31, Chrome-
        // Erweiterung, laufende Wiedergabe desselben Kids-Videos). Der echte
        // Player sendet 18 Felder / 102 B; wir kamen mit 3 Feldern / 13 B.
        // Feldnummern und Groessenordnungen 1:1 uebernommen — nur die
        // zeitabhaengigen Werte setzen wir aus dem echten Sitzungsverlauf.
        // ⚠️ Feld 79 (`playback_authorization`, 18 B) und Feld 72 (14 B) sendet
        // der Browser ebenfalls; ihr INHALT ist sitzungsgebunden und liesse
        // sich nicht sinnvoll kopieren — sie bleiben deshalb weg.
        return new ProtoWriter()
                .varintField(18, 2084)          // client_viewport_width
                .varintField(19, 1172)          // client_viewport_height
                .varintField(21, 0)             // sticky_resolution
                .varintField(23, 3221101)       // bandwidth_estimate (B/s)
                .varintField(28, playerTimeMs)  // player_time_ms
                .varintField(29, buffered)      // time_since_last_seek
                .varintField(34, 0)             // visibility
                .varintField(36, wall)          // elapsed_wall_time_ms
                .varintField(39, wall)          // time_since_last_action_ms
                .varintField(46, 1)             // drc_enabled
                .varintField(57, 162)           // field57
                .varintField(58, 0)             // prefer_vp9
                .varintField(59, 8192)          // av1_quality_threshold
                .varintField(68, 7631)          // sabr_force_max_network_interruption_duration_ms
                .varintField(71, 1)             // field71 (Browser: 1)
                .varintField(85, 1)             // field85 (Browser: 1)
                .toByteArray();
    }

    private byte[] buildRequest(Map<Integer, FState> states, long playerTimeMs, byte[] cookie) {
        final byte[] cas = clientAbrState(playerTimeMs, states);
        // Beim ERSTEN Request den gesendeten client_abr_state zeigen — ohne diesen
        // Beleg waere ein Negativbefund wertlos (man wuesste nicht, ob der volle
        // Zustand ueberhaupt rausging).
        if (requestNo == 0 && TRACE) {
            final StringBuilder h = new StringBuilder();
            for (byte b : cas) h.append(String.format("%02x", b));
            System.out.println("[Sabr] client_abr_state len=" + cas.length
                    + " form=" + (useCasRef() ? "ref"
                    : "1".equals(System.getenv("YT_SABR_CAS_FULL")) ? "full" : "legacy")
                    + " hex=" + h);
        }
        final ProtoWriter req = new ProtoWriter();
        req.bytesField(1, cas);
        // selected_format_ids: ONLY formats already initialized (FORMAT_INIT
        // received). On the first request none are -> empty -> the server sends
        // the init segments + FORMAT_INIT. Sending selected too early puts the
        // server in continuation mode (no init). Matches LuanRT/googlevideo
        // prepareFormatSelections.
        for (FState s : states.values()) {
            if (s.totalSegments > 0) req.bytesField(2, formatId(s.fmt));
        }
        for (FState s : states.values()) {
            for (byte[] br : bufferedRanges(s)) req.bytesField(3, br);
        }
        // ⚠️ NUR den bevorzugten Pick nennen — NICHT alle Kandidaten wie der echte
        // Web-Player (2026-07-25 versucht und wieder entfernt): der Server waehlte
        // daraufhin ANDERE Formate (249/396 statt 140/137), waehrend unsere
        // Buchhaltung (itags-Manifest, ensureFile, Playlist-Layer) weiter vom
        // Wunschpaar ausging -> Cache-Dateien _249/_396, angefragt wurde _137
        // -> Dauer-500 auf /synth-hls. Fuer das WEB-Experiment brachte die
        // Kandidatenliste ohnehin nichts (unveraendert 11B-Antworten).

        // Feld 4 schickt der echte Web-Player NICHT (die Spielzeit steht im
        // client_abr_state, Feld 28). Im vollen Modus lassen wir es weg, damit
        // die Anfrage der mitgeschnittenen Form entspricht.
        if (!"1".equals(System.getenv("YT_SABR_CAS_FULL")) && !useCasRef())
            req.varintField(4, playerTimeMs);
        // ⚠️ Feld-Reihenfolge AUFSTEIGEND (1,2,3,5,16,17,19) — so serialisiert die
        // Referenz-Implementierung. Wir schickten 16/17 VOR 5. Semantisch ist die
        // Reihenfolge in protobuf egal, auf dem Draht aber nicht: nach dem
        // Runde-2-Diff (2026-07-30) war das bei identischer Gesamtlaenge (2004 B)
        // der EINZIGE verbleibende Unterschied zur Referenz.
        req.bytesField(5, ustreamerConfig);
        req.bytesField(16, formatId(prefAudio));
        req.bytesField(17, formatId(prefVideo));
        // streamerContext: client_info(1), po_token(2), playback_cookie(3).
        // po_token authorizes the gvs streaming session — without it googlevideo
        // caps the readahead at ~one buffer window (~60s) then stops sending.
        final ProtoWriter sc = new ProtoWriter().bytesField(1, clientInfo);
        if (poToken != null) sc.bytesField(2, poToken);
        // ⚠️ NUR bei INHALT senden. Ein LEERES playback_cookie (Feld 3, 0 Byte)
        // ist nicht dasselbe wie „kein Cookie": der Server bekommt damit einen
        // ungueltigen Wiedergabe-Zeiger. Runde-2-Diff gegen die Referenz
        // (2026-07-30) zeigte genau diesen einen Unterschied — alles andere,
        // inkl. sabr_contexts (86B), war byte-identisch.
        if (cookie != null && cookie.length > 0) sc.bytesField(3, cookie);
        // repeated sabr_contexts = 5, je { 1=type, 2=value } — zurückgespiegelt
        // aus SABR_CONTEXT_UPDATE (s. handleContextUpdate).
        for (var e : sabrContexts.entrySet()) {
            sc.bytesField(5, new ProtoWriter()
                    .varintField(1, e.getKey())
                    .bytesField(2, e.getValue())
                    .toByteArray());
        }
        // `unsent_sabr_contexts` (Feld 6) schickt die Referenz immer mit — leer,
        // wenn alle bekannten Kontexte aktiv sind. Ohne das Feld fehlt dem Server
        // die Aussage „ich kenne keine weiteren".
        sc.bytesField(6, new byte[0]);
        req.bytesField(19, sc.toByteArray());
        final byte[] out = req.toByteArray();
        // VOLLER Request-Koerper beim ersten Request — Vergleichsgrundlage gegen
        // die Referenz-Implementierung, die mit denselben Eingaben Medien
        // bekommt. Ohne diesen Diff bleibt jede Aenderung Raterei.
        // Die ERSTEN ZWEI Runden dumpen. Runde 0 ist auf beiden Seiten unauffaellig
        // (belegt 2026-07-30: die Bytes der Referenz geben per curl ebenfalls nur
        // 104B) — die Referenz gewinnt erst in Runde 2. Genau die braucht der Diff.
        // ⚠️ EIGENER Schalter, nicht YT_SABR_TRACE: seit der WEB-Pfad Default ist,
        // liefe sonst pro Session zweimal ein ~2 KB-Hexdump ins Produktionslog.
        if (requestNo <= 1 && "1".equals(System.getenv("YT_SABR_REQHEX"))) {
            final StringBuilder fh = new StringBuilder();
            for (byte b : out) fh.append(String.format("%02x", b));
            System.out.println("[Sabr] REQ-HEX#" + requestNo + " len=" + out.length + " " + fh);
        }
        return out;
    }

    private static byte[] formatId(Fmt f) {
        return new ProtoWriter().varintField(1, f.itag).varintField(2, f.lmt).toByteArray();
    }

    /// EHRLICHE buffered_ranges: ein Range pro ZUSAMMENHAENGENDEM Lauf empfangener
    /// Segmente, mit echten Zeitfeldern aus den MediaHeadern. Vorher meldeten wir
    /// pauschal `start=1..end=maxSeq` — bei einem Loch in der Segmentfolge
    /// BEHAUPTETEN wir damit, das fehlende Segment zu haben, der Server lieferte
    /// es nie nach, der writeCursor blockierte und die Session endete stuck
    /// (2026-07-30, Kids-Session: disk fror bei Runde 8 ein, seen wuchs bis 432,
    /// ab Runde ~172 schickte der Server in Schleife dieselben 1,78 MB).
    /// Mehrere Ranges pro Format sind legitim: der echte Web-Player schickte im
    /// Mitschnitt 2026-07-25 DREI buffered_ranges.
    private java.util.List<byte[]> bufferedRanges(FState s) {
        final java.util.List<byte[]> out = new java.util.ArrayList<>();
        int runStart = -1, prev = -2;
        long runStartMs = 0, runDur = 0;
        for (var e : s.segTimes.entrySet()) {
            final int seq = e.getKey();
            if (runStart < 0) {
                runStart = seq; runStartMs = s.estStartMs(seq, e.getValue()[0]);
            } else if (seq != prev + 1) {
                out.add(rangeBytes(s, runStart, prev, runStartMs, runDur));
                runStart = seq; runStartMs = s.estStartMs(seq, e.getValue()[0]); runDur = 0;
            }
            runDur += s.estDurMs(e.getValue()[1]);
            prev = seq;
        }
        if (runStart >= 0) out.add(rangeBytes(s, runStart, prev, runStartMs, runDur));
        return out;
    }

    private byte[] rangeBytes(FState s, int startSeq, int endSeq, long startMs, long durMs) {
        return new ProtoWriter()
                .bytesField(1, formatId(s.fmt))   // format_id
                .varintField(2, startMs)          // start_time_ms
                .varintField(3, durMs)            // duration_ms
                .varintField(4, startSeq)         // start_segment_index
                .varintField(5, endSeq)           // end_segment_index
                .toByteArray();
    }

    // ---- response handling ----
    private void handleFormatInit(byte[] p, Map<Integer, FState> states) {
        final ProtoReader r = new ProtoReader(p);
        int itag = -1;
        long lmt = 0;
        long endSeg = -1;
        long durUnits = 0;
        long durScale = 0;
        while (r.hasMore()) {
            final int f = r.readTag();
            if (f == 2 && r.wireType() == 2) {
                final long[] il = innerFormat(r.readBytes());
                itag = (int) il[0];
                lmt = il[1];
            } else if (f == 4 && r.wireType() == 0) {
                endSeg = r.readVarint();
            } else if (f == 9 && r.wireType() == 0) {
                durUnits = r.readVarint();
            } else if (f == 10 && r.wireType() == 0) {
                durScale = r.readVarint();
            } else {
                r.skip();
            }
        }
        if (itag >= 0 && endSeg > 0) {
            final int fi = itag;
            final long fl = lmt;
            final FState st = states.computeIfAbsent(fi, k -> new FState(new Fmt(fi, fl)));
            st.totalSegments = (int) endSeg;
            if (durScale > 0) st.totalDurationMs = durUnits * 1000L / durScale;
        }
    }

    private static long[] innerFormat(byte[] formatIdBytes) {
        final ProtoReader r = new ProtoReader(formatIdBytes);
        long itag = -1, lmt = 0;
        while (r.hasMore()) {
            final int f = r.readTag();
            if (f == 1 && r.wireType() == 0) itag = r.readVarint();
            else if (f == 2 && r.wireType() == 0) lmt = r.readVarint();
            else r.skip();
        }
        return new long[]{itag, lmt};
    }

    private void handleHeader(byte[] p, Map<Long, Pending> pend) {
        final ProtoReader r = new ProtoReader(p);
        long headerId = -1;
        final Pending pg = new Pending();
        while (r.hasMore()) {
            final int f = r.readTag();
            if (r.wireType() != 0) { r.skip(); continue; }
            final long v = r.readVarint();
            switch (f) {
                case 1: headerId = v; break;
                case 3: pg.itag = (int) v; break;
                case 4: pg.lmt = v; break;
                case 8: pg.isInit = v != 0; break;
                case 9: pg.seq = (int) v; break;
                case 11: pg.startMs = v; break;
                case 12: pg.durationMs = v; break;
                default: break;
            }
        }
        if (headerId >= 0) pend.put(headerId, pg);
    }

    private void handleMedia(byte[] payload, Map<Long, Pending> pend) {
        final long[] r = UmpReader.varint(payload, 0);
        if (r == null) return;
        final Pending pg = pend.get(r[0]);
        if (pg != null) pg.data.write(payload, (int) r[1], payload.length - (int) r[1]);
    }

    private void finalizeOne(byte[] mediaEndPayload, Map<Long, Pending> pend,
                             Map<Integer, FState> states, int[] newSegments) {
        final long[] r = UmpReader.varint(mediaEndPayload, 0);
        if (r == null) return;
        final Pending pg = pend.remove(r[0]);
        if (pg != null) store(pg, states, newSegments);
    }

    /// Record a finalized segment in RAM (dedup + reorder buffer). No disk IO here
    /// — this runs inside the UmpReader.parse callback; drainToDisk() flushes after.
    private void store(Pending pg, Map<Integer, FState> states, int[] newSegments) {
        if (pg.itag < 0) return;
        final FState s = states.computeIfAbsent(pg.itag, k -> new FState(new Fmt(pg.itag, pg.lmt)));
        if (pg.isInit) {
            if (!s.initWritten && s.pendingInit == null) { s.pendingInit = pg.data.toByteArray(); newSegments[0]++; }
        } else if (s.seen.add(pg.seq)) {
            if (pg.seq > s.maxSeq) s.maxSeq = pg.seq;
            s.bufferedMs += pg.durationMs;
            s.segTimes.put(pg.seq, new long[]{pg.startMs, pg.durationMs});
            s.buf.put(pg.seq, pg.data.toByteArray());
            newSegments[0]++;
        }
    }

    private String extractRedirect(byte[] p) {
        final ProtoReader r = new ProtoReader(p);
        while (r.hasMore()) {
            if (r.readTag() == 1 && r.wireType() == 2) return new String(r.readBytes());
            r.skip();
        }
        return null;
    }

    private byte[] post(byte[] body) throws Exception {
        // Through reqwest4j so the socket binds to an explicit egress family —
        // gvs URLs are IP-signed, and the family-retry in SabrCache needs the
        // resolve + every ABR request of one attempt on the SAME family.
        final String family = egressFamily != null
                ? egressFamily
                : me.kavin.piped.utils.EgressManager.activeEgress();
        // Probe (YT_SABR_POT_IN_URL=1): den Attestierungs-Token ZUSAETZLICH als
        // `pot=`-Query anhaengen — genau so autorisiert unser funktionierender
        // Direktpfad die googlevideo-Range-GETs. Beim ABR-POST steckt er bisher
        // nur im streamerContext.
        // `rn` (Request-Nummer, pro Anfrage hochgezaehlt) und `alr=yes` schickt der
        // echte Web-Player mit — im Mitschnitt 2026-07-25 belegt.
        // `cpn` (client playback nonce, 16 URL-sichere Zeichen) identifiziert die
        // WIEDERGABE. Der Browser schickt ihn an jeder Medien-Anfrage mit, unser
        // Direktpfad auch (SynthHlsHandlers.swapCpn) — nur der ABR-POST bisher
        // nicht. Im Mitschnitt 2026-07-25 steht er in der Browser-Parameterliste
        // (`alr,c,cpn,cps,cver,…`). Ohne ihn nimmt googlevideo den POST an und
        // schickt trotzdem nichts (unser WEB-Bild: 105B -> 11B, kein
        // FORMAT_INIT, kein Fehler). EINMAL pro Session erzeugt und konstant
        // gehalten: ein wechselnder cpn waere eine neue Wiedergabe pro Runde,
        // und ein wiederverwendeter cpn hat 2026-05-29 die Drossel ausgeloest.
        // NUR `rn` anhaengen — so macht es die Referenz-Implementierung, die mit
        // denselben Session-Daten Medien bekommt (Parameter-Mitschnitt 2026-07-30:
        // "… c n sparams sig lsparams lsig rn", sonst nichts).
        //
        // ⚠️ `alr=yes` haben wir am 2026-07-25 aus einem Browser-Mitschnitt
        // uebernommen — es steht dort aber an den MEDIEN-Requests, nicht am
        // ABR-POST. `alr` = "adaptive live redirect": der Server antwortet dann
        // mit einer Weiterleitung statt mit Medien. Genau unser Fehlerbild
        // (Antwort kommt an, enthaelt aber nichts). `cpn` gehoert ebenfalls an
        // die Medien-URLs, nicht hierher.
        String url = abrUrl + "&rn=" + (++requestNo);
        if (POT_IN_URL && poToken != null && !url.contains("&pot=")) {
            url = url + "&pot=" + java.util.Base64.getUrlEncoder().withoutPadding()
                    .encodeToString(poToken);
        }
        // Browser-Identitaet: googlevideo weist den ABR-POST ohne Origin/Referer
        // mit 403 ab (gemessen 2026-07-25, WEB-Client-Session). Fuer die
        // App-Clients (Android/VR) bleibt der Header-Satz unveraendert.
        // ⚠️ `Accept: application/vnd.yt-ump` — DEN Header schickt die Referenz-
        // Implementierung an jedem ABR-POST, wir bisher nicht. Ohne ihn nimmt
        // googlevideo die Anfrage an und antwortet mit einem leeren UMP-Rahmen
        // (unser Bild: 105B -> 11B, kein FORMAT_INIT, kein Fehler).
        final java.util.Map<String, String> headers = new java.util.HashMap<>(Map.of(
                "Content-Type", "application/x-protobuf",
                "Accept-Encoding", "identity",
                "Accept", "application/vnd.yt-ump",
                "User-Agent", userAgent));
        // ⚠️ KEIN Origin/Referer — das ist die Voraussetzung dafuer, dass der
        // WEB-Pfad ueberhaupt laeuft. Sie kamen 2026-07-25 dazu („ohne sie 403"),
        // aber das war eine Messung ohne den damals fehlenden
        // `Accept: application/vnd.yt-ump`. Umgekehrt belegt: MIT Origin/Referer
        // antwortet googlevideo dem WEB-Client auf BEIDEN Egress-Familien mit
        // 403 (zuletzt reproduziert 2026-07-30 beim Umstellen auf WEB-Default),
        // ohne sie liefert er Medien — jeder erfolgreiche WEB-Lauf des Tages
        // lief ohne. Die Referenz-Implementierung schickt sie ebenfalls nicht.
        // `YT_SABR_ORIGIN=1` stellt sie fuer Vergleichsmessungen wieder her.
        if (userAgent != null && userAgent.startsWith("Mozilla/")
                && "1".equals(System.getenv("YT_SABR_ORIGIN"))) {
            headers.put("Origin", "https://www.youtube.com");
            headers.put("Referer", "https://www.youtube.com/");
        }
        // TRANSPORT (2026-07-30): ueber reqwest4j bekamen wir auf einen byte-gleichen
        // Request nur einen leeren UMP-Rahmen (11B), waehrend curl mit EXAKT
        // denselben Bytes, denselben Headern, auf BEIDEN Egress-Familien und in
        // beiden Verbindungsmodi 6825272B Medien bekam. Der Request war also nie
        // das Problem — der Client war es. Java-HttpClient verhaelt sich wie curl.
        // Die Egress-Bindung entfaellt dabei bewusst: beide Familien liefern.
        // Schalter YT_SABR_REQWEST=1 stellt den alten Weg wieder her.
        if ("1".equals(System.getenv("YT_SABR_REQWEST"))) {
            final var resp = rocks.kavin.reqwest4j.ReqwestUtils.fetchWithProxy(
                    url, "POST", body, headers,
                    family).get(60, java.util.concurrent.TimeUnit.SECONDS);
            if (resp.status() / 100 != 2)
                throw new IOException("SABR POST HTTP " + resp.status() + " (egress=" + family + ")");
            return resp.body();
        }
        final var reqB = java.net.http.HttpRequest.newBuilder()
                .uri(java.net.URI.create(url))
                .timeout(java.time.Duration.ofSeconds(60))
                .POST(java.net.http.HttpRequest.BodyPublishers.ofByteArray(body));
        for (var h : headers.entrySet()) reqB.header(h.getKey(), h.getValue());
        final java.net.http.HttpResponse<byte[]> r2;
        try {
            r2 = HTTP.send(reqB.build(), java.net.http.HttpResponse.BodyHandlers.ofByteArray());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("SABR POST unterbrochen", ie);
        }
        if (r2.statusCode() / 100 != 2)
            throw new IOException("SABR POST HTTP " + r2.statusCode() + " (egress=" + family + ")");
        return r2.body();
    }
}
