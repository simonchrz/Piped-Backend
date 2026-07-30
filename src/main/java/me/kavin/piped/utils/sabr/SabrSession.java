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
    @FunctionalInterface
    public interface Sink {
        OutputStream open(int itag) throws IOException;
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
        OutputStream out;                 // opened lazily via sink
        byte[] pendingInit;               // init bytes, held until drainToDisk writes them
        boolean initWritten;
        final TreeMap<Integer, byte[]> buf = new TreeMap<>();  // received, not yet flushed
        final Set<Integer> seen = new HashSet<>();             // dedup + segment count
        int writeCursor = Integer.MIN_VALUE;                   // next seq to flush
        int maxSeq = 0;
        long bytesWritten = 0;
        int totalSegments = -1;
        long totalDurationMs = 0;
        long bufferedMs = 0;
        FState(Fmt f) { fmt = f; }
        long perSegMs() { return totalSegments > 0 ? totalDurationMs / totalSegments : 0; }
        long frontierMs() { return perSegMs() * maxSeq; }
        boolean complete() { return totalSegments > 0 && seen.size() >= totalSegments && initWritten; }

        /// Flush init (once) + any now-contiguous buffered segments to disk. Called
        /// after every round; for in-order delivery buf drains to empty each time
        /// so RAM stays bounded to a single round's worth of media.
        void drainToDisk() throws IOException {
            if (pendingInit != null && !initWritten) {
                if (out == null) out = sink.open(fmt.itag);
                out.write(pendingInit);
                bytesWritten += pendingInit.length;
                pendingInit = null;
                initWritten = true;
            }
            if (!initWritten) return;                 // can't write data before init
            if (writeCursor == Integer.MIN_VALUE) {
                if (buf.isEmpty()) return;
                writeCursor = buf.firstKey();          // true min seq (TreeMap)
            }
            while (buf.containsKey(writeCursor)) {
                final byte[] d = buf.remove(writeCursor);
                out.write(d);
                bytesWritten += d.length;
                writeCursor++;
            }
        }

        void flush() {
            try { if (out != null) out.flush(); } catch (IOException ignored) {}
        }

        void close() {
            try { if (out != null) out.close(); } catch (IOException ignored) {}
        }
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
        String stopReason = "maxIterations";
        System.out.println("[Sabr] session start maxIter=" + maxIterations
                + (paced ? " PACED" : "")
                + " poToken=" + (poToken != null ? poToken.length + "B" : "NONE"));

        try {
            for (int iter = 0; iter < maxIterations; iter++) {
                res.iterations = iter + 1;
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
                        case UmpReader.NEXT_REQUEST_POLICY: cookie[0] = extractCookie(payload); break;
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
                    for (FState s : states.values())
                        sb.append(' ').append(s.fmt.itag).append(":seg").append(s.seen.size())
                          .append('/').append(s.totalSegments).append(",disk").append(s.bytesWritten >> 20).append("MB");
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
                // advance the playback head to the buffered frontier (min across
                // formats, so audio+video march together) to pull the next window.
                final long frontier = states.values().stream().mapToLong(FState::frontierMs).min().orElse(playerTimeMs);
                if (!paced) {
                    playerTimeMs = frontier;
                } else {
                    // never claim playback ahead of wall-clock (+8s head start)
                    playerTimeMs = Math.min(frontier, (System.currentTimeMillis() - wallStart) + 8_000);
                    for (FState s : states.values()) s.flush();
                    if (publishHook != null) publishHook.run();
                    Thread.sleep(newSegments[0] > 0 ? 1_000 : 5_000);
                }
            }

            res.complete = !states.isEmpty() && states.values().stream().allMatch(FState::complete);
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
        if ("1".equals(System.getenv("YT_SABR_CAS_REF"))) {
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
        return new ProtoWriter()
                .varintField(18, 2072)          // client_viewport_width
                .varintField(19, 1166)          // client_viewport_height
                .varintField(21, 0)             // sticky_resolution
                .varintField(23, 13335011)      // bandwidth_estimate (B/s)
                .varintField(28, playerTimeMs)  // player_time_ms
                .varintField(29, buffered)      // time_since_last_seek
                .varintField(34, 0)             // visibility
                .varintField(36, wall)          // elapsed_wall_time_ms
                .varintField(39, wall)          // time_since_last_action_ms
                .varintField(46, 1)             // drc_enabled
                .varintField(57, 78)            // field57
                .varintField(58, 0)             // prefer_vp9
                .varintField(59, 8192)          // av1_quality_threshold
                .varintField(68, 2383)          // sabr_force_max_network_interruption_duration_ms
                .varintField(76, 0)             // enable_voice_boost
                .toByteArray();
    }

    private byte[] buildRequest(Map<Integer, FState> states, long playerTimeMs, byte[] cookie) {
        final byte[] cas = clientAbrState(playerTimeMs, states);
        // Beim ERSTEN Request den gesendeten client_abr_state zeigen — ohne diesen
        // Beleg waere ein Negativbefund wertlos (man wuesste nicht, ob der volle
        // Zustand ueberhaupt rausging).
        if (requestNo == 0 && "1".equals(System.getenv("YT_SABR_TRACE"))) {
            final StringBuilder h = new StringBuilder();
            for (byte b : cas) h.append(String.format("%02x", b));
            System.out.println("[Sabr] client_abr_state len=" + cas.length
                    + " full=" + "1".equals(System.getenv("YT_SABR_CAS_FULL")) + " hex=" + h);
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
            if (!s.seen.isEmpty()) req.bytesField(3, bufferedRange(s));
        }
        // ⚠️ NUR den bevorzugten Pick nennen — NICHT alle Kandidaten wie der echte
        // Web-Player (2026-07-25 versucht und wieder entfernt): der Server waehlte
        // daraufhin ANDERE Formate (249/396 statt 140/137), waehrend unsere
        // Buchhaltung (itags-Manifest, ensureFile, Playlist-Layer) weiter vom
        // Wunschpaar ausging -> Cache-Dateien _249/_396, angefragt wurde _137
        // -> Dauer-500 auf /synth-hls. Fuer das WEB-Experiment brachte die
        // Kandidatenliste ohnehin nichts (unveraendert 11B-Antworten).
        req.bytesField(16, formatId(prefAudio));
        req.bytesField(17, formatId(prefVideo));
        // Feld 4 schickt der echte Web-Player NICHT (die Spielzeit steht im
        // client_abr_state, Feld 28). Im vollen Modus lassen wir es weg, damit
        // die Anfrage der mitgeschnittenen Form entspricht.
        if (!"1".equals(System.getenv("YT_SABR_CAS_FULL"))
                && !"1".equals(System.getenv("YT_SABR_CAS_REF"))) req.varintField(4, playerTimeMs);
        req.bytesField(5, ustreamerConfig);
        // streamerContext: client_info(1), po_token(2), playback_cookie(3).
        // po_token authorizes the gvs streaming session — without it googlevideo
        // caps the readahead at ~one buffer window (~60s) then stops sending.
        final ProtoWriter sc = new ProtoWriter().bytesField(1, clientInfo);
        if (poToken != null) sc.bytesField(2, poToken);
        if (cookie != null) sc.bytesField(3, cookie);
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
        if (requestNo == 0 && "1".equals(System.getenv("YT_SABR_TRACE"))) {
            final StringBuilder fh = new StringBuilder();
            for (byte b : out) fh.append(String.format("%02x", b));
            System.out.println("[Sabr] REQ-HEX len=" + out.length + " " + fh);
        }
        return out;
    }

    private static byte[] formatId(Fmt f) {
        return new ProtoWriter().varintField(1, f.itag).varintField(2, f.lmt).toByteArray();
    }

    private byte[] bufferedRange(FState s) {
        return new ProtoWriter()
                .bytesField(1, formatId(s.fmt))   // format_id
                .varintField(2, 0)                // start_time_ms
                .varintField(3, s.bufferedMs)     // duration_ms
                .varintField(4, 1)                // start_segment_index (1-based)
                .varintField(5, s.maxSeq)         // end_segment_index
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

    private byte[] extractCookie(byte[] p) {
        final ProtoReader r = new ProtoReader(p);
        while (r.hasMore()) {
            if (r.readTag() == 7 && r.wireType() == 2) return r.readBytes();
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
        // ⚠️ Origin/Referer kamen 2026-07-25 dazu („ohne sie 403"). Mit dem seither
        // fehlenden `Accept: application/vnd.yt-ump` war das aber eine Messung
        // unter falscher Voraussetzung. Die Referenz-Implementierung, die Medien
        // bekommt, schickt WEDER Origin NOCH Referer NOCH einen Browser-UA —
        // nur content-type, accept-encoding und accept. `YT_SABR_NO_ORIGIN=1`
        // testet das (eine Variable).
        if (userAgent != null && userAgent.startsWith("Mozilla/")
                && !"1".equals(System.getenv("YT_SABR_NO_ORIGIN"))) {
            headers.put("Origin", "https://www.youtube.com");
            headers.put("Referer", "https://www.youtube.com/");
        }
        final var resp = rocks.kavin.reqwest4j.ReqwestUtils.fetchWithProxy(
                url, "POST", body, headers,
                family).get(60, java.util.concurrent.TimeUnit.SECONDS);
        if (resp.status() / 100 != 2)
            throw new IOException("SABR POST HTTP " + resp.status() + " (egress=" + family + ")");
        return resp.body();
    }
}
