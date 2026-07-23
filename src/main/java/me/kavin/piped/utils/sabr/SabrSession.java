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
    private final byte[] ustreamerConfig;
    private final byte[] clientInfo;
    private final String userAgent;
    private final Fmt prefAudio;
    private final Fmt prefVideo;
    private final byte[] poToken;   // decoded gvs po_token bytes, or null
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
        this.sink = sink;
        final Map<Integer, FState> states = new LinkedHashMap<>();
        byte[] playbackCookie = null;
        long playerTimeMs = 0;
        int stuckRounds = 0;
        final Result res = new Result();
        String stopReason = "maxIterations";
        System.out.println("[Sabr] session start maxIter=" + maxIterations
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

                UmpReader.parse(resp, (type, payload) -> {
                    switch (type) {
                        case UmpReader.FORMAT_INIT_METADATA: handleFormatInit(payload, states); break;
                        case UmpReader.MEDIA_HEADER: handleHeader(payload, pend); break;
                        case UmpReader.MEDIA: handleMedia(payload, pend); break;
                        case UmpReader.MEDIA_END: finalizeOne(payload, pend, states, newSegments); break;
                        case UmpReader.NEXT_REQUEST_POLICY: cookie[0] = extractCookie(payload); break;
                        case UmpReader.SABR_REDIRECT: { String u = extractRedirect(payload); if (u != null) { abrUrl = u; redirected[0] = true; } break; }
                        case UmpReader.SABR_ERROR: sabrError[0] = true; break;
                        default: break;
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
                            + (redirected[0] ? " REDIRECT" : "") + sb);
                }

                playbackCookie = cookie[0];
                if (sabrError[0]) { stopReason = "SABR_ERROR"; break; }
                if (states.values().stream().allMatch(FState::complete)) { stopReason = "complete"; break; }
                if (newSegments[0] == 0) {
                    if (++stuckRounds > 3) { stopReason = "stuck(no new segments 4 rounds)"; break; }
                } else {
                    stuckRounds = 0;
                }
                // advance the playback head to the buffered frontier (min across
                // formats, so audio+video march together) to pull the next window.
                playerTimeMs = states.values().stream().mapToLong(FState::frontierMs).min().orElse(playerTimeMs);
            }

            res.complete = states.values().stream().allMatch(FState::complete);
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
    private byte[] buildRequest(Map<Integer, FState> states, long playerTimeMs, byte[] cookie) {
        final byte[] cas = new ProtoWriter().varintField(28, playerTimeMs).varintField(40, 7).toByteArray();
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
        req.bytesField(16, formatId(prefAudio)); // preferred_audio_format_ids -> force exact codecs
        req.bytesField(17, formatId(prefVideo)); // preferred_video_format_ids
        req.varintField(4, playerTimeMs);
        req.bytesField(5, ustreamerConfig);
        // streamerContext: client_info(1), po_token(2), playback_cookie(3).
        // po_token authorizes the gvs streaming session — without it googlevideo
        // caps the readahead at ~one buffer window (~60s) then stops sending.
        final ProtoWriter sc = new ProtoWriter().bytesField(1, clientInfo);
        if (poToken != null) sc.bytesField(2, poToken);
        if (cookie != null) sc.bytesField(3, cookie);
        req.bytesField(19, sc.toByteArray());
        return req.toByteArray();
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
        final var resp = rocks.kavin.reqwest4j.ReqwestUtils.fetchWithProxy(
                abrUrl, "POST", body,
                Map.of("Content-Type", "application/x-protobuf",
                        "Accept-Encoding", "identity",
                        "User-Agent", userAgent),
                family).get(60, java.util.concurrent.TimeUnit.SECONDS);
        if (resp.status() / 100 != 2)
            throw new IOException("SABR POST HTTP " + resp.status() + " (egress=" + family + ")");
        return resp.body();
    }
}
