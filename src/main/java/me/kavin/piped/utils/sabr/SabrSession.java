package me.kavin.piped.utils.sabr;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/// SABR streaming session (Baustein 3). Drives the VideoPlaybackAbrRequest loop:
/// each request reports buffered_ranges + player_time, the server streams the
/// next UMP media segments; we reassemble per format (init + segments by seq)
/// into a complete fragmented-mp4 byte[]. See SABR-SPEC.md.
public final class SabrSession {

    public static final class Fmt {
        public final int itag;
        public final long lmt;
        public Fmt(int itag, long lmt) { this.itag = itag; this.lmt = lmt; }
    }

    public static final class Result {
        public final Map<Integer, byte[]> media = new LinkedHashMap<>();
        public int iterations;
        public boolean complete;
        public final Map<Integer, Map<String, Object>> perFormat = new LinkedHashMap<>();
    }

    private static final class FState {
        final Fmt fmt;
        byte[] init;
        final TreeMap<Integer, byte[]> segs = new TreeMap<>();
        int totalSegments = -1;
        long totalDurationMs = 0;
        long bufferedMs = 0;
        FState(Fmt f) { fmt = f; }
        long perSegMs() { return totalSegments > 0 ? totalDurationMs / totalSegments : 0; }
        long frontierMs() { return perSegMs() * maxSeq(); }
        int maxSeq() { return segs.isEmpty() ? 0 : segs.lastKey(); }
        boolean complete() { return totalSegments > 0 && segs.size() >= totalSegments && init != null; }
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

    public SabrSession(String abrUrl, byte[] ustreamerConfig, Fmt prefAudio, Fmt prefVideo,
                       byte[] clientInfo, String userAgent) {
        this.abrUrl = abrUrl;
        this.ustreamerConfig = ustreamerConfig;
        this.prefAudio = prefAudio;
        this.prefVideo = prefVideo;
        this.clientInfo = clientInfo;
        this.userAgent = userAgent;
    }

    public Result fetchAll(int maxIterations) throws Exception {
        final Map<Integer, FState> states = new LinkedHashMap<>();
        byte[] playbackCookie = null;
        long playerTimeMs = 0;
        int stuckRounds = 0;
        final Result res = new Result();

        for (int iter = 0; iter < maxIterations; iter++) {
            res.iterations = iter + 1;
            final byte[] resp = post(buildRequest(states, playerTimeMs, playbackCookie));

            final Map<Long, Pending> pend = new HashMap<>();
            final int[] newSegments = {0};
            final byte[][] cookie = {playbackCookie};
            final boolean[] sabrError = {false};

            UmpReader.parse(resp, (type, payload) -> {
                switch (type) {
                    case UmpReader.FORMAT_INIT_METADATA: handleFormatInit(payload, states); break;
                    case UmpReader.MEDIA_HEADER: handleHeader(payload, pend); break;
                    case UmpReader.MEDIA: handleMedia(payload, pend); break;
                    case UmpReader.MEDIA_END: finalizeOne(payload, pend, states, newSegments); break;
                    case UmpReader.NEXT_REQUEST_POLICY: cookie[0] = extractCookie(payload); break;
                    case UmpReader.SABR_REDIRECT: { String u = extractRedirect(payload); if (u != null) abrUrl = u; break; }
                    case UmpReader.SABR_ERROR: sabrError[0] = true; break;
                    default: break;
                }
            });
            // sweep any segment whose MEDIA_END we didn't see
            for (Map.Entry<Long, Pending> e : pend.entrySet()) store(e.getValue(), states, newSegments);

            playbackCookie = cookie[0];
            if (sabrError[0]) break;
            if (states.values().stream().allMatch(FState::complete)) break;
            if (newSegments[0] == 0) {
                if (++stuckRounds > 3) break; // genuinely stuck
            } else {
                stuckRounds = 0;
            }
            // advance the playback head to the buffered frontier (min across
            // formats, so audio+video march together) to pull the next window.
            playerTimeMs = states.values().stream().mapToLong(FState::frontierMs).min().orElse(playerTimeMs);
        }

        res.complete = states.values().stream().allMatch(FState::complete);
        for (FState s : states.values()) {
            res.media.put(s.fmt.itag, reassemble(s));
            final Map<String, Object> info = new LinkedHashMap<>();
            info.put("segments", s.segs.size());
            info.put("totalSegments", s.totalSegments);
            info.put("hasInit", s.init != null);
            info.put("bytes", reassemble(s).length);
            info.put("totalDurationMs", s.totalDurationMs);
            info.put("frontierMs", s.frontierMs());
            res.perFormat.put(s.fmt.itag, info);
        }
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
            if (!s.segs.isEmpty()) req.bytesField(3, bufferedRange(s));
        }
        req.bytesField(16, formatId(prefAudio)); // preferred_audio_format_ids -> force exact codecs
        req.bytesField(17, formatId(prefVideo)); // preferred_video_format_ids
        req.varintField(4, playerTimeMs);
        req.bytesField(5, ustreamerConfig);
        final ProtoWriter sc = new ProtoWriter().bytesField(1, clientInfo);
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
                .varintField(5, s.maxSeq())       // end_segment_index
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

    private void store(Pending pg, Map<Integer, FState> states, int[] newSegments) {
        if (pg.itag < 0) return;
        final FState s = states.computeIfAbsent(pg.itag, k -> new FState(new Fmt(pg.itag, pg.lmt)));
        if (pg.isInit) {
            if (s.init == null) { s.init = pg.data.toByteArray(); newSegments[0]++; }
        } else if (!s.segs.containsKey(pg.seq)) {
            s.segs.put(pg.seq, pg.data.toByteArray());
            s.bufferedMs += pg.durationMs;
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

    private byte[] reassemble(FState s) {
        final ByteArrayOutputStream o = new ByteArrayOutputStream();
        if (s.init != null) o.writeBytes(s.init);
        for (byte[] seg : s.segs.values()) o.writeBytes(seg);
        return o.toByteArray();
    }

    private byte[] post(byte[] body) throws Exception {
        final HttpURLConnection c = (HttpURLConnection) new URL(abrUrl).openConnection();
        try {
            c.setRequestMethod("POST");
            c.setDoOutput(true);
            c.setConnectTimeout(10_000);
            c.setReadTimeout(30_000);
            c.setRequestProperty("Content-Type", "application/x-protobuf");
            c.setRequestProperty("Accept-Encoding", "identity");
            c.setRequestProperty("User-Agent", userAgent);
            try (OutputStream os = c.getOutputStream()) { os.write(body); }
            try (InputStream is = c.getInputStream()) { return is.readAllBytes(); }
        } finally {
            c.disconnect();
        }
    }
}
