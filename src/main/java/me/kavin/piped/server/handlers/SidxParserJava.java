package me.kavin.piped.server.handlers;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/// Minimal ISO 14496-12 sidx box parser. Java port of the Swift SidxParser.
/// Used by SynthHlsHandlers to split single-byte-range segments into multiple
/// keyframe-aligned fragments — YouTube CDN blocks Range requests > ~10-50MB,
/// so a single 130MB segment fails. Each fragment from sidx is typically 1-2MB.
public class SidxParserJava {

    public static class Entry {
        public final double duration; // seconds
        public final int byteSize;
        Entry(double d, int b) { duration = d; byteSize = b; }
    }

    public static class Data {
        public final int timescale;
        public final int firstOffset;
        public final List<Entry> entries;
        Data(int ts, int fo, List<Entry> e) { timescale = ts; firstOffset = fo; entries = e; }
    }

    private static final ConcurrentMap<String, Data> CACHE = new ConcurrentHashMap<>();
    private static final String CHROME_UA =
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

    /// Fetch + parse sidx for given url at byte range [start, end] (inclusive).
    /// Resolves nested sidx-chains recursively.
    public static Data fetch(String url, int start, int end, String cookies) {
        String key = url + "#" + start + "-" + end;
        Data hit = CACHE.get(key);
        if (hit != null) return hit;
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Range", "bytes=" + start + "-" + end);
            conn.setRequestProperty("User-Agent", CHROME_UA);
            if (cookies != null) conn.setRequestProperty("Cookie", cookies);
            conn.setConnectTimeout(8000);
            conn.setReadTimeout(15000);
            int code = conn.getResponseCode();
            if (code != 200 && code != 206) {
                conn.disconnect();
                return null;
            }
            byte[] data;
            try (var is = conn.getInputStream()) {
                data = is.readAllBytes();
            }
            conn.disconnect();

            RawBox top = parseSidx(data);
            if (top == null) return null;

            List<Entry> flat = new ArrayList<>();
            int cursor = end + 1 + top.firstOffset;
            for (RawEntry e : top.entries) {
                if (e.isIndex) {
                    Data sub = fetch(url, cursor, cursor + e.byteSize - 1, cookies);
                    if (sub != null) flat.addAll(sub.entries);
                } else {
                    flat.add(new Entry(e.duration, e.byteSize));
                }
                cursor += e.byteSize;
            }
            Data out = new Data(top.timescale, top.firstOffset, flat);
            CACHE.put(key, out);
            return out;
        } catch (IOException e) {
            return null;
        }
    }

    private static class RawEntry {
        final boolean isIndex;
        final int byteSize;
        final double duration;
        RawEntry(boolean ii, int bs, double d) { isIndex = ii; byteSize = bs; duration = d; }
    }
    private static class RawBox {
        final int timescale;
        final int firstOffset;
        final List<RawEntry> entries;
        RawBox(int ts, int fo, List<RawEntry> e) { timescale = ts; firstOffset = fo; entries = e; }
    }

    private static RawBox parseSidx(byte[] data) {
        int offset = 0;
        while (offset + 8 <= data.length) {
            long size = readU32(data, offset);
            String type = new String(data, offset + 4, 4);
            if (type.equals("sidx")) {
                return parseSidxBox(data, offset, (int) size);
            }
            if (size < 8 || offset + size > data.length) return null;
            offset += (int) size;
        }
        return null;
    }

    private static RawBox parseSidxBox(byte[] data, int offset, int size) {
        int p = offset + 8; // skip size + type
        if (p >= data.length) return null;
        int version = data[p] & 0xff;
        p += 4; // version + flags

        p += 4; // reference_ID
        if (p + 4 > data.length) return null;
        int timescale = (int) readU32(data, p);
        p += 4;

        int firstOffset;
        if (version == 0) {
            if (p + 8 > data.length) return null;
            p += 4; // earliest_presentation_time
            firstOffset = (int) readU32(data, p);
            p += 4;
        } else {
            if (p + 16 > data.length) return null;
            p += 8; // earliest_presentation_time 64-bit
            firstOffset = (int) readU64(data, p);
            p += 8;
        }

        if (p + 4 > data.length) return null;
        p += 2; // reserved
        int refCount = ((data[p] & 0xff) << 8) | (data[p + 1] & 0xff);
        p += 2;

        List<RawEntry> entries = new ArrayList<>(refCount);
        for (int i = 0; i < refCount; i++) {
            if (p + 12 > data.length) return null;
            long sizeWord = readU32(data, p);
            boolean isIndex = (sizeWord & 0x80000000L) != 0;
            int refSize = (int) (sizeWord & 0x7FFFFFFFL);
            p += 4;
            long subDur = readU32(data, p);
            p += 4;
            p += 4; // SAP info
            double durSec = timescale > 0 ? (double) subDur / (double) timescale : 0;
            entries.add(new RawEntry(isIndex, refSize, durSec));
        }
        return new RawBox(timescale, firstOffset, entries);
    }

    private static long readU32(byte[] d, int o) {
        return ((long)(d[o] & 0xff) << 24) | ((d[o+1] & 0xff) << 16) | ((d[o+2] & 0xff) << 8) | (d[o+3] & 0xff);
    }

    private static long readU64(byte[] d, int o) {
        long v = 0;
        for (int i = 0; i < 8; i++) v = (v << 8) | (d[o+i] & 0xff);
        return v;
    }
}
