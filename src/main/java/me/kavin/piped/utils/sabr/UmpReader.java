package me.kavin.piped.utils.sabr;

import java.util.Arrays;
import java.util.function.BiConsumer;

/// Parses a UMP (Universal Media Playback) byte stream into its parts. Each part
/// is [varint partType][varint partSize][partSize bytes]. The UMP varint is NOT
/// the protobuf varint -- the first byte's value selects the total length. See
/// LuanRT/googlevideo UmpReader.readVarInt + SABR-SPEC.md.
public final class UmpReader {

    /// Parse an in-memory UMP buffer, invoking handler(partType, payload) per part.
    public static void parse(byte[] buf, BiConsumer<Integer, byte[]> handler) {
        int off = 0;
        while (off < buf.length) {
            long[] t = varint(buf, off);
            if (t == null) break;
            int type = (int) t[0];
            off = (int) t[1];
            long[] s = varint(buf, off);
            if (s == null) break;
            int size = (int) s[0];
            off = (int) s[1];
            if (off + size > buf.length) break;
            handler.accept(type, Arrays.copyOfRange(buf, off, off + size));
            off += size;
        }
    }

    /// UMP varint: firstByte<128->1B, <192->2B, <224->3B, <240->4B, else 5B.
    /// Returns {value, newOffset} or null if out of bounds.
    static long[] varint(byte[] b, int off) {
        if (off >= b.length) return null;
        int b0 = b[off] & 0xff;
        int n = b0 < 128 ? 1 : b0 < 192 ? 2 : b0 < 224 ? 3 : b0 < 240 ? 4 : 5;
        if (off + n > b.length) return null;
        long v;
        switch (n) {
            case 1:
                v = b0;
                break;
            case 2:
                v = (b0 & 0x3f) + 64L * (b[off + 1] & 0xff);
                break;
            case 3:
                v = (b0 & 0x1f) + 32L * ((b[off + 1] & 0xff) + 256L * (b[off + 2] & 0xff));
                break;
            case 4:
                v = (b0 & 0x0f) + 16L * ((b[off + 1] & 0xff)
                        + 256L * ((b[off + 2] & 0xff) + 256L * (b[off + 3] & 0xff)));
                break;
            default:
                v = (b[off + 1] & 0xffL) | ((b[off + 2] & 0xffL) << 8)
                        | ((b[off + 3] & 0xffL) << 16) | ((b[off + 4] & 0xffL) << 24);
                break;
        }
        return new long[]{v, off + n};
    }

    // UMP part-type IDs we care about.
    public static final int MEDIA_HEADER = 20, MEDIA = 21, MEDIA_END = 22,
            NEXT_REQUEST_POLICY = 35, FORMAT_INIT_METADATA = 42, SABR_REDIRECT = 43,
            SABR_ERROR = 44, STREAM_PROTECTION_STATUS = 58;
}
