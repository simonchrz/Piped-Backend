package me.kavin.piped.utils.sabr;

import java.util.Arrays;

/// Minimal protobuf wire-format reader. Iterate fields with readTag() then read
/// the value by the field's wire type (or skip()). Only used to decode the few
/// SABR response messages (MediaHeader, etc.).
public final class ProtoReader {

    private final byte[] buf;
    private int pos;
    private final int end;
    private int wireType;

    public ProtoReader(byte[] b) {
        this(b, 0, b.length);
    }

    public ProtoReader(byte[] b, int off, int len) {
        this.buf = b;
        this.pos = off;
        this.end = off + len;
    }

    public boolean hasMore() {
        return pos < end;
    }

    public long readVarint() {
        long r = 0;
        int s = 0;
        while (true) {
            int x = buf[pos++] & 0xff;
            r |= ((long) (x & 0x7f)) << s;
            if ((x & 0x80) == 0) {
                return r;
            }
            s += 7;
        }
    }

    /// Reads the next field's tag; returns the field number and stores the wire type.
    public int readTag() {
        long tag = readVarint();
        wireType = (int) (tag & 7);
        return (int) (tag >>> 3);
    }

    public int wireType() {
        return wireType;
    }

    public byte[] readBytes() {
        int len = (int) readVarint();
        byte[] b = Arrays.copyOfRange(buf, pos, pos + len);
        pos += len;
        return b;
    }

    /// Skip the current field's value based on its wire type.
    public void skip() {
        switch (wireType) {
            case 0:
                readVarint();
                break;
            case 2: {
                final int len = (int) readVarint(); // read BEFORE advancing pos
                pos += len;
                break;
            }
            case 5:
                pos += 4;
                break;
            case 1:
                pos += 8;
                break;
            default:
                throw new IllegalStateException("unknown wire type " + wireType);
        }
    }
}
