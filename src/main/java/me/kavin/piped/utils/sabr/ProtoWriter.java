package me.kavin.piped.utils.sabr;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

/// Minimal protobuf wire-format encoder (no protobuf-java dependency). Only the
/// pieces SABR needs: varint + length-delimited (bytes/string/embedded message)
/// fields. See SABR-SPEC.md.
public final class ProtoWriter {

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();

    public static byte[] varint(long value) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        long u = value;
        while (true) {
            int x = (int) (u & 0x7f);
            u >>>= 7;
            if (u != 0) {
                b.write(x | 0x80);
            } else {
                b.write(x);
                break;
            }
        }
        return b.toByteArray();
    }

    private void writeTag(int field, int wireType) {
        out.writeBytes(varint(((long) field << 3) | wireType));
    }

    public ProtoWriter varintField(int field, long value) {
        writeTag(field, 0);
        out.writeBytes(varint(value));
        return this;
    }

    public ProtoWriter bytesField(int field, byte[] data) {
        writeTag(field, 2);
        out.writeBytes(varint(data.length));
        out.writeBytes(data);
        return this;
    }

    public ProtoWriter stringField(int field, String s) {
        return bytesField(field, s.getBytes(StandardCharsets.UTF_8));
    }

    /// proto2 `float` = wire type 5 (fixed32), little-endian IEEE-754.
    /// Gebraucht fuer `client_abr_state.playback_rate` (Feld 35) — die
    /// Referenz-Implementierung schickt es in JEDER ABR-Anfrage.
    public ProtoWriter floatField(int field, float value) {
        writeTag(field, 5);
        final int bits = Float.floatToRawIntBits(value);
        out.write(bits & 0xff);
        out.write((bits >>> 8) & 0xff);
        out.write((bits >>> 16) & 0xff);
        out.write((bits >>> 24) & 0xff);
        return this;
    }

    public byte[] toByteArray() {
        return out.toByteArray();
    }
}
