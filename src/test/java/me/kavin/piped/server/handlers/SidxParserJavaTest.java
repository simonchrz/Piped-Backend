package me.kavin.piped.server.handlers;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/// Byte-Ebene des sidx-Parsers (`fromFile`).
///
/// Diese Schicht erzeugt die Byte-Ranges der HLS-Segmente. Ein Fehler hier ist
/// nicht sichtbar, sondern äußert sich als 416 vom /sabr-Endpoint oder als
/// AVPlayer-Abbruch — also weit weg von der Ursache. `fromFile` wurde
/// 2026-07-25 neu geschrieben (der frühere Weg lief per HTTP gegen den eigenen
/// Endpoint und blockierte sich am Resolve-Limiter selbst), inklusive
/// handgeschriebenem Box-Scanning und Range-Lesen — genau die Art Code, die
/// Tests braucht.
class SidxParserJavaTest {

    @TempDir Path tmp;

    // ── Fixture-Bau (ISO 14496-12) ─────────────────────────────────────────

    private static void u32(ByteArrayOutputStream o, long v) {
        o.write((int) (v >> 24 & 0xff)); o.write((int) (v >> 16 & 0xff));
        o.write((int) (v >> 8 & 0xff));  o.write((int) (v & 0xff));
    }

    private static void u16(ByteArrayOutputStream o, int v) {
        o.write(v >> 8 & 0xff); o.write(v & 0xff);
    }

    /// sidx-Box, Version 0. `refs` = {byteSize, durationTicks, isIndex}.
    private static byte[] sidxBox(int timescale, int firstOffset, long[][] refs) {
        final ByteArrayOutputStream body = new ByteArrayOutputStream();
        u32(body, 0);                       // version(1)+flags(3) -> Version 0
        u32(body, 1);                       // reference_ID
        u32(body, timescale);
        u32(body, 0);                       // earliest_presentation_time
        u32(body, firstOffset);
        u16(body, 0);                       // reserved
        u16(body, refs.length);             // reference_count
        for (long[] r : refs) {
            u32(body, (r[2] != 0 ? 0x80000000L : 0L) | r[0]);   // index-Bit + size
            u32(body, r[1]);                                    // subsegment_duration
            u32(body, 0);                                       // SAP
        }
        final byte[] b = body.toByteArray();
        final ByteArrayOutputStream box = new ByteArrayOutputStream();
        u32(box, b.length + 8);
        box.write('s'); box.write('i'); box.write('d'); box.write('x');
        box.writeBytes(b);
        return box.toByteArray();
    }

    /// Datei aus Präfix (ftyp/moov-Attrappe) + sidx + `mediaBytes` Nutzdaten.
    private Path file(int prefix, byte[] sidx, int mediaBytes) throws IOException {
        final ByteArrayOutputStream o = new ByteArrayOutputStream();
        o.writeBytes(new byte[prefix]);
        o.writeBytes(sidx);
        o.writeBytes(new byte[mediaBytes]);
        final Path p = tmp.resolve("m.bin");
        Files.write(p, o.toByteArray());
        return p;
    }

    // ── Tests ──────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Einträge werden mit Größe und Dauer korrekt gelesen")
    void liestEintraege() throws IOException {
        final byte[] sidx = sidxBox(1000, 0, new long[][]{{500, 2000, 0}, {700, 3000, 0}});
        final Path f = file(64, sidx, 2000);
        final SidxParserJava.Data d = SidxParserJava.fromFile(f, 64, 64 + sidx.length - 1);

        assertNotNull(d);
        assertEquals(2, d.entries.size());
        assertEquals(500, d.entries.get(0).byteSize);
        assertEquals(2.0, d.entries.get(0).duration, 1e-9);   // 2000/1000 Ticks
        assertEquals(700, d.entries.get(1).byteSize);
        assertEquals(3.0, d.entries.get(1).duration, 1e-9);
        assertEquals(0, d.firstOffset);
    }

    @Test
    @DisplayName("firstOffset wird durchgereicht — er verschiebt ALLE Segment-Offsets")
    void firstOffsetWirdGelesen() throws IOException {
        final byte[] sidx = sidxBox(1000, 128, new long[][]{{100, 1000, 0}});
        final Path f = file(0, sidx, 500);
        final SidxParserJava.Data d = SidxParserJava.fromFile(f, 0, sidx.length - 1);
        assertNotNull(d);
        assertEquals(128, d.firstOffset);
    }

    @Test
    @DisplayName("Zu kurze Datei -> null statt halb geparster Box")
    void zuKurzeDateiGibtNull() throws IOException {
        final byte[] sidx = sidxBox(1000, 0, new long[][]{{500, 2000, 0}});
        // Datei endet MITTEN in der sidx-Box: der angeforderte Range liegt nicht
        // vollständig auf Platte. Das passiert real, solange der SABR-Download
        // noch läuft — dann muss der Aufrufer warten, nicht raten.
        final Path p = tmp.resolve("kurz.bin");
        Files.write(p, java.util.Arrays.copyOf(sidx, sidx.length / 2));
        assertNull(SidxParserJava.fromFile(p, 0, sidx.length - 1));
    }

    @Test
    @DisplayName("Range außerhalb der Datei -> null, kein Absturz")
    void rangeAusserhalbGibtNull() throws IOException {
        final Path f = file(16, sidxBox(1000, 0, new long[][]{{10, 100, 0}}), 0);
        assertNull(SidxParserJava.fromFile(f, 100_000, 100_100));
        assertNull(SidxParserJava.fromFile(f, 5, 4));      // end < start
        assertNull(SidxParserJava.fromFile(f, -1, 10));    // negativer Start
    }

    @Test
    @DisplayName("Kein sidx an der Stelle -> null")
    void keinSidxGibtNull() throws IOException {
        final Path p = tmp.resolve("mist.bin");
        Files.write(p, new byte[256]);   // nur Nullen, keine Box
        assertNull(SidxParserJava.fromFile(p, 0, 255));
    }

    @Test
    @DisplayName("Verschachtelter sidx wird aufgelöst und flach eingehängt")
    void verschachtelterSidxWirdAufgeloest() throws IOException {
        // YouTube nutzt sidx-Ketten auf langen Videos: der erste Eintrag zeigt
        // per Index-Bit auf einen UNTER-sidx. Behandelt man ihn als Mediendaten,
        // holt der Player Index-Bytes statt Video — der bekannte Stall bei ~46s.
        final byte[] sub = sidxBox(1000, 0, new long[][]{{111, 1000, 0}, {222, 2000, 0}});
        final byte[] top = sidxBox(1000, 0, new long[][]{{sub.length, 3000, 1}});

        final ByteArrayOutputStream o = new ByteArrayOutputStream();
        o.writeBytes(top);    // top-sidx ab 0
        o.writeBytes(sub);    // sub-sidx direkt dahinter (cursor = end+1+firstOffset)
        o.writeBytes(new byte[1000]);
        final Path p = tmp.resolve("nested.bin");
        Files.write(p, o.toByteArray());

        final SidxParserJava.Data d = SidxParserJava.fromFile(p, 0, top.length - 1);
        assertNotNull(d);
        assertEquals(2, d.entries.size(), "Unter-Einträge müssen flach eingehängt sein");
        assertEquals(111, d.entries.get(0).byteSize);
        assertEquals(222, d.entries.get(1).byteSize);
        assertTrue(d.entries.stream().noneMatch(e -> e.byteSize == sub.length),
                "der Index-Eintrag selbst darf NICHT als Mediensegment auftauchen");
    }

    @Test
    @DisplayName("timescale=0 stürzt nicht ab (Division)")
    void timescaleNullOhneAbsturz() throws IOException {
        final byte[] sidx = sidxBox(0, 0, new long[][]{{100, 1000, 0}});
        final Path f = file(0, sidx, 100);
        final SidxParserJava.Data d = SidxParserJava.fromFile(f, 0, sidx.length - 1);
        assertNotNull(d);
        assertEquals(0.0, d.entries.get(0).duration, 1e-9);
    }
}
