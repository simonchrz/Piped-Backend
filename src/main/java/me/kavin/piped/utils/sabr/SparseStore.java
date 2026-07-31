package me.kavin.piped.utils.sabr;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/// Segment-adressierter Speicher fuer SABR-Medien (Streifen-Modus, `SABR_SPARSE=1`).
///
/// WARUM: Bisher war die Cache-Datei ein fortlaufender Strom ab Segment 1. Damit
/// kann der Player nur innerhalb des geladenen Anfangs springen — an eine
/// beliebige Stelle zu spulen war unmoeglich, weil die Bytes dorthin schlicht
/// nicht existieren und ein Loch in einem Strom nicht darstellbar ist.
///
/// WIE: Der `sidx` am Dateianfang beschreibt Position UND Groesse JEDES Segments
/// des ganzen Videos, auch der noch nicht geholten (gemessen: 14 vorhandene von
/// 640 gelisteten). Jedes Segment wird deshalb an genau die Position geschrieben,
/// die es in der fertigen Datei hat. Fehlende Segmente sind Loecher im
/// Dateisystem — die Byte-Anordnung ist identisch zur bisherigen, eine fertig
/// gefuellte Datei ist Byte fuer Byte dieselbe wie eine am Stueck geladene.
///
/// Was da ist, steht in `<id>_<itag>.map`: `lmt totalSegments 1-14,120-133`.
/// ⚠️ Der `lmt` gehoert dazu: YouTube liefert fuer dasselbe itag verschiedene
/// Encodes (an EINEM Video gemessen: 553/640/656 Segmente). Segmentnummern und
/// Offsets gelten nur innerhalb eines Encodes — bei Wechsel wird die Datei
/// verworfen statt zwei Fassungen zu vermischen.
public final class SparseStore {

    private SparseStore() {}

    /// Offsets je (video,itag): Index = Segmentnummer, Wert = Byte-Position.
    private static final Map<String, long[]> OFFSETS = new ConcurrentHashMap<>();
    /// Vorhandene Segmente je (video,itag).
    private static final Map<String, NavigableSet<Integer>> PRESENT = new ConcurrentHashMap<>();

    private static String key(String videoId, int itag) { return videoId + "_" + itag; }

    static Path binPath(String videoId, int itag) {
        return SabrCache.dir().resolve(SabrCache.safeId(videoId) + "_" + itag + ".bin");
    }

    private static Path mapPath(String videoId, int itag) {
        return SabrCache.dir().resolve(SabrCache.safeId(videoId) + "_" + itag + ".map");
    }

    /// Init schreiben, Offset-Tabelle aus dem sidx aufbauen. Bei Encode-Wechsel
    /// wird die alte Datei verworfen (s. Klassenkommentar).
    public static synchronized void writeInit(String videoId, int itag, long lmt, byte[] data)
            throws IOException {
        final Path bin = binPath(videoId, itag);
        final long knownLmt = readMapLmt(videoId, itag);
        if (knownLmt != 0 && knownLmt != lmt) {
            System.out.println("[Sparse] " + videoId + "/" + itag + " Encode-Wechsel (lmt "
                    + knownLmt + " -> " + lmt + ") -> Datei neu");
            Files.deleteIfExists(bin);
            Files.deleteIfExists(mapPath(videoId, itag));
            PRESENT.remove(key(videoId, itag));
            OFFSETS.remove(key(videoId, itag));
        }
        try (RandomAccessFile raf = new RandomAccessFile(bin.toFile(), "rw")) {
            raf.seek(0);
            raf.write(data);
        }
        buildOffsets(videoId, itag, data, lmt);
    }

    /// Segment an SEINE Position schreiben — auch wenn frühere fehlen.
    public static synchronized void writeSegment(String videoId, int itag, int seq, byte[] data)
            throws IOException {
        final long[] offs = OFFSETS.get(key(videoId, itag));
        if (offs == null || seq < 1 || seq >= offs.length) return;   // ohne sidx keine Position
        try (RandomAccessFile raf = new RandomAccessFile(binPath(videoId, itag).toFile(), "rw")) {
            raf.seek(offs[seq]);
            raf.write(data);
        }
        present(videoId, itag).add(seq);
    }

    public static NavigableSet<Integer> present(String videoId, int itag) {
        return PRESENT.computeIfAbsent(key(videoId, itag), k -> {
            final NavigableSet<Integer> set = new TreeSet<>();
            try {
                final Path mp = mapPath(videoId, itag);
                if (Files.exists(mp)) {
                    final String[] f = Files.readString(mp).trim().split("\\s+");
                    if (f.length >= 3) parseRanges(f[2], set);
                }
            } catch (Exception ignored) {}
            return set;
        });
    }

    /// Byte-Position eines Segments; -1 wenn (noch) unbekannt.
    public static long offsetOf(String videoId, int itag, int seq) {
        final long[] offs = OFFSETS.get(key(videoId, itag));
        return offs == null || seq < 1 || seq >= offs.length ? -1 : offs[seq];
    }

    /// Anwesenheitskarte festschreiben. Wird pro Runde aufgerufen.
    public static synchronized void persist(String videoId, int itag, long lmt, int totalSegments) {
        try {
            Files.writeString(mapPath(videoId, itag),
                    lmt + " " + totalSegments + " " + formatRanges(present(videoId, itag)));
        } catch (IOException ignored) {}
    }

    private static long readMapLmt(String videoId, int itag) {
        try {
            final Path mp = mapPath(videoId, itag);
            if (!Files.exists(mp)) return 0;
            final String[] f = Files.readString(mp).trim().split("\\s+");
            return f.length >= 1 ? Long.parseLong(f[0]) : 0;
        } catch (Exception e) {
            return 0;
        }
    }

    /// Offsets aus dem sidx im Init: Segment k liegt hinter dem sidx-Kasten plus
    /// der Summe aller vorherigen Segmentgroessen. Genau die Rechnung, die auch
    /// die Playlist-Schicht fuer ihre BYTERANGE-Angaben benutzt.
    private static void buildOffsets(String videoId, int itag, byte[] init, long lmt) {
        final int[] box = scanSidx(init);
        if (box == null) return;
        final int sidxStart = box[0], sidxSize = box[1];
        final List<long[]> entries = parseSidxEntries(init, sidxStart, sidxSize);
        if (entries.isEmpty()) return;
        final long[] offs = new long[entries.size() + 1];
        long cursor = sidxStart + sidxSize;   // firstOffset ist bei YouTube 0
        for (int i = 0; i < entries.size(); i++) {
            offs[i + 1] = cursor;
            cursor += entries.get(i)[0];
        }
        OFFSETS.put(key(videoId, itag), offs);
        persist(videoId, itag, lmt, entries.size());
        System.out.println("[Sparse] " + videoId + "/" + itag + " Offset-Tabelle: "
                + entries.size() + " Segmente, Gesamtlaenge " + cursor + "B");
    }

    /// {groesse, dauerTicks} je Eintrag aus dem sidx.
    private static List<long[]> parseSidxEntries(byte[] b, int sidxStart, int sidxSize) {
        final List<long[]> out = new ArrayList<>();
        try {
            int p = sidxStart + 8;                       // size+type
            final int version = b[p] & 0xff;
            p += 4;                                      // version+flags
            p += 8;                                      // reference_ID + timescale
            p += version == 0 ? 8 : 16;                  // earliest_pts + first_offset
            p += 2;                                      // reserved
            final int count = ((b[p] & 0xff) << 8) | (b[p + 1] & 0xff);
            p += 2;
            for (int i = 0; i < count && p + 12 <= sidxStart + sidxSize; i++) {
                final long sz = ((b[p] & 0x7fL) << 24) | ((b[p + 1] & 0xffL) << 16)
                        | ((b[p + 2] & 0xffL) << 8) | (b[p + 3] & 0xffL);
                final long dur = ((b[p + 4] & 0xffL) << 24) | ((b[p + 5] & 0xffL) << 16)
                        | ((b[p + 6] & 0xffL) << 8) | (b[p + 7] & 0xffL);
                out.add(new long[]{sz, dur});
                p += 12;
            }
        } catch (Exception ignored) {}
        return out;
    }

    private static int[] scanSidx(byte[] h) {
        int off = 0;
        while (off + 8 <= h.length) {
            final long size = ((h[off] & 0xffL) << 24) | ((h[off + 1] & 0xffL) << 16)
                    | ((h[off + 2] & 0xffL) << 8) | (h[off + 3] & 0xffL);
            final String type = new String(h, off + 4, 4, java.nio.charset.StandardCharsets.ISO_8859_1);
            if ("sidx".equals(type)) return new int[]{off, (int) size};
            if (size <= 0 || size > 100_000_000L) break;
            off += (int) size;
        }
        return null;
    }

    private static void parseRanges(String spec, NavigableSet<Integer> out) {
        if (spec == null || spec.isBlank() || "-".equals(spec)) return;
        for (String part : spec.split(",")) {
            final int dash = part.indexOf('-');
            if (dash < 0) { out.add(Integer.parseInt(part.trim())); continue; }
            final int a = Integer.parseInt(part.substring(0, dash).trim());
            final int b = Integer.parseInt(part.substring(dash + 1).trim());
            for (int i = a; i <= b; i++) out.add(i);
        }
    }

    private static String formatRanges(NavigableSet<Integer> set) {
        if (set.isEmpty()) return "-";
        final StringBuilder sb = new StringBuilder();
        int start = -1, prev = -1;
        for (int v : set) {
            if (start < 0) { start = prev = v; continue; }
            if (v == prev + 1) { prev = v; continue; }
            if (sb.length() > 0) sb.append(',');
            sb.append(start).append('-').append(prev);
            start = prev = v;
        }
        if (sb.length() > 0) sb.append(',');
        sb.append(start).append('-').append(prev);
        return sb.toString();
    }
}
