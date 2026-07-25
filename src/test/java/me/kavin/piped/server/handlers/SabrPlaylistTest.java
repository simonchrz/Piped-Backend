package me.kavin.piped.server.handlers;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/// Invarianten der SABR-Variant-Playlist.
///
/// Jeder Test hier steht für einen ECHTEN Ausfall vom 2026-07-24/25, bei dem
/// Kinder-Videos nicht starteten. Die Fehlerbilder (AVPlayer -12646 / -16849)
/// sagen nichts über die Ursache — deshalb sind die Verträge hier festgenagelt.
class SabrPlaylistTest {

    /// sidx mit `n` gleich grossen Einträgen.
    private static SidxParserJava.Data sidx(int n, int byteSize, double dur) {
        final List<SidxParserJava.Entry> es = new ArrayList<>();
        for (int i = 0; i < n; i++) es.add(new SidxParserJava.Entry(dur, byteSize));
        return new SidxParserJava.Data(1000, 0, es);
    }

    /// Dateigrösse, die genau `segs` Segmente vollständig enthält.
    private static long fileLenFor(int sidxEnd, int segs, int byteSize) {
        return sidxEnd + 1L + (long) segs * byteSize;
    }

    private static String build(SidxParserJava.Data d, long fileLen, boolean terminal) {
        // videoId=null -> kein requestRefill-Seiteneffekt im Test
        return SynthHlsHandlers.buildSabrVariantPlaylist(d, 100, 199, fileLen, "/sabr/x/137",
                terminal, null, 137);
    }

    @Test
    @DisplayName("Kein Segment vollständig auf Platte -> null (NIE leere ENDLIST-Playlist)")
    void noSegmentsYieldsNull() {
        // War der Ausfall: 0 Segmente + ENDLIST = gültiges, aber LEERES VOD ->
        // AVPlayer -12646, Video startet gar nicht.
        assertNull(build(sidx(10, 1000, 5.0), fileLenFor(199, 0, 1000), true));
        assertNull(build(sidx(10, 1000, 5.0), fileLenFor(199, 0, 1000), false));
    }

    @Test
    @DisplayName("Leerer/ fehlender sidx -> null statt Fehlertext")
    void emptySidxYieldsNull() {
        assertNull(build(sidx(0, 1000, 5.0), 999_999, true));
        assertNull(SynthHlsHandlers.buildSabrVariantPlaylist(null, 100, 199, 9_999, "/s", true, null, 137));
    }

    @Test
    @DisplayName("Playlist enthält NIEMALS einen #ERROR-Body")
    void neverEmitsErrorBody() {
        // Ein 200 mit „#ERROR: …" ist für AVPlayer eine kaputte Playlist -> -12646
        // ohne Retry. Fehlerfälle MÜSSEN null liefern (Aufrufer wartet/5xx).
        for (int segs = 0; segs <= 10; segs++) {
            for (boolean terminal : new boolean[]{true, false}) {
                final String pl = build(sidx(10, 1000, 5.0), fileLenFor(199, segs, 1000), terminal);
                if (pl != null) assertFalse(pl.contains("#ERROR"), "segs=" + segs);
            }
        }
    }

    @Test
    @DisplayName("Nur vollständig vorhandene Segmente werden ausgewiesen")
    void onlyFullyPresentSegments() {
        // Ein angekündigtes, aber nicht vorhandenes Segment lässt /sabr mit 416
        // antworten -> der Player verwirft das ganze Video.
        final String pl = build(sidx(10, 1000, 5.0), fileLenFor(199, 3, 1000), false);
        assertNotNull(pl);
        assertEquals(3, countOccurrences(pl, "#EXTINF:"));
        // ein halbes Segment mehr an Bytes ändert nichts
        final String pl2 = build(sidx(10, 1000, 5.0), fileLenFor(199, 3, 1000) + 999, false);
        assertEquals(3, countOccurrences(pl2, "#EXTINF:"));
    }

    @Test
    @DisplayName("Vollständiger Cache -> ENDLIST")
    void completeCacheGetsEndlist() {
        final String pl = build(sidx(4, 1000, 5.0), fileLenFor(199, 4, 1000), false);
        assertNotNull(pl);
        assertTrue(pl.endsWith("#EXT-X-ENDLIST"));
        assertEquals(4, countOccurrences(pl, "#EXTINF:"));
    }

    @Test
    @DisplayName("Teil-Cache, nicht terminal -> KEIN ENDLIST (Player pollt weiter)")
    void partialNonTerminalHasNoEndlist() {
        final String pl = build(sidx(10, 1000, 5.0), fileLenFor(199, 2, 1000), false);
        assertNotNull(pl);
        assertFalse(pl.contains("#EXT-X-ENDLIST"));
    }

    @Test
    @DisplayName("Teil-Cache, terminal (Kids-Cap) -> ENDLIST, damit der Teil sauber spielt")
    void partialTerminalGetsEndlist() {
        // Ohne ENDLIST wartet AVPlayer auf Wachstum, das nie kommt -> -12646.
        final String pl = build(sidx(10, 1000, 5.0), fileLenFor(199, 2, 1000), true);
        assertNotNull(pl);
        assertTrue(pl.endsWith("#EXT-X-ENDLIST"));
        assertEquals(2, countOccurrences(pl, "#EXTINF:"));
    }

    @Test
    @DisplayName("Byte-Ranges sind lückenlos und starten hinter dem sidx")
    void byteRangesAreContiguous() {
        final String pl = build(sidx(3, 1000, 5.0), fileLenFor(199, 3, 1000), true);
        assertNotNull(pl);
        assertTrue(pl.contains("#EXT-X-BYTERANGE:1000@200"), pl);
        assertTrue(pl.contains("#EXT-X-BYTERANGE:1000@1200"), pl);
        assertTrue(pl.contains("#EXT-X-BYTERANGE:1000@2200"), pl);
    }

    @Test
    @DisplayName("Pflicht-Tags + Init-Segment vorhanden")
    void hasRequiredTags() {
        final String pl = build(sidx(2, 1000, 6.4), fileLenFor(199, 2, 1000), true);
        assertNotNull(pl);
        assertTrue(pl.startsWith("#EXTM3U"));
        assertTrue(pl.contains("#EXT-X-VERSION:7"));
        assertTrue(pl.contains("#EXT-X-MEDIA-SEQUENCE:0"));
        assertTrue(pl.contains("#EXT-X-MAP:URI=\"/sabr/x/137\",BYTERANGE=\"100@0\""));
        // TARGETDURATION muss >= der laengsten Segmentdauer sein (aufgerundet)
        assertTrue(pl.contains("#EXT-X-TARGETDURATION:7"), pl);
    }

    private static int countOccurrences(String hay, String needle) {
        int n = 0, i = 0;
        while ((i = hay.indexOf(needle, i)) >= 0) { n++; i += needle.length(); }
        return n;
    }
}
