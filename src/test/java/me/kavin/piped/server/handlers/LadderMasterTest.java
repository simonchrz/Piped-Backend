package me.kavin.piped.server.handlers;

import me.kavin.piped.utils.obj.PipedStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/// Start-Variante im synth-hls-Master (`?ladder=1`).
///
/// Der Sinn der Leiter: das erste Bild kommt aus einem ~200KB-480p-Segment
/// statt aus 1,3-1,6MB 1080p (gemessen 2026-07-30 der dominante Posten des
/// Serve-Pfads, ~1,3-1,5s bei ~1MB/s durch /yt-proxy). Traegt nur, wenn drei
/// Dinge stimmen — und genau die sind hier festgenagelt:
///  1. Die Start-Variante steht ZUERST (AVPlayer beginnt mit der ersten).
///  2. Sie erscheint nur, wenn sie echt kleiner ist (sonst Variant-Pingpong).
///  3. Jede Variante traegt IHRE eigenen Attribute + ihr eigenes ?maxh=
///     (die video0-Route waehlt die Rendition per Query — falsches maxh
///     hiesse: Master verspricht 480p, Variante liefert 1080p).
class LadderMasterTest {

    private static PipedStream video(int height, int width, int bitrate, String codec) {
        final PipedStream s = new PipedStream();
        s.height = height;
        s.width = width;
        s.bitrate = bitrate;
        s.codec = codec;
        s.fps = 30;
        s.videoOnly = true;
        return s;
    }

    private static PipedStream audio() {
        final PipedStream a = new PipedStream();
        a.codec = "mp4a.40.2";
        a.bitrate = 128_000;
        return a;
    }

    private static String master(List<PipedStream> videos, List<String> uris) {
        return new String(SynthHlsHandlers.buildMasterPlaylist(audio(), videos, uris),
                StandardCharsets.UTF_8);
    }

    // ── shouldPrependStart: wann lohnt die Leiter? ─────────────────────────

    @Test
    @DisplayName("Kleinere Start-Rendition vorhanden -> Leiter lohnt")
    void kleinereRenditionLohnt() {
        assertTrue(SynthHlsHandlers.shouldPrependStart(
                video(480, 854, 700_000, "avc1.4d401f"),
                video(1080, 1920, 4_300_000, "avc1.640028")));
    }

    @Test
    @DisplayName("Quelle hat nur <=480p -> KEINE doppelte Variante")
    void gleicheRenditionLohntNicht() {
        // pickedVideoStreams(480) und (1080) geben dann dieselbe Rendition —
        // zwei identische Varianten liessen AVPlayer sinnlos wechseln.
        final PipedStream only = video(480, 854, 700_000, "avc1.4d401f");
        assertFalse(SynthHlsHandlers.shouldPrependStart(only, only));
        assertFalse(SynthHlsHandlers.shouldPrependStart(
                video(480, 854, 700_000, "avc1.4d401f"),
                video(480, 854, 900_000, "avc1.4d401f")));
    }

    @Test
    @DisplayName("Fehlende Start-Rendition oder fehlende Hoehen -> keine Leiter")
    void fehlendeDatenKeineLeiter() {
        final PipedStream main = video(1080, 1920, 4_300_000, "avc1.640028");
        assertFalse(SynthHlsHandlers.shouldPrependStart(null, main));
        assertFalse(SynthHlsHandlers.shouldPrependStart(video(0, 0, 1, "avc1"), main));
        assertFalse(SynthHlsHandlers.shouldPrependStart(video(480, 854, 1, "avc1"), video(0, 0, 1, "avc1")));
    }

    // ── buildMasterPlaylist: der Text, den AVPlayer liest ──────────────────

    @Test
    @DisplayName("Start-Variante steht ZUERST und traegt ihr eigenes maxh")
    void startVarianteZuerstMitEigenemMaxh() {
        final String m = master(
                List.of(video(480, 854, 700_000, "avc1.4d401f"),
                        video(1080, 1920, 4_300_000, "avc1.640028")),
                List.of("video0.m3u8?maxh=480&codecs=avc",
                        "video0.m3u8?maxh=1080&codecs=avc"));
        final int i480 = m.indexOf("video0.m3u8?maxh=480");
        final int i1080 = m.indexOf("video0.m3u8?maxh=1080");
        assertTrue(i480 >= 0 && i1080 >= 0, "beide Varianten muessen im Master stehen:\n" + m);
        assertTrue(i480 < i1080, "AVPlayer beginnt mit der ERSTEN Variante — 480 muss vorn stehen:\n" + m);
    }

    @Test
    @DisplayName("Jede Variante traegt IHRE Attribute (Aufloesung, Bandbreite, Codec)")
    void attributeProVariante() {
        final String m = master(
                List.of(video(480, 854, 700_000, "avc1.4d401f"),
                        video(1080, 1920, 4_300_000, "avc1.640028")),
                List.of("video0.m3u8?maxh=480&codecs=avc",
                        "video0.m3u8?maxh=1080&codecs=avc"));
        // Falsche RESOLUTION/BANDWIDTH liesse AVPlayers ABR-Schaetzung ins Leere
        // laufen — er waehlt Varianten NACH diesen Angaben.
        assertTrue(m.contains("RESOLUTION=854x480"), m);
        assertTrue(m.contains("RESOLUTION=1920x1080"), m);
        assertTrue(m.contains("BANDWIDTH=" + (700_000 + 128_000)), m);
        assertTrue(m.contains("BANDWIDTH=" + (4_300_000 + 128_000)), m);
        assertTrue(m.contains("avc1.4d401f"), m);
        assertTrue(m.contains("avc1.640028"), m);
    }

    @Test
    @DisplayName("Ohne Leiter: byte-identisches Single-Variant-Verhalten")
    void ohneLeiterEineVariante() {
        final String m = master(
                List.of(video(1080, 1920, 4_300_000, "avc1.640028")),
                List.of("video0.m3u8?maxh=1080&codecs=avc"));
        assertEquals(1, m.split("#EXT-X-STREAM-INF", -1).length - 1,
                "genau EINE Variante ohne ladder:\n" + m);
        assertTrue(m.contains("video0.m3u8?maxh=1080&codecs=avc\n"), m);
    }

    @Test
    @DisplayName("SDR bleibt ohne VIDEO-RANGE, HDR-av01 bekommt PQ")
    void videoRangeNurBeiHDR() {
        // Regression-Schutz: der Umbau auf buildMasterPlaylist darf die
        // -12927-Absicherung (VIDEO-RANGE fuer HDR) nicht verlieren.
        final String sdr = master(List.of(video(1080, 1920, 4_300_000, "avc1.640028")),
                List.of("video0.m3u8"));
        assertFalse(sdr.contains("VIDEO-RANGE"), sdr);
        // av01-Feld 8 (Index 7) = transfer characteristics; 16 = PQ/HDR10.
        final String hdr = master(List.of(video(2160, 3840, 12_000_000, "av01.0.13M.10.0.110.09.16.09.0")),
                List.of("video0.m3u8"));
        assertTrue(hdr.contains("VIDEO-RANGE=PQ"), hdr);
    }
}
