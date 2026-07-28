package me.kavin.piped.utils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/// Verträge des Kanal-Urteils.
///
/// Diese Heuristik entscheidet, ob neue Videos eines Kanals direkt auf den
/// langsameren, aber bewährten Auflöse-Weg geschickt werden. Sie ist bewusst
/// vorsichtig gebaut: ein falsches Urteil kostet 3–6 s pro Tap, und ohne die
/// Selbstprüfung könnte sie uns dauerhaft festhalten, obwohl ein Drosselungs-
/// fenster längst vorbei ist. Genau diese Sicherungen sind hier festgenagelt.
class ResolveMemoTest {

    private static final long NOW = 1_800_000_000_000L;
    private static final long FRISCH = NOW - 60_000;              // 1 min alt
    private static final long ALT = NOW - 7 * 3600_000L;          // 7 h → über TTL (6 h)

    /// callNo, der NICHT auf die Selbstprüfung fällt (jeder 5. prüft sich selbst).
    private static final int NORMAL = 1;

    @Test
    @DisplayName("Zu wenig Evidenz -> kein Urteil")
    void unterSchwelleKeinUrteil() {
        // Schwelle ist 3 langsame Resolves; 2 reichen nicht.
        assertFalse(ResolveMemo.shouldPreferSlowPath(0, 0, FRISCH, NOW, NORMAL));
        assertFalse(ResolveMemo.shouldPreferSlowPath(1, 0, FRISCH, NOW, NORMAL));
        assertFalse(ResolveMemo.shouldPreferSlowPath(2, 0, FRISCH, NOW, NORMAL));
        assertTrue(ResolveMemo.shouldPreferSlowPath(3, 0, FRISCH, NOW, NORMAL));
    }

    @Test
    @DisplayName("Nur bei klarer Dominanz -> einzelne Ausreißer kippen keinen gesunden Kanal")
    void nurBeiDominanz() {
        // slow muss > 2*fast sein. Ein Kanal, der meistens schnell auflöst, darf
        // durch ein paar Ausreißer nicht auf den langsamen Weg gezwungen werden.
        assertFalse(ResolveMemo.shouldPreferSlowPath(4, 2, FRISCH, NOW, NORMAL));  // 4 <= 4
        assertFalse(ResolveMemo.shouldPreferSlowPath(4, 10, FRISCH, NOW, NORMAL));
        assertTrue(ResolveMemo.shouldPreferSlowPath(5, 2, FRISCH, NOW, NORMAL));   // 5 > 4
        assertTrue(ResolveMemo.shouldPreferSlowPath(9, 0, FRISCH, NOW, NORMAL));
    }

    @Test
    @DisplayName("Urteil verfällt -> Drosselung ist transient, kein Dauerurteil")
    void urteilVerfaellt() {
        assertTrue(ResolveMemo.shouldPreferSlowPath(9, 0, FRISCH, NOW, NORMAL));
        assertFalse(ResolveMemo.shouldPreferSlowPath(9, 0, ALT, NOW, NORMAL));
    }

    @Test
    @DisplayName("Selbstprüfung: jeder 5. Aufruf probiert trotz Urteil den schnellen Weg")
    void selbstpruefungKannUrteilWiderlegen() {
        // OHNE das könnte die Heuristik sich nie selbst korrigieren.
        int slowPath = 0;
        for (int call = 1; call <= 20; call++)
            if (ResolveMemo.shouldPreferSlowPath(9, 0, FRISCH, NOW, call)) slowPath++;
        assertEquals(16, slowPath, "16 von 20 langsam, 4 Selbstprüfungen");

        assertFalse(ResolveMemo.shouldPreferSlowPath(9, 0, FRISCH, NOW, 5));
        assertFalse(ResolveMemo.shouldPreferSlowPath(9, 0, FRISCH, NOW, 10));
        assertTrue(ResolveMemo.shouldPreferSlowPath(9, 0, FRISCH, NOW, 4));
    }

    @Test
    @DisplayName("Selbstprüfung wird NUR gemeldet, wenn ein Urteil tatsächlich vorlag")
    void selbstpruefungNurBeiVorliegendemUrteil() {
        // Sonst würde jeder 5. Aufruf eines unbekannten Kanals eine Meldung
        // erzeugen, die nichts bedeutet.
        assertTrue(ResolveMemo.isSelfCheck(9, 0, FRISCH, NOW, 5));
        assertFalse(ResolveMemo.isSelfCheck(1, 0, FRISCH, NOW, 5));   // kein Urteil
        assertFalse(ResolveMemo.isSelfCheck(9, 0, ALT, NOW, 5));      // verfallen
        assertFalse(ResolveMemo.isSelfCheck(9, 0, FRISCH, NOW, 4));   // keine Prüfrunde
    }

    @Test
    @DisplayName("Historie wird gedeckelt -> neues Verhalten setzt sich durch")
    void historieWirdGedeckelt() {
        final ResolveMemo.ChannelStat st = new ResolveMemo.ChannelStat();
        for (int i = 0; i < 41; i++) ResolveMemo.applyOutcome(st, true, NOW);
        assertTrue(st.slow + st.fast <= 40, "Summe gedeckelt, war " + (st.slow + st.fast));

        // Ein Kanal, der lange langsam war und jetzt schnell auflöst, muss das
        // Urteil in endlicher Zeit verlieren.
        for (int i = 0; i < 40; i++) ResolveMemo.applyOutcome(st, false, NOW);
        assertFalse(ResolveMemo.shouldPreferSlowPath(st.slow, st.fast, st.updated, NOW, NORMAL),
                "nach 40 schnellen Resolves darf kein Urteil mehr gelten (slow="
                        + st.slow + " fast=" + st.fast + ")");
    }

    @Test
    @DisplayName("Buchung zählt in die richtige Richtung und stempelt die Zeit")
    void buchungZaehltRichtig() {
        final ResolveMemo.ChannelStat st = new ResolveMemo.ChannelStat();
        ResolveMemo.applyOutcome(st, true, NOW);
        assertEquals(1, st.slow);
        assertEquals(0, st.fast);
        assertEquals(NOW, st.updated);

        ResolveMemo.applyOutcome(st, false, NOW + 5);
        assertEquals(1, st.slow);
        assertEquals(1, st.fast);
        assertEquals(NOW + 5, st.updated);
    }
}
