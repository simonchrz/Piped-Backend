#!/usr/bin/env python3
"""Konformitaetssuite fuer den yt-Backend-Vertrag (Phase 3b des Go-Ports).

Prueft ausschliesslich ueber HTTP und kennt die Implementierung nicht — sie
laeuft heute gegen den Java-Stack (:8881) und spaeter unveraendert gegen die
Go-Binary (:8891). Keine Abhaengigkeiten ausser der Standardbibliothek.

    python3 konformitaet.py --base http://127.0.0.1:8881
    python3 konformitaet.py --base http://127.0.0.1:8891 --videos VID1 VID2
    python3 konformitaet.py --vergleich http://127.0.0.1:8891   # Java gegen Go

Jede Regel stammt aus einem echten Fehler. Wo das so ist, steht er dabei —
ohne den Fehler ist nicht nachvollziehbar, warum die Regel so scharf ist.

Exit 0 = alle Regeln erfuellt. Exit 1 = mindestens eine verletzt.
"""

import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

# Ueber SSH steht die Ausgabe sonst auf iso8859-15 und schon ein Gedankenstrich
# bricht den Lauf ab. Die Suite soll ohne PYTHONIOENCODING ueberall laufen.
for _strom in (sys.stdout, sys.stderr):
    if hasattr(_strom, "reconfigure"):
        _strom.reconfigure(encoding="utf-8", errors="replace")

# Bekannte Audio-itags: AAC 139/140/141, Opus 249/250/251, AC-3 256/258,
# DTSE 325/328. Deckungsgleich mit SabrSession.istAudioItag.
AUDIO_ITAGS = {139, 140, 141, 249, 250, 251, 256, 258, 325, 328}

# Standardauswahl: je eine Videoklasse, die sich anders verhaelt.
STANDARD_VIDEOS = [
    ("jNQXAC9IVRw", "kurz"),
    ("dQw4w9WgXcQ", "normal"),
    ("tg0Ll77eBHI", "gedrosselt"),
    ("9HwZZ4lMr2o", "made-for-kids"),
]


class Fehler(Exception):
    pass


# ---------------------------------------------------------------- HTTP


def hole(url, bereich=None, timeout=90):
    """GET, optional mit Range. Liefert (status, bytes)."""
    anfrage = urllib.request.Request(url)
    if bereich:
        anfrage.add_header("Range", "bytes=%d-%d" % bereich)
    try:
        with urllib.request.urlopen(anfrage, timeout=timeout) as a:
            return a.status, a.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def hole_text(url, timeout=90):
    status, rumpf = hole(url, timeout=timeout)
    if status != 200:
        raise Fehler("HTTP %d fuer %s" % (status, url))
    return rumpf.decode("utf-8", "replace")


# ---------------------------------------------------------------- Parsen


class Spur:
    """Eine geparste Medien-Playlist."""

    def __init__(self, name, text, basis_url):
        self.name = name
        self.text = text
        self.map_uri = None
        self.map_range = None
        self.segmente = []       # (dauer_s, laenge, offset, uri)
        self.endlist = "#EXT-X-ENDLIST" in text
        self._parse(basis_url)

    def _parse(self, basis_url):
        dauer = None
        bereich = None
        for zeile in self.text.splitlines():
            zeile = zeile.strip()
            if zeile.startswith("#EXT-X-MAP:"):
                m = re.search(r'URI="([^"]+)"', zeile)
                r = re.search(r'BYTERANGE="(\d+)@(\d+)"', zeile)
                if m:
                    self.map_uri = urllib.parse.urljoin(basis_url, m.group(1))
                if r:
                    self.map_range = (int(r.group(2)),
                                      int(r.group(2)) + int(r.group(1)) - 1)
            elif zeile.startswith("#EXTINF:"):
                dauer = float(zeile.split(":", 1)[1].rstrip(","))
            elif zeile.startswith("#EXT-X-BYTERANGE:"):
                laenge, _, offset = zeile.split(":", 1)[1].partition("@")
                bereich = (int(laenge), int(offset))
            elif zeile and not zeile.startswith("#"):
                if dauer is not None and bereich is not None:
                    self.segmente.append(
                        (dauer, bereich[0], bereich[1],
                         urllib.parse.urljoin(basis_url, zeile)))
                dauer, bereich = None, None

    @property
    def gesamtdauer(self):
        return sum(s[0] for s in self.segmente)

    @property
    def itag(self):
        """itag der Spur — aus der Query oder aus dem /sabr/<id>/<itag>-Pfad."""
        quelle = self.map_uri or (self.segmente[0][3] if self.segmente else None)
        if not quelle:
            return None
        m = re.search(r"[?&]itag=(\d+)", quelle)
        if m:
            return int(m.group(1))
        m = re.search(r"/sabr/[^/]+/(\d+)", urllib.parse.urlparse(quelle).path)
        return int(m.group(1)) if m else None


# ---------------------------------------------------------------- Regeln


class Lauf:
    def __init__(self, ausfuehrlich=False):
        self.ok = 0
        self.fehler = []
        self.uebersprungen = 0
        self.ausfuehrlich = ausfuehrlich
        # Fehlt der Debug-Endpunkt, nur EINMAL melden statt je Video.
        self.marken_gemeldet = False

    def pruefe(self, bedingung, regel, kontext, detail=""):
        if bedingung:
            self.ok += 1
            if self.ausfuehrlich:
                print("      ok   %s" % regel)
        else:
            self.fehler.append((kontext, regel, detail))
            print("      FEHLER %s%s" % (regel, (" — " + detail) if detail else ""))

    def ueberspringe(self, regel, grund):
        self.uebersprungen += 1
        print("      (uebersprungen) %s — %s" % (regel, grund))


def spur_laden(basis, video_id, playlist_pfad, name):
    url = urllib.parse.urljoin(basis + "/", playlist_pfad)
    return Spur(name, hole_text(url), url)


def regeln_playlist(ton, bild, dauer_soll=0):
    """Alle Regeln, die allein aus den Playlisten folgen — ohne Netz.

    Getrennt, damit der Selbsttest dieselben Ausdruecke prueft wie der
    Ernstfall. Sonst driften Regel und Test auseinander und die Suite
    behauptet nur, etwas zu koennen.

    Liefert [(regel, erfuellt, detail)].
    """
    r = []

    # 2026-08-02: passendesFormat() durchsuchte nur videoStreams und fiel ohne
    # Treffer auf "bestes Video" zurueck. Die audio.m3u8 verwies auf itag 137,
    # AVPlayer bekam zweimal Video: schwarzer Frame, kein Ton, KEINE
    # Fehlermeldung. Beide Seiten lieferten HTTP 200 — ueber Statuscodes oder
    # normalisiertes JSON ist das nicht zu finden.
    r.append(("audio.m3u8 verweist auf einen Audio-itag",
              ton.itag in AUDIO_ITAGS, "itag=%s" % ton.itag))
    r.append(("die Videovariante verweist auf einen Video-itag",
              bild.itag is not None and bild.itag not in AUDIO_ITAGS,
              "itag=%s" % bild.itag))
    r.append(("Ton und Bild sind verschiedene Formate",
              ton.itag != bild.itag, "beide itag=%s" % ton.itag))

    for spur in (ton, bild):
        r.append(("%s: Playlist hat Segmente" % spur.name,
                  bool(spur.segmente), ""))
    if not ton.segmente or not bild.segmente:
        return r

    # Die Segmentzahlen DUERFEN sich unterscheiden (Ton 182 gegen Bild 345 beim
    # selben Video) — die Summe nicht. Faellt eine Spur zu kurz aus, endet die
    # Wiedergabe vorzeitig.
    r.append(("Ton und Bild decken dieselbe Laufzeit",
              abs(ton.gesamtdauer - bild.gesamtdauer)
              <= max(10.0, 0.02 * bild.gesamtdauer),
              "Ton %.1fs, Bild %.1fs" % (ton.gesamtdauer, bild.gesamtdauer)))

    if dauer_soll > 0:
        r.append(("Playlistlaufzeit passt zur gemeldeten Videodauer",
                  abs(bild.gesamtdauer - dauer_soll)
                  <= max(15.0, 0.05 * dauer_soll),
                  "Playlist %.1fs, streams %.1fs" % (bild.gesamtdauer, dauer_soll)))

    # 0f746d7: weist googlevideo den Index-Bereich ab, wird die Playlist EIN
    # Segment ueber die volle Laufzeit — die Wiedergabe laeuft, aber AVPlayer
    # kann nicht springen. Am Statuscode nicht zu erkennen.
    for spur in (ton, bild):
        if spur.gesamtdauer > 120:
            r.append(("%s: mehr als ein Segment (springbar)" % spur.name,
                      len(spur.segmente) > 1,
                      "%d Segment(e) ueber %.0fs"
                      % (len(spur.segmente), spur.gesamtdauer)))

    for spur in (ton, bild):
        loecher = [i for i in range(1, len(spur.segmente))
                   if spur.segmente[i - 1][2] + spur.segmente[i - 1][1]
                   != spur.segmente[i][2]]
        r.append(("%s: Byte-Bereiche schliessen luecken- und ueberlappungsfrei an"
                  % spur.name, not loecher,
                  "erste Bruchstelle bei Segment %s" % (loecher[0] if loecher else "-")))
        r.append(("%s: EXT-X-MAP mit BYTERANGE vorhanden" % spur.name,
                  spur.map_uri is not None and spur.map_range is not None, ""))
    return r


def pruefe_video(basis, video_id, klasse, lauf):
    print("  %s (%s)" % (video_id, klasse))
    kontext = "%s/%s" % (video_id, klasse)

    # --- Ausgangsdaten
    streams = json.loads(hole_text("%s/streams/%s" % (basis, video_id)))
    dauer_soll = float(streams.get("duration") or 0)
    hls = streams.get("hls")
    lauf.pruefe(bool(hls), "streams liefert eine hls-URL", kontext)
    if not hls:
        return

    master = hole_text(hls)
    lauf.pruefe(master.startswith("#EXTM3U"), "master ist eine HLS-Playlist", kontext)

    varianten = [z.strip() for z in master.splitlines()
                 if z.strip() and not z.startswith("#")]
    lauf.pruefe(bool(varianten), "master nennt mindestens eine Variante", kontext)
    hat_audio_gruppe = "#EXT-X-MEDIA:TYPE=AUDIO" in master
    lauf.pruefe(hat_audio_gruppe, "master nennt eine Audio-Gruppe", kontext)
    if not varianten or not hat_audio_gruppe:
        return

    ton = spur_laden(basis, video_id, "synth-hls/%s/audio.m3u8" % video_id, "Ton")
    bild = spur_laden(basis, video_id,
                      "synth-hls/%s/%s" % (video_id, varianten[0]), "Bild")

    # --- Regeln aus den Playlisten allein (s. regeln_playlist)
    for regel, erfuellt, detail in regeln_playlist(ton, bild, dauer_soll):
        lauf.pruefe(erfuellt, regel, kontext, "" if erfuellt else detail)
    if not ton.segmente or not bild.segmente:
        return

    # --- Was die Playlist verspricht, liefert der Server auch.
    # Genau die Bytes, nicht mehr und nicht weniger. Ein Server, der bei
    # Bereichsanfragen die ganze Datei schickt, sprengt den Speicher
    # (serveFile-OOM 2026-07-31) und bringt AVPlayer zum Abbruch.
    for spur in (ton, bild):
        if spur.map_range:
            status, rumpf = hole(spur.map_uri, spur.map_range)
            soll = spur.map_range[1] - spur.map_range[0] + 1
            lauf.pruefe(status == 206 and len(rumpf) == soll,
                        "%s: Init-Segment kommt als 206 in exakter Laenge" % spur.name,
                        kontext, "http %s, %d statt %d B" % (status, len(rumpf), soll))
        dauer_s, laenge, offset, uri = spur.segmente[0]
        status, rumpf = hole(uri, (offset, offset + laenge - 1))
        lauf.pruefe(status == 206 and len(rumpf) == laenge,
                    "%s: erstes Segment kommt als 206 in exakter Laenge" % spur.name,
                    kontext, "http %s, %d statt %d B" % (status, len(rumpf), laenge))

    # --- Regel 6: ein Sprung ins Ungeladene liefert Daten, keinen Fehler.
    # 2026-08-02: die Zielzeit wurde aus einer MITTLEREN Segmentdauer gerechnet
    # (die echten schwanken 3921-6798 ms). Der Server begann darum ein Segment
    # zu spaet, das angeforderte kam nie, nach 20 s kam HTTP 503. Die Regel
    # prueft das Verhalten, nicht den Rechenweg — sie gilt fuer jede
    # Implementierung.
    # --- Markenzustand (s. yt-backend-marken.md)
    marken = marken_holen(basis, video_id)
    if marken is None and not lauf.marken_gemeldet:
        lauf.marken_gemeldet = True
        lauf.ueberspringe("Markenregeln",
                          "/debug/marken/<videoId> nicht vorhanden — bei einer "
                          "Implementierung ohne diesen Endpunkt erwartbar")
    elif marken is not None:
        for regel, erfuellt, detail in regeln_marken(
                marken, [s[3] for s in bild.segmente[:5]]):
            lauf.pruefe(erfuellt, regel, kontext, "" if erfuellt else detail)

    ziel = int(len(bild.segmente) * 0.75)
    if ziel > 2:
        dauer_s, laenge, offset, uri = bild.segmente[ziel]
        t0 = time.time()
        status, rumpf = hole(uri, (offset, offset + laenge - 1), timeout=60)
        gebraucht = time.time() - t0
        lauf.pruefe(status == 206 and len(rumpf) == laenge,
                    "Sprung auf Segment %d liefert 206 in exakter Laenge" % ziel,
                    kontext, "http %s nach %.1fs, %d statt %d B"
                    % (status, gebraucht, len(rumpf), laenge))
    else:
        lauf.ueberspringe("Sprungtest", "zu wenige Segmente")


def marken_holen(basis, video_id):
    """`/debug/marken/<videoId>` — None, wenn es den Endpunkt nicht gibt."""
    status, rumpf = hole("%s/debug/marken/%s" % (basis, video_id), timeout=20)
    if status != 200:
        return None
    try:
        return json.loads(rumpf.decode("utf-8", "replace"))
    except ValueError:
        return None


# Frist je Markendatei laut Code (SabrCache). Die Suite prueft nicht die
# Konstante, sondern dass keine Marke JENSEITS ihrer Frist noch gilt — sonst
# haelt ein Video ewig an einem Sonderweg fest.
MARKEN_FRISTEN_MS = {
    "throttled": 24 * 3600_000,
    "webembed": 24 * 3600_000,
    "sabr_tot": 6 * 3600_000,
}


def regeln_marken(marken, segment_uris):
    """Regeln ueber den Markenzustand. [(regel, erfuellt, detail)].

    `segment_uris` sind die Segment-URLs der Videospur — daran haengt, ob
    gerade ueber /sabr oder direkt ausgeliefert wird.
    """
    r = []
    if marken is None:
        return [("/debug/marken/<videoId> ist erreichbar", False,
                 "kein JSON — der Endpunkt fehlt oder antwortet anders")]

    for gruppe in ("routing", "sitzung", "sprung", "global"):
        r.append(("Markenbericht enthaelt die Gruppe %s" % gruppe,
                  isinstance(marken.get(gruppe), dict), ""))
    if not all(isinstance(marken.get(g), dict)
               for g in ("routing", "sitzung", "global")):
        return r

    routing = marken["routing"]
    sitzung = marken["sitzung"]

    # 🔴 DIE Regel aus dem Deadlock vom 2026-08-02.
    # `sabr_tot` heisst: googlevideo weist die SABR-POST mit 403 ab, ueber SABR
    # kommt fuer dieses Video NICHTS. Wird die Playlist trotzdem aus /sabr
    # bedient und liegt kein Cache vor, wartet der Player auf Daten, die nie
    # entstehen — in der App HTTP 500 nach 12 s. Genau diese Kombination stand
    # damals an: throttled + storm + sabr_tot, hat_cache=false.
    ueber_sabr = any("/sabr/" in u for u in segment_uris)
    r.append(("SABR-toten Videos wird nicht aus /sabr ausgeliefert",
              not (routing.get("sabr_tot") and not sitzung.get("hat_cache")
                   and ueber_sabr),
              "sabr_tot=%s hat_cache=%s ueber_sabr=%s"
              % (routing.get("sabr_tot"), sitzung.get("hat_cache"), ueber_sabr)))

    # Storm heisst "aus /sabr bedienen". Zusammen mit sabr_tot und ohne Cache
    # ist das ein Widerspruch, den keine Ebene mehr aufloest.
    r.append(("storm und sabr_tot widersprechen sich nicht ohne Cache",
              not (routing.get("storm") and routing.get("sabr_tot")
                   and not sitzung.get("hat_cache")),
              "storm=%s sabr_tot=%s hat_cache=%s"
              % (routing.get("storm"), routing.get("sabr_tot"),
                 sitzung.get("hat_cache"))))

    # Eine geltende Marke muss juenger als ihre Frist sein. Haelt eine laenger,
    # klebt das Video dauerhaft an einem Sonderweg.
    for name, frist in MARKEN_FRISTEN_MS.items():
        alter = routing.get("%s_datei_alter_ms" % name, -1)
        if routing.get(name) and alter >= 0:
            r.append(("Marke %s liegt innerhalb ihrer Frist" % name,
                      alter < frist,
                      "%.1f h alt, Frist %.0f h" % (alter / 3600_000.0,
                                                    frist / 3600_000.0)))

    # Kein Fehler, aber immer melden: der globale Schalter betrifft ALLE Videos.
    memo = marken.get("global", {}).get("web_memo_rest_ms", -1)
    if memo and memo > 0:
        r.append(("globales WEB-Memo ist nicht aktiv", False,
                  "noch %.0f s — betrifft ALLE Videos, nicht nur dieses"
                  % (memo / 1000.0)))
    return r


# ---------------------------------------------------------------- Selbsttest


def _playlist(segmente, itag, start=1000):
    """Kuenstliche Medien-Playlist: [(dauer, laenge)] -> HLS-Text."""
    url = "https://x.googlevideo.com/videoplayback?itag=%d&mime=video%%2Fmp4" % itag
    zeilen = ["#EXTM3U", "#EXT-X-VERSION:7", "#EXT-X-TARGETDURATION:10",
              "#EXT-X-MEDIA-SEQUENCE:0",
              '#EXT-X-MAP:URI="%s",BYTERANGE="740@0"' % url]
    versatz = start
    for dauer, laenge in segmente:
        zeilen.append("#EXTINF:%.3f," % dauer)
        zeilen.append("#EXT-X-BYTERANGE:%d@%d" % (laenge, versatz))
        zeilen.append(url)
        versatz += laenge
    zeilen.append("#EXT-X-ENDLIST")
    return "\n".join(zeilen)


def _spur(name, segmente, itag, start=1000):
    return Spur(name, _playlist(segmente, itag, start), "http://x/")


def selbsttest():
    """Faengt die Suite die Fehler, fuer die sie geschrieben wurde?

    Eine Suite, die nur gruen ist, beweist nichts. Hier laufen dieselben
    Ausdruecke (regeln_playlist) gegen kuenstlich kaputte Playlisten; jeder
    Fall ist ein echter Fehler aus der Historie. Erwartet wird, dass genau
    die benannte Regel ANSCHLAEGT.
    """
    gesund_bild = [(5.0, 100000)] * 60      # 300 s
    gesund_ton = [(10.0, 20000)] * 30       # 300 s, andere Segmentierung

    faelle = [
        ("2026-08-02: Ton-Playlist zeigte aufs Bild (schwarzer Frame, kein Ton, "
         "keine Fehlermeldung)",
         _spur("Ton", gesund_bild, 137), _spur("Bild", gesund_bild, 137),
         "audio.m3u8 verweist auf einen Audio-itag"),

        ("0f746d7: fehlender sidx -> EIN Segment ueber die volle Laufzeit, "
         "AVPlayer kann nicht springen",
         _spur("Ton", gesund_ton, 140), _spur("Bild", [(300.0, 6000000)], 137),
         "Bild: mehr als ein Segment (springbar)"),

        ("Ton faellt zu kurz aus -> Wiedergabe endet vorzeitig",
         _spur("Ton", [(10.0, 20000)] * 10, 140), _spur("Bild", gesund_bild, 137),
         "Ton und Bild decken dieselbe Laufzeit"),
    ]

    # Loch in den Byte-Bereichen: Segment 3 beginnt 500 B zu spaet.
    loch_text = _playlist(gesund_bild, 137).replace(
        "#EXT-X-BYTERANGE:100000@201000", "#EXT-X-BYTERANGE:100000@201500", 1)
    faelle.append((
        "Byte-Bereiche mit Loch -> Player bekommt Muell an der Nahtstelle",
        _spur("Ton", gesund_ton, 140), Spur("Bild", loch_text, "http://x/"),
        "Bild: Byte-Bereiche schliessen luecken- und ueberlappungsfrei an"))

    print("Selbsttest: schlaegt die Suite bei bekannten Fehlern an?\n")
    schlecht = 0

    # Gegenprobe: die gesunde Kombination darf NICHTS ausloesen.
    verletzt = [r for r, ok, _ in regeln_playlist(
        _spur("Ton", gesund_ton, 140), _spur("Bild", gesund_bild, 137), 300.0) if not ok]
    if verletzt:
        schlecht += 1
        print("  FEHLALARM  gesunde Playlisten verletzen: %s" % ", ".join(verletzt))
    else:
        print("  ok         gesunde Playlisten loesen nichts aus")

    for beschreibung, ton, bild, erwartete_regel in faelle:
        ergebnis = regeln_playlist(ton, bild, 300.0)
        getroffen = [r for r, ok, _ in ergebnis if not ok]
        if erwartete_regel in getroffen:
            print("  ok         %s\n             -> \"%s\" schlaegt an"
                  % (beschreibung, erwartete_regel))
        else:
            schlecht += 1
            print("  NICHT GEFANGEN  %s\n             erwartet: \"%s\", verletzt: %s"
                  % (beschreibung, erwartete_regel, getroffen or "nichts"))

    # --- Markenregeln, gleiche Bauart
    gesunde_marken = {
        "routing": {"throttled": False, "webembed": False, "sabr_tot": False,
                    "storm": False, "throttled_datei_alter_ms": -1,
                    "webembed_datei_alter_ms": -1, "sabr_tot_datei_alter_ms": -1},
        "sitzung": {"hat_cache": True},
        "sprung": {},
        "global": {"web_memo_rest_ms": -1},
    }
    direkt = ["https://x.googlevideo.com/videoplayback?itag=137"]
    ueber_sabr = ["/sabr/VID/137"]

    def mit(pfad_werte, gruppe="routing"):
        import copy
        m = copy.deepcopy(gesunde_marken)
        m[gruppe].update(pfad_werte)
        return m

    marken_faelle = [
        ("2026-08-02: sabr_tot + kein Cache, trotzdem aus /sabr bedient "
         "(HTTP 500 nach 12 s)",
         mit({"sabr_tot": True, "sabr_tot_datei_alter_ms": 60_000}),
         ueber_sabr, "SABR-toten Videos wird nicht aus /sabr ausgeliefert",
         {"sitzung": {"hat_cache": False}}),

        ("storm und sabr_tot gleichzeitig, ohne Cache — Widerspruch, den keine "
         "Ebene aufloest",
         mit({"storm": True, "sabr_tot": True, "sabr_tot_datei_alter_ms": 60_000}),
         direkt, "storm und sabr_tot widersprechen sich nicht ohne Cache",
         {"sitzung": {"hat_cache": False}}),

        ("Marke gilt jenseits ihrer Frist — Video klebt dauerhaft am Sonderweg",
         mit({"throttled": True, "throttled_datei_alter_ms": 30 * 3600_000}),
         direkt, "Marke throttled liegt innerhalb ihrer Frist", {}),

        ("globales WEB-Memo aktiv — betrifft ALLE Videos",
         mit({"web_memo_rest_ms": 300_000}, "global"),
         direkt, "globales WEB-Memo ist nicht aktiv", {}),
    ]

    verletzt = [r for r, ok, _ in regeln_marken(gesunde_marken, direkt) if not ok]
    if verletzt:
        schlecht += 1
        print("  FEHLALARM  gesunder Markenzustand verletzt: %s" % ", ".join(verletzt))
    else:
        print("  ok         gesunder Markenzustand loest nichts aus")

    for beschreibung, marken, uris, erwartete_regel, nachtrag in marken_faelle:
        for gruppe, werte in nachtrag.items():
            marken[gruppe].update(werte)
        getroffen = [r for r, ok, _ in regeln_marken(marken, uris) if not ok]
        if erwartete_regel in getroffen:
            print("  ok         %s\n             -> \"%s\" schlaegt an"
                  % (beschreibung, erwartete_regel))
        else:
            schlecht += 1
            print("  NICHT GEFANGEN  %s\n             erwartet: \"%s\", verletzt: %s"
                  % (beschreibung, erwartete_regel, getroffen or "nichts"))

    gesamt = len(faelle) + len(marken_faelle) + 2
    print("\n%d von %d Faellen wie erwartet" % (gesamt - schlecht, gesamt))
    return 1 if schlecht else 0


# ---------------------------------------------------------------- Vergleich


def vergleiche(basis_a, basis_b, video_id):
    """Zwei Implementierungen gegeneinander: strukturell, nicht byteweise.

    URLs unterscheiden sich zwangslaeufig (frische cpn, andere Egress-IP), die
    SEGMENTIERUNG darf es nicht: gleiche Segmentzahl je Spur, gleiche
    Byte-Bereiche, gleicher itag-Typ.
    """
    abweichungen = []
    for pfad, name in (("audio.m3u8", "Ton"), (None, "Bild")):
        spuren = []
        for basis in (basis_a, basis_b):
            master = hole_text("%s/synth-hls/%s/master.m3u8" % (basis, video_id))
            if pfad is None:
                v = [z.strip() for z in master.splitlines()
                     if z.strip() and not z.startswith("#")]
                if not v:
                    return ["%s: master ohne Variante (%s)" % (name, basis)]
                p = v[0]
            else:
                p = pfad
            spuren.append(spur_laden(basis, video_id,
                                     "synth-hls/%s/%s" % (video_id, p), name))
        a, b = spuren
        if len(a.segmente) != len(b.segmente):
            abweichungen.append("%s: %d gegen %d Segmente"
                                % (name, len(a.segmente), len(b.segmente)))
        elif [(s[1], s[2]) for s in a.segmente] != [(s[1], s[2]) for s in b.segmente]:
            abweichungen.append("%s: Byte-Bereiche weichen ab" % name)
        if (a.itag in AUDIO_ITAGS) != (b.itag in AUDIO_ITAGS):
            abweichungen.append("%s: itag-Art weicht ab (%s gegen %s)"
                                % (name, a.itag, b.itag))
    return abweichungen


# ---------------------------------------------------------------- main


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--base", default="http://127.0.0.1:8881",
                   help="zu pruefende Implementierung (Standard: Java :8881)")
    p.add_argument("--vergleich", metavar="URL",
                   help="zweite Implementierung; prueft zusaetzlich auf Gleichheit")
    p.add_argument("--videos", nargs="+", metavar="ID",
                   help="videoIds statt der Standardauswahl")
    p.add_argument("-v", "--ausfuehrlich", action="store_true",
                   help="auch erfuellte Regeln zeigen")
    p.add_argument("--selbsttest", action="store_true",
                   help="ohne Netz: faengt die Suite die historischen Fehler?")
    args = p.parse_args()

    if args.selbsttest:
        return selbsttest()

    basis = args.base.rstrip("/")
    videos = ([(v, "eigene Auswahl") for v in args.videos]
              if args.videos else STANDARD_VIDEOS)

    print("Konformitaetssuite gegen %s" % basis)
    print("%d Video(s)\n" % len(videos))

    lauf = Lauf(args.ausfuehrlich)
    for video_id, klasse in videos:
        try:
            pruefe_video(basis, video_id, klasse, lauf)
        except Fehler as e:
            lauf.fehler.append((video_id, "Video ueberhaupt abrufbar", str(e)))
            print("      FEHLER Video ueberhaupt abrufbar — %s" % e)
        except Exception as e:                       # noqa: BLE001
            lauf.fehler.append((video_id, "unerwarteter Abbruch",
                                "%s: %s" % (type(e).__name__, e)))
            print("      FEHLER unerwarteter Abbruch — %s: %s" % (type(e).__name__, e))
        print()

    if args.vergleich:
        print("Vergleich mit %s" % args.vergleich)
        for video_id, _ in videos:
            try:
                ab = vergleiche(basis, args.vergleich.rstrip("/"), video_id)
            except Exception as e:                   # noqa: BLE001
                ab = ["Vergleich abgebrochen: %s" % e]
            if ab:
                for a in ab:
                    print("      ABWEICHUNG %s: %s" % (video_id, a))
                    lauf.fehler.append((video_id, "Vergleich", a))
            else:
                lauf.ok += 1
                print("      ok   %s deckungsgleich" % video_id)
        print()

    print("%d Regeln erfuellt, %d verletzt, %d uebersprungen"
          % (lauf.ok, len(lauf.fehler), lauf.uebersprungen))
    if lauf.fehler:
        print("\nVerletzt:")
        for kontext, regel, detail in lauf.fehler:
            print("  %-24s %s%s" % (kontext, regel, (" — " + detail) if detail else ""))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
