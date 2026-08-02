# Konformitätssuite (Phase 3b des Go-Ports)

Vertragsprüfung des yt-Backends **über HTTP**. Sie kennt die Implementierung
nicht und läuft deshalb heute gegen den Java-Stack und später unverändert
gegen die Go-Binary. Keine Abhängigkeiten außer der Standardbibliothek.

```sh
python3 konformitaet.py                                  # gegen :8881
python3 konformitaet.py --base http://127.0.0.1:8891     # gegen die Go-Binary
python3 konformitaet.py --vergleich http://127.0.0.1:8891 # Java gegen Go
python3 konformitaet.py --videos VID1 VID2 -v
python3 konformitaet.py --selbsttest                     # ohne Netz
```

Exit 0 = alle Regeln erfüllt, Exit 1 = mindestens eine verletzt. Damit
taugt sie als Cutover-Gate und als Cron.

## Warum nicht Unit-Tests

Java-Unit-Tests überleben den Port nicht — die Suite soll genau das. Sie
prüft nur, was von außen sichtbar ist: Playlisten, Byte-Bereiche, Status-
codes, gelieferte Längen.

## Die Regeln

Jede stammt aus einem echten Fehler. Ohne den Fehler ist nicht
nachvollziehbar, warum sie so scharf formuliert ist — deshalb steht er im
Code jeweils daneben.

| Regel | Fehler dahinter |
|---|---|
| `audio.m3u8` verweist auf einen Audio-itag | 2026-08-02: Format-Suche kannte nur `videoStreams` → Tonspur zeigte auf itag 137. Schwarzer Frame, kein Ton, **keine Fehlermeldung**, beide Seiten HTTP 200 |
| Ton und Bild sind verschiedene Formate | dito |
| Ton und Bild decken dieselbe Laufzeit | Segmentzahlen dürfen abweichen (182 gegen 345), die Summe nicht — sonst endet die Wiedergabe vorzeitig |
| mehr als ein Segment (springbar) | `0f746d7`: fehlender sidx → EIN Segment über die volle Laufzeit, AVPlayer kann nicht springen |
| Byte-Bereiche lückenlos | Nahtstellen-Müll; am Statuscode nicht erkennbar |
| Init/Segment kommt als 206 in **exakter** Länge | `serveFile` las range-los die ganze Datei → OOM bei 662-MB-Downloads |
| Sprung ins Ungeladene liefert 206 | 2026-08-02: Zielzeit aus **mittlerer** Segmentdauer (echte 3921–6798 ms) → Server begann ein Segment zu spät, 503 nach 20 s |
| Playlistlaufzeit passt zur gemeldeten Dauer | abgeschnittene Playlisten |

## Selbsttest

Eine Suite, die nur grün ist, beweist nichts. `--selbsttest` lässt **dieselben
Ausdrücke** (`regeln_playlist`) gegen künstlich kaputte Playlisten laufen und
verlangt, dass genau die benannte Regel anschlägt — plus eine Gegenprobe, dass
gesunde Playlisten nichts auslösen. Läuft ohne Netz und ohne Backend.

## Markenregeln

Braucht `/debug/marken/<videoId>`. Fehlt der Endpunkt, werden diese Regeln
übersprungen (einmalig gemeldet) — eine Implementierung ohne ihn fällt damit
nicht durch, verliert aber die Prüfung.

| Regel | Fehler dahinter |
|---|---|
| `sabr_tot` + kein Cache ⇒ **nicht** aus `/sabr` ausliefern | 2026-08-02: genau diese Kombination stand an. Der Player wartet auf Daten, die nie entstehen → HTTP 500 nach 12 s |
| `storm` + `sabr_tot` ohne Cache ist ein Widerspruch | „aus /sabr bedienen" gegen „SABR liefert nichts" — keine Ebene löst das auf |
| geltende Marke ist jünger als ihre Frist | sonst klebt das Video dauerhaft am Sonderweg |
| globales WEB-Memo wird gemeldet | es betrifft **alle** Videos, nicht nur das geprüfte |

## Stand

2026-08-02 gegen Java :8881: **113 Regeln erfüllt, 0 verletzt**, 1
übersprungen (Sprungtest bei einem 19-Sekunden-Video). Selbsttest 10 von 10,
davon 5 Markenfälle.

⚠️ Das heißt nicht, dass die Suite vollständig ist. Sie deckt die
Playback-Kette ab, **nicht** Browse/Metadaten (Phase 2) und nicht den
User-State (Phase 4). Und sie prüft Verhalten, nicht Leistung — der Erstaufruf
darf 12 s dauern und gilt trotzdem als erfüllt.

## Wenn eine Regel fehlschlägt

Erst prüfen, ob das Backend gerade unter Last steht. Am 2026-08-02 fiel die
Erfolgsquote eines Tests von 4/4 auf 3/6, weil rund 40 Container-Neustarts in
kurzer Zeit die Token-Prägung von 386 ms auf 19,6 s trieben (Load 3,74). Nach
drei Minuten Ruhe war wieder alles grün. `uptime` vor der Diagnose.
