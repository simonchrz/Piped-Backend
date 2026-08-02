package me.kavin.piped.utils.sabr;

import com.fasterxml.jackson.databind.JsonNode;
import me.kavin.piped.consts.Constants;
import me.kavin.piped.utils.BgPoTokenProvider;
import org.schabi.newpipe.extractor.services.youtube.PoTokenResult;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/// SABR resolve helper: fetches the ANDROID player response for a video and runs
/// one SabrSession, returning the reassembled fmp4 bytes for the forced audio
/// (140 m4a) + video (137 avc 1080p). The serving layer (SabrCache) calls this.
public final class SabrHandlers {

    /// Bedarfssteuerung der Sessions. SabrCache setzt beides (dort liegt das
    /// Wissen, was der Player angefordert hat und was auf Platte liegt); hier
    /// nur durchgereicht, damit die sabr-Schicht nicht auf die Cache-Schicht
    /// zeigen muss. STOP = Sitzung beenden (niemand schaut mehr zu),
    /// PAUSE = genug Vorlauf, diese Runde nicht fragen.
    public static volatile java.util.function.Function<String, java.util.function.BooleanSupplier> SESSION_STOP;
    public static volatile java.util.function.Function<String, java.util.function.BooleanSupplier> SESSION_PAUSE;
    /// Sprungziel (Segmentnummer) je Video — gesetzt von SabrCache, wenn der
    /// Player Bytes anfordert, die noch fehlen.
    public static volatile java.util.function.Function<String, java.util.function.IntSupplier> SESSION_SEEK;

    private static final String ANDROID_UA =
            "com.google.android.youtube/20.10.38 (Linux; U; Android 14) gzip";
    private static final String ANDROID_VR_UA =
            "com.google.android.apps.youtube.vr.oculus/1.62.27 (Linux; U; Android 12L; eureka-user Build/SQ3A.220605.009.A1) gzip";
    private static final String WEB_UA =
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";
    private static final String WEB_EMBEDDED_VERSION = "1.20260122.01.00";
    /// ⚠️ Am 2026-07-31 meldete ein echter Chromium 2.20260731.00.00 — unsere
    /// Konstante war ein halbes Jahr alt. Der Wert steht auch im att/get-Kontext
    /// und im Präge-Kontext; alle drei müssen zusammenpassen.
    private static final String WEB_VERSION = "2.20260731.00.00";

    /// Cheap viability probe for the storm fallback: ONE ANDROID player call.
    /// SABR is viable when it answers with a serverAbrStreamingUrl — the media
    /// transport itself is verified later by the actual download (ensureFile).
    /// Bounded by androidPlayer's own timeouts; any failure = not viable.
    /// ⚠️ Mit DEM Client proben, den die Leiter auch benutzt.
    ///
    /// Die Probe fragte den ANDROID-Client an — anonym, ohne Token — waehrend
    /// der Standard laengst WEB ist. Im Drosselfenster scheitert der
    /// ANDROID-Aufruf, also meldeten wir „SABR tot" und gaben 503 zurueck,
    /// obwohl der WEB-Pfad lieferte: gemessen 2026-07-31 an S8UJrXGlGmg —
    /// /streams antwortete 503, ein direkter /sabr-Abruf 206 in 2,6 s.
    /// Erst WEB (mit visitorData + Attestierung wie die echte Sitzung),
    /// dann ANDROID als Rueckfall.
    public static boolean sabrViable(String videoId) {
        String visitorData = null, attest = null;
        try {
            final BgPoTokenProvider bg = BgPoTokenProvider.instance();
            if (bg != null) {
                final PoTokenResult pot = bg.sabrSessionPoToken();
                if (pot != null) {
                    visitorData = pot.visitorData;
                    attest = pot.playerRequestPoToken;
                }
            }
            if (webPlayer(videoId, visitorData, null, attest)
                    .path("streamingData").path("serverAbrStreamingUrl").asText(null) != null)
                return true;
        } catch (Exception ignored) {
            // WEB nicht verfuegbar -> ANDROID probieren
        }
        try {
            return androidPlayer(videoId, null, null, null, false)
                    .path("streamingData").path("serverAbrStreamingUrl")
                    .asText(null) != null;
        } catch (Exception e) {
            return false;
        }
    }

    /// Result of one SABR session: which itags the format picker actually chose
    /// (NOT always 140/137 — videos without 1080p avc fall back to the first
    /// matching format), plus the session outcome so SabrCache can decide on a
    /// family-retry (SABR_ERROR/empty) or a later refill (incomplete). Media
    /// bytes are streamed to the caller's Sink, not returned — a full-length
    /// video is ~1 GB and must not live in RAM.
    public record SabrMedia(int audioItag, int videoItag,
                            boolean complete, String stopReason, long segments) {}

    public static SabrMedia runSession(String videoId, SabrSession.Sink sink) throws Exception {
        return runSession(videoId, sink, null, false);
    }

    /// family: explicit egress family ("v4"/"v6") for BOTH the ANDROID player
    /// resolve and the SABR session — gvs URLs are IP-signed, so a mismatch
    /// between resolve- and stream-family 403s. null = global ACTIVE family.
    /// contentBoundToken: mint a videoId-content-bound po_token for the ABR
    /// streamerContext instead of the pooled visitorData-bound one. Kids
    /// content ignores the visitor-bound token (session stalls at the ~60s
    /// readahead window despite the token being present); content binding is
    /// the same shape the direct-URL path mints per video (pot= param).
    public static SabrMedia runSession(String videoId, SabrSession.Sink sink,
                                       String family, boolean contentBoundToken) throws Exception {
        return runSession(videoId, sink, family, contentBoundToken, false);
    }

    /// clientMode 1 = ANDROID_VR (client 28), 2 = WEB_EMBEDDED_PLAYER (client
    /// 56, the client that verifiably SERVES kids content — and the client the
    /// pooled visitor-bound web po_token was minted FOR; kids gvs appears to
    /// enforce client<->token consistency), else ANDROID. Ladder rungs when the
    /// readahead stalls at one window.
    public static SabrMedia runSession(String videoId, SabrSession.Sink sink,
                                       String family, boolean contentBoundToken,
                                       boolean vrClient) throws Exception {
        return runSession(videoId, sink, family, contentBoundToken, vrClient ? 1 : 0);
    }

    public static SabrMedia runSession(String videoId, SabrSession.Sink sink,
                                       String family, boolean contentBoundToken,
                                       int clientMode) throws Exception {
        return runSession(videoId, sink, family, contentBoundToken, clientMode, false, null);
    }

    public static SabrMedia runSession(String videoId, SabrSession.Sink sink,
                                       String family, boolean contentBoundToken,
                                       int clientMode, boolean paced, Runnable publishHook) throws Exception {
        return runSession(videoId, sink, family, contentBoundToken, clientMode, paced, publishHook, null, null);
    }

    /// paced: 1x-Echtzeit-Session (SabrSession.fetchAll paced) für den Kids-
    /// Readahead-Cap; publishHook läuft pro Runde nach dem Disk-Flush.
    /// resume: was schon auf Platte liegt (fortsetzen ab Segment N+1);
    /// progress: Fortschrittsmeldung, aus der die Cache-Schicht ihren
    /// Fortsetz-Zustand schreibt.
    public static SabrMedia runSession(String videoId, SabrSession.Sink sink,
                                       String family, boolean contentBoundToken,
                                       int clientMode, boolean paced, Runnable publishHook,
                                       java.util.Map<Integer, SabrSession.Resume> resume,
                                       SabrSession.ProgressSink progress) throws Exception {
        final boolean vrClient = clientMode == 1;
        final boolean webClient = clientMode == 2;
        final boolean pureWeb = clientMode == 3;
        // A pooled visitorData-bound po_token authorizes the gvs streaming session.
        // Without it googlevideo caps the SABR readahead at ~60s (one buffer window)
        // then stops. visitorData goes into the player context; the po_token bytes
        // go into every ABR streamerContext (SabrSession field 2).
        final BgPoTokenProvider bg = BgPoTokenProvider.instance();
        String visitorData = null;
        byte[] poToken = null;
        String attestationPoToken = null;
        if (bg != null) {
            final PoTokenResult pot = bg.sabrSessionPoToken();
            if (pot != null) {
                visitorData = pot.visitorData;
                if (pot.playerRequestPoToken != null) poToken = b64(pot.playerRequestPoToken);
            }
            // ⚠️ BINDUNGEN NICHT VERTAUSCHEN (yt-dlp PO-Token-Guide):
            //   • Player-Aufruf  -> Token an die VIDEO-ID gebunden
            //   • Streaming/GVS  -> Token an die SITZUNG gebunden
            //     (datasyncId wenn angemeldet, sonst visitorData)
            // Wir hatten beides ueber einen Kamm geschoren: derselbe
            // sitzungsgebundene Token diente als Player-Attestierung, und im
            // content-bound-Rung wanderte ausgerechnet der VIDEO-gebundene in
            // den streamerContext — also genau falsch herum. Der Server duldet
            // so etwas 1-2 MB und schaltet dann auf "Attestierung erforderlich"
            // (prot=2 -> 3 bei ~61 s). Beleg, dass es nicht am Inhalt liegt:
            // derselbe Anschluss, dasselbe Video, ein echter Browser puffert
            // 148 s (2026-07-31 per Chrome-Erweiterung gemessen).
            // Kill-Switch: YT_POT_SPLIT_BINDING=0 stellt das alte Verhalten her.
            // 🏁 DIE LOESUNG DES 60-SEKUNDEN-DECKELS (2026-08-01).
            //
            // Der GVS-Token (streamerContext) muss an die VIDEO-ID gebunden
            // sein. Genau das verhinderte `splitBinding`: es gab dem
            // Player-Aufruf einen videoId-gebundenen Token und liess den
            // GVS-Token sitzungsgebunden ("bleibt der sitzungsgebundene") —
            // damit sah der Server nie ein gueltiges Token fuer den Stream und
            // meldete prot=2, bis er bei ~61 s auf prot=3 ging.
            //
            // Beleg aus der Referenz-Implementierung (LuanRT/googlevideo), zwei
            // Meldungen mit exakt unserem Symptom:
            //   * Issue #38, vom Autor: "requires the web client to use content
            //     bound PO tokens — changing the binding to videoId should fix it"
            //   * Issue #45: "caused when the used potoken is WRONG. Using
            //     potoken generation with getAttestationChallenge(
            //     'ENGAGEMENT_TYPE_UNBOUND') makes the stream no longer cut off
            //     after 60 seconds"
            //
            // Gemessen mit videoId-Bindung + eigener Aufgabe: poToken=93B,
            // prot=1 ab Runde 2, Play-Head 78 s / 96 s / 115 s, Cache 54 statt
            // 12 Segmenten. Vorher: prot=2, Abbruch bei 61,3 s, immer Segment 12.
            //
            // YT_POT_SPLIT_BINDING=1 stellt das alte Verhalten wieder her.
            final boolean splitBinding = "1".equals(System.getenv("YT_POT_SPLIT_BINDING"));
            if (splitBinding) {
                final String perVideo = bg.sabrPoTokenForBinding(videoId);
                if (perVideo != null) attestationPoToken = perVideo;   // Player-Aufruf
                // poToken (streamerContext/GVS) bleibt der sitzungsgebundene.
            } else {
                // Ohne splitBinding IMMER content-bound — der Aufrufer-Schalter
                // `contentBoundToken` war nur fuer die alte Leiter gedacht.
                if (true) {
                final String cb = bg.sabrContentBoundPoToken(videoId);
                if (cb != null) { poToken = b64(cb); attestationPoToken = cb; }
                else System.out.println("[Sabr] " + videoId
                        + " content-bound mint failed -> keeping visitor-bound token");
                }
            }
        }
        // ⚠️ ÜBERSTEUERUNG fuer den Attestierungs-Test: Token UND Sitzungskennung
        // stammen dann aus einer echten Browser-Sitzung (pot-browser/capture-pot.js
        // praegt sie mit unseren Cookies in einem headless Chromium). Genau diese
        // Paarung fehlt uns sonst — unser Token kommt aus dem bgutil-Container mit
        // EIGENER BotGuard-Sitzung, und YouTube duldet den fremden Ausweis nur
        // ~1-2 MB (STREAM_PROTECTION_STATUS 2->3 bei ~61 s).
        final String potOverride = System.getenv("YT_SABR_POT_B64");
        if (potOverride != null && !potOverride.isBlank()) {
            try {
                poToken = java.util.Base64.getDecoder().decode(potOverride.trim());
            } catch (IllegalArgumentException e) {
                poToken = java.util.Base64.getUrlDecoder().decode(potOverride.trim());
            }
            final String visitorOverride = System.getenv("YT_SABR_VISITOR");
            if (visitorOverride != null && !visitorOverride.isBlank()) visitorData = visitorOverride;
            System.out.println("[Sabr] " + videoId + " ÜBERSTEUERT: Browser-Token "
                    + poToken.length + "B + zugehoerige Sitzungskennung");
        }
        System.out.println("[Sabr] " + videoId + " runSession poToken="
                + (poToken != null ? poToken.length + "B" : "NONE")
                + (contentBoundToken ? " (content-bound)" : "")
                + " visitor=" + (visitorData != null ? "yes" : "no"));

        // WEB mode always attests the player call with the pooled web token —
        // that pairing is the whole point of the rung.
        if (webClient && attestationPoToken == null && bg != null) {
            final PoTokenResult pot2 = bg.sabrSessionPoToken();
            if (pot2 != null) attestationPoToken = pot2.playerRequestPoToken;
        }
        if (pureWeb && attestationPoToken == null && bg != null) {
            final PoTokenResult p3 = bg.sabrSessionPoToken();
            if (p3 != null) attestationPoToken = p3.playerRequestPoToken;
        }
        // ⚠️ VOLLES SITZUNGS-TRANSPLANTAT (Test): abrUrl + ustreamerConfig aus
        // der Browser-Sitzung uebernehmen. Die Duldungsgrenze liegt immer bei
        // exakt 61,1 s / 12 Segmenten — unabhaengig vom Token. Das deutet darauf
        // hin, dass sie an der SITZUNG haengt, und die entsteht im Player-Aufruf.
        final String abrOverride = System.getenv("YT_SABR_ABRURL");
        final String ustOverride = System.getenv("YT_SABR_UST");
        final boolean transplant = abrOverride != null && !abrOverride.isBlank()
                && ustOverride != null && !ustOverride.isBlank();

        final JsonNode player = pureWeb
                ? spielerAntwort(videoId, visitorData, family, attestationPoToken)
                : webClient
                ? webEmbedPlayer(videoId, visitorData, family, attestationPoToken)
                : androidPlayer(videoId, visitorData, family, attestationPoToken, vrClient);
        final JsonNode sd = player.path("streamingData");
        String abrUrl = sd.path("serverAbrStreamingUrl").asText(null);
        String ustB64 = findFirst(player, "videoPlaybackUstreamerConfig");
        if (transplant) {
            abrUrl = abrOverride.trim();
            ustB64 = ustOverride.trim();
            System.out.println("[Sabr] " + videoId + " SITZUNGS-TRANSPLANTAT: abrUrl "
                    + abrUrl.length() + " Zeichen, ustreamerConfig " + ustB64.length() + " Zeichen");
        }
        if (abrUrl == null || ustB64 == null) {
            throw new IllegalStateException("video " + videoId + " has no SABR streaming url "
                    + "(status=" + player.path("playabilityStatus").path("status").asText() + ")");
        }
        // ⚠️ Wir pinnen ein ALTFORMAT-Paar: AAC 140 + H.264 137. Ein echter
        // Chromium fordert bei Made-for-Kids weder das eine noch das andere an —
        // er schickt einen ganzen Katalog moderner Formate (Opus 250/251,
        // VP9 242-248/278, AV1 394-399) und laesst den Server waehlen
        // (Feldvergleich der ersten Runde, 2026-08-01). Verdacht: fuer
        // Kids-Inhalte sind nur die modernen Formate voll freigegeben.
        // YT_SABR_ITAG_A / YT_SABR_ITAG_V stellen die Wunsch-itags um.
        // ⚠️ Zweitwunsch merken: bricht die Sitzung mit `sabr.no_audio_selected`
        // ab, ist unser Audioformat fuer DIESE Sitzung nicht waehlbar — dann
        // hilft nur ein anderes. Gemessen 2026-08-01 an hxOApe1P9dM und
        // WRVsOCh907o: itag 140 (mit deutlich aelterem lmt als das Video) wurde
        // abgelehnt, die Sitzung lieferte gar nichts.
        final int wunschA = ALT_AUDIO.get() != null ? ALT_AUDIO.get()
                : wunschItag("YT_SABR_ITAG_A", 140);
        final JsonNode aud = pickFormat(sd, "audio", wunschA);
        final JsonNode vid = pickFormat(sd, "video", wunschItag("YT_SABR_ITAG_V", 137));
        final byte[] clientInfo;
        final String ua;
        if (pureWeb) {
            // ⚠️ VOLLE Form. Wir schickten hier nur clientName+Version (22 B).
            // Ein echter Web-Player schickt 68 B — Sprache, Gerät und Betriebs-
            // system gehören dazu (Feldvergleich gegen einen echten Chromium-
            // Mitschnitt, 2026-07-31: Feld 19.1 wir 22 B vs. Chromium 68 B).
            // Der po_token wird gegen DIESE Client-Angaben geprüft; ein Rumpf-
            // Block ist der wahrscheinlichste Grund, warum googlevideo uns nach
            // ~60 s auf STREAM_PROTECTION_STATUS 3 setzt. Die Werte müssen zur
            // WEB_UA passen — UA sagt Windows, also sagt client_info Windows.
            clientInfo = new ProtoWriter()
                    .stringField(1, "de_DE")
                    .varintField(16, 1).stringField(17, WEB_VERSION)
                    .stringField(18, "Windows").stringField(19, "10.0")
                    .toByteArray();
            ua = WEB_UA;
        } else if (webClient) {
            clientInfo = new ProtoWriter()
                    .varintField(16, 56).stringField(17, WEB_EMBEDDED_VERSION)
                    .toByteArray();
            ua = WEB_UA;
        } else if (vrClient) {
            clientInfo = new ProtoWriter()
                    .varintField(16, 28).stringField(17, "1.62.27")
                    .stringField(18, "Android").stringField(19, "12L")
                    .varintField(64, 32).toByteArray();
            ua = ANDROID_VR_UA;
        } else {
            clientInfo = new ProtoWriter()
                    .varintField(16, 3).stringField(17, "20.10.38")
                    .stringField(18, "Android").stringField(19, "14")
                    .varintField(64, 34).toByteArray();
            ua = ANDROID_UA;
        }
        final SabrSession.Fmt pa = new SabrSession.Fmt(aud.path("itag").asInt(), aud.path("lastModified").asLong());
        final SabrSession.Fmt pv = new SabrSession.Fmt(vid.path("itag").asInt(), vid.path("lastModified").asLong());
        // Was bietet DIESER Client an, und was haben wir daraus gewaehlt? Der
        // WEB-Pfad lieferte 105B->11B ohne Medien und ohne Fehler — genau das
        // Bild, das entsteht, wenn die format_id (itag+lmt) nicht zu etwas passt,
        // das der Server hat. `lastModified` fehlt in manchen Antworten; ein
        // lmt=0 macht die format_id unauffindbar, ohne dass jemand meckert.
        if ("1".equals(System.getenv("YT_SABR_TRACE"))) {
            final StringBuilder fl = new StringBuilder();
            int n = 0;
            for (JsonNode f : sd.path("adaptiveFormats")) {
                if (n++ < 8) fl.append(' ').append(f.path("itag").asInt())
                        .append('/').append(f.path("lastModified").asLong(-1))
                        .append(f.has("url") ? "u" : "-");
            }
            System.out.println("[Sabr] " + videoId + " client=" + (pureWeb ? "WEB" : webClient
                    ? "WEB_EMBEDDED" : vrClient ? "ANDROID_VR" : "ANDROID")
                    + " formate=" + n + " gewaehlt a=" + pa.itag + "/" + pa.lmt
                    + " v=" + pv.itag + "/" + pv.lmt + " ersten:" + fl);
        }
        // Alle angebotenen Formate als Kandidaten sammeln (s. SabrSession).
        final java.util.List<SabrSession.Fmt> candA = new java.util.ArrayList<>();
        final java.util.List<SabrSession.Fmt> candV = new java.util.ArrayList<>();
        for (JsonNode f : sd.path("adaptiveFormats")) {
            final String mime = f.path("mimeType").asText("");
            final SabrSession.Fmt fm = new SabrSession.Fmt(
                    f.path("itag").asInt(), f.path("lastModified").asLong());
            if (mime.startsWith("audio")) candA.add(fm);
            else if (mime.startsWith("video")) candV.add(fm);
        }
        // maxIterations bumped 500 -> 8000: a full-length video needs one round per
        // buffer window (~5-10s), so ~55min = several hundred rounds. The loop still
        // breaks early on complete()/stuck; 8000 is just a runaway ceiling.
        // ⚠️ nsig auf die ABR-URL anwenden (Browser-Mitschnitt 2026-07-25): der echte
        // Web-Player schickt den `n`-Parameter ENTSCHLUESSELT — in der Player-Antwort
        // 16 Zeichen, im tatsaechlichen Request 14, nachweislich verschieden. Ohne
        // diesen Schritt drosselt/403t googlevideo den ABR-POST. Dieselbe
        // Behandlung bekommen unsere Direkt-URLs laengst (URLUtils.rewriteVideoURL).
        final String abrUrlN = me.kavin.piped.utils.NSigClient.rewriteNUnprocessed(abrUrl);
        if (!abrUrlN.equals(abrUrl))
            System.out.println("[Sabr] " + videoId + " abrUrl: n-Parameter entschluesselt");
        // ⚠️ Die Referenz-Implementierung (LuanRT/googlevideo) ruft auf die
        // serverAbrStreamingUrl `player.decipher()` — also die VOLLE Signatur-
        // Behandlung (s->sig UND n), nicht nur nsig. Fehlt die Signatur, nimmt
        // googlevideo den POST an und schickt trotzdem nichts (genau unser
        // WEB-Bild: 105B -> 11B, kein FORMAT_INIT, kein Fehler). Deshalb hier
        // die Parameternamen zeigen — `s=` vorhanden oder `sig=` fehlend ist
        // der Beleg.
        if ("1".equals(System.getenv("YT_SABR_TRACE"))) {
            final int q = abrUrlN.indexOf('?');
            final StringBuilder names = new StringBuilder();
            if (q >= 0) for (String kv : abrUrlN.substring(q + 1).split("&")) {
                final int eq = kv.indexOf('=');
                names.append(' ').append(eq > 0 ? kv.substring(0, eq) : kv);
            }
            System.out.println("[Sabr] " + videoId + " abrUrl-Params:" + names);
        }
        // YT_SABR_DUMP=<pfad>: die Session-EINGABEN als JSON ablegen. Damit laesst
        // sich EXAKT dieselbe Session von der Referenz-Implementierung
        // (LuanRT/googlevideo) fahren — gleiche abrUrl, gleicher ustreamerConfig,
        // gleicher Token, gleiche Formate. Liefert die Referenz Medien und wir
        // nicht, liegt der Unterschied nachweislich in UNSEREM Request-Aufbau;
        // liefert sie ebenfalls nichts, liegt er in den Eingaben. Das ist der
        // Diff, den fuenf geratene Hypothesen nicht ersetzen konnten.
        final String dumpPath = System.getenv("YT_SABR_DUMP");
        if (dumpPath != null && !dumpPath.isEmpty()) {
            try {
                final var on = Constants.mapper.createObjectNode();
                on.put("videoId", videoId);
                on.put("abrUrl", abrUrlN);
                on.put("ustreamerConfig", ustB64);
                on.put("poToken", poToken == null ? null
                        : java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(poToken));
                on.put("clientName", pureWeb ? 1 : webClient ? 56 : vrClient ? 28 : 3);
                on.put("clientVersion", pureWeb ? WEB_VERSION
                        : webClient ? WEB_EMBEDDED_VERSION : vrClient ? "1.62.27" : "20.10.38");
                on.put("userAgent", ua);
                on.put("audioItag", pa.itag); on.put("audioLmt", pa.lmt);
                on.put("videoItag", pv.itag); on.put("videoLmt", pv.lmt);
                // Die vollstaendigen adaptiveFormats mitgeben — die Formatwahl der
                // Referenz braucht Breite/Hoehe/Bitrate/qualityLabel, nicht nur itag.
                on.set("adaptiveFormats", sd.path("adaptiveFormats").deepCopy());
                java.nio.file.Files.writeString(java.nio.file.Path.of(dumpPath),
                        Constants.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(on));
                System.out.println("[Sabr] " + videoId + " Session-Eingaben -> " + dumpPath);
            } catch (Exception e) {
                System.out.println("[Sabr] dump fehlgeschlagen: " + e);
            }
        }
        final SabrSession sessionTmp = null;
        final SabrSession session = new SabrSession(abrUrlN, b64(ustB64), pa, pv, clientInfo, ua, poToken, family);
        // Re-Attest-Hook: bei STREAM_PROTECTION_STATUS=3 einen FRISCHEN
        // content-bound po_token minten und die Session damit fortsetzen
        // (s. SabrSession — das war die vermeintliche Kids-Readahead-Sperre).
        session.setFormatCandidates(candA, candV);
        // WEB braucht die Referenz-Form des client_abr_state (s. SabrSession).
        session.setWebClient(pureWeb);
        // Bedarfsgetriebene Taktung (s. SabrCache): aufhoeren, wenn niemand mehr
        // anfordert; pausieren, solange genug Vorlauf da ist.
        session.setStopWhen(SESSION_STOP == null ? null : SESSION_STOP.apply(videoId));
        session.setPauseWhen(SESSION_PAUSE == null ? null : SESSION_PAUSE.apply(videoId));
        session.setResume(resume);
        session.setProgressSink(progress);
        session.setSeekTargetSeq(SESSION_SEEK == null ? null : SESSION_SEEK.apply(videoId));
        session.setSeekItag(() -> me.kavin.piped.utils.sabr.SabrCache.seekItag(videoId));
        session.setSeekTimeMs(seq -> me.kavin.piped.utils.sabr.SparseStore.segmentStartMs(
                videoId, me.kavin.piped.utils.sabr.SabrCache.seekItag(videoId), seq));
        session.setExakteStartzeit((itag, seq) ->
                me.kavin.piped.utils.sabr.SparseStore.segmentStartMs(videoId, itag, seq));

        // Vollstaendige Session-Erneuerung bei prot=3: NEUER Player-Call (gleicher
        // Client/Egress/visitorData) → frische abrUrl + ustreamerConfig + frischer
        // Attestierungs-Token. Nur einen Token nachzureichen genuegt nicht.
        final String visitorForRenewal = visitorData;
        session.setSessionRefresher(new SabrSession.SessionRefresher() {
            @Override public SabrSession.Renewal fresh() { return fresh(null); }
            @Override public SabrSession.Renewal fresh(String reloadToken) {
            try {
                String attest = null;
                if (bg != null) {
                    final PoTokenResult p2 = bg.sabrSessionPoToken();
                    if (p2 != null) attest = p2.playerRequestPoToken;
                }
                // ⚠️ Derselbe Client wie die laufende Session — eine Erneuerung
                // mit einem ANDEREN Client waere genau die Client<->Token-
                // Zwickmuehle, die uns Tage gekostet hat. `pureWeb` fehlte hier.
                // ⚠️ Den Reload-Kontext aus UMP-Teil 46 mitgeben — ohne ihn
                // antwortet der Server mit exakt demselben Teil 46 (gemessen).
                final JsonNode p = pureWeb
                        ? webPlayer(videoId, visitorForRenewal, family, attest, reloadToken)
                        : webClient
                        ? webEmbedPlayer(videoId, visitorForRenewal, family, attest)
                        : androidPlayer(videoId, visitorForRenewal, family, attest, vrClient);
                final JsonNode sd2 = p.path("streamingData");
                final String url2 = sd2.path("serverAbrStreamingUrl").asText(null);
                final String ust2 = findFirst(p, "videoPlaybackUstreamerConfig");
                if (ust2 == null) return null;
                byte[] pot2 = null;
                if (bg != null) {
                    final String cb = bg.sabrContentBoundPoToken(videoId);
                    if (cb != null) pot2 = b64(cb);
                }
                return new SabrSession.Renewal(url2, b64(ust2), pot2);
            } catch (Exception e) {
                System.out.println("[Sabr] " + videoId + " Session-Erneuerung fehlgeschlagen: " + e.getMessage());
                return null;
            }
        }
        });

        final String reattestBinding = System.getenv("YT_SABR_REATTEST_BINDING");
        final String sessionVisitor = visitorData;
        if (bg != null) session.setTokenRefresher(() -> {
            try {
                // Bindung waehlbar: "visitor" = an dieselbe visitorData wie der
                // Player-Call (Session-Identitaet), sonst an die videoId.
                final String cb = "visitor".equals(reattestBinding) && sessionVisitor != null
                        ? bg.sabrPoTokenForBinding(sessionVisitor)
                        : bg.sabrContentBoundPoToken(videoId);
                return cb != null ? b64(cb) : null;
            } catch (Exception e) {
                System.out.println("[Sabr] " + videoId + " Re-Attest-Mint fehlgeschlagen: " + e.getMessage());
                return null;
            }
        });
        final SabrSession.Result res = session.fetchAll(8000, sink, paced, publishHook);
        long segs = 0;
        for (var info : res.perFormat.values()) {
            final Object v = info.get("segments");
            if (v instanceof Number n) segs += n.longValue();
        }
        return new SabrMedia(aud.path("itag").asInt(), vid.path("itag").asInt(),
                res.complete, res.stopReason, segs);
    }

    private static JsonNode pickFormat(JsonNode sd, String mimePrefix, int preferItag) {
        JsonNode firstMatch = null;
        for (JsonNode f : sd.path("adaptiveFormats")) {
            if (f.path("itag").asInt() == preferItag) return f;
            if (firstMatch == null && f.path("mimeType").asText("").startsWith(mimePrefix)) firstMatch = f;
        }
        return firstMatch;
    }

    /// Echter WEB-Client (Client 1) — der Client, FÜR DEN unser BotGuard-Token
    /// gemünzt ist. Belegt (Protokoll-Analyse 2026-07-25 + öffentliche Doku):
    /// STREAM_PROTECTION_STATUS 2 heißt bereits „Stream braucht einen PoToken,
    /// wir dulden dich noch 1–2 MB", 3 heißt „Token fehlt/ungültig, sofort
    /// Schluss". Unsere Session gab sich bisher als ANDROID (clientInfo 3/28)
    /// aus, präsentierte aber den WEB-BotGuard-Token — die Paarung passt nicht,
    /// deshalb kam ab Runde 1 prot=2 und nach dem Duldungsfenster prot=3.
    /// `STS` (signatureTimestamp) aus der Watch-Page; wechselt nur mit der
    /// Player-JS-Version, daher 6h gecacht. Fallback = zuletzt gesehener Wert.
    private static volatile int STS_CACHE = 20655;
    private static volatile long STS_AT = 0;
    private static int signatureTimestamp() {
        final long now = System.currentTimeMillis();
        if (now - STS_AT < 6 * 3600_000L) return STS_CACHE;
        try {
            final var r = rocks.kavin.reqwest4j.ReqwestUtils.fetch(
                    "https://www.youtube.com/watch?v=dQw4w9WgXcQ", "GET", new byte[0],
                    Map.of("User-Agent", WEB_UA)).get(15, java.util.concurrent.TimeUnit.SECONDS);
            final var m = java.util.regex.Pattern.compile("\"STS\":(\\d+)")
                    .matcher(new String(r.body(), StandardCharsets.UTF_8));
            if (m.find()) { STS_CACHE = Integer.parseInt(m.group(1)); }
        } catch (Exception e) {
            System.out.println("[Sabr] STS-Abruf fehlgeschlagen, nutze " + STS_CACHE);
        }
        STS_AT = now;
        return STS_CACHE;
    }

    /// Ausweich-Audioformat fuer den zweiten Anlauf (pro Aufruf gesetzt).
    static final ThreadLocal<Integer> ALT_AUDIO = new ThreadLocal<>();

    /// Welche Audioformate bietet die Antwort ausser dem gewaehlten?
    static java.util.List<Integer> andereAudioItags(JsonNode sd, int ausser) {
        final java.util.List<Integer> out = new java.util.ArrayList<>();
        for (JsonNode f : sd.path("adaptiveFormats"))
            if (f.path("mimeType").asText("").startsWith("audio")) {
                final int it = f.path("itag").asInt();
                if (it != ausser) out.add(it);
            }
        return out;
    }

    /// Wunsch-itag aus der Umgebung, sonst der bisherige Vorgabewert.
    private static int wunschItag(String schluessel, int vorgabe) {
        final String v = System.getenv(schluessel);
        if (v == null || v.isBlank()) return vorgabe;
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            return vorgabe;
        }
    }

    /// Woher kommt die Player-Antwort? Drei Quellen, in dieser Reihenfolge:
    ///   1. YT_SABR_PLAYER_JSON=<Datei> — fertige Antwort von aussen. Damit
    ///      lassen sich Sitzungen testen, die ein anderer Client geholt hat
    ///      (z.B. ein HTTP-Client mit Chrome-TLS-Fingerabdruck), OHNE die
    ///      Anfrage von Hand zusammenzubauen: das Backend arbeitet ganz normal
    ///      mit einer selbstkonsistenten Antwort weiter.
    ///      ⚠️ Die Datei wird nach dem Lesen NICHT geloescht — sie gilt fuer
    ///      genau ein Video, deshalb steht die videoId im Dateinamen.
    ///   2. YT_SABR_WATCHPAGE=1 — Sitzung aus der Watch-Seite (wie der Browser).
    ///   3. sonst der Player-API-Aufruf wie bisher.
    private static JsonNode spielerAntwort(String videoId, String visitorData, String family,
                                           String attestationPoToken) throws Exception {
        final String ordner = System.getenv("YT_SABR_PLAYER_JSON");
        if (ordner != null && !ordner.isBlank()) {
            final java.nio.file.Path datei = java.nio.file.Path.of(ordner, videoId + ".json");
            if (java.nio.file.Files.exists(datei)) {
                final JsonNode pr = Constants.mapper.readTree(java.nio.file.Files.readString(datei));
                System.out.println("[Sabr] Player-Antwort AUS DATEI " + datei
                        + ": status=" + pr.path("playabilityStatus").path("status").asText()
                        + " formate=" + pr.path("streamingData").path("adaptiveFormats").size()
                        + " ust=" + findFirst(pr, "videoPlaybackUstreamerConfig").length());
                return pr;
            }
            System.out.println("[Sabr] YT_SABR_PLAYER_JSON gesetzt, aber " + datei + " fehlt");
        }
        if ("1".equals(System.getenv("YT_SABR_WATCHPAGE")))
            return watchSeitePlayer(videoId, family);
        return webPlayer(videoId, visitorData, family, attestationPoToken);
    }

    /// Holt die Streaming-Sitzung aus der WATCH-SEITE statt aus der Player-API.
    ///
    /// WARUM: Ein echter Chromium ruft `/youtubei/v1/player` überhaupt nicht auf
    /// (Abfangschicht 2026-08-01: null Player-Calls). Seine Sitzung steckt als
    /// `ytInitialPlayerResponse` im HTML der Watch-Seite. Wir benutzen dagegen
    /// den API-Endpunkt — die einzige Stufe, die im Vergleich Browser/wir nie
    /// angeglichen wurde. Zu prüfen ist, ob die so erzeugte Sitzung eine andere
    /// Schutzstufe bekommt (wir bleiben bei Made-for-Kids immer auf prot=2 und
    /// damit auf den ersten 60 s, der Browser erreicht prot=1).
    ///
    /// Schaltbar über YT_SABR_WATCHPAGE=1, Standard AUS.
    private static JsonNode watchSeitePlayer(String videoId, String family) throws Exception {
        final java.util.Map<String, String> kopf = angemeldeteKopfzeilen(Map.of(
                "User-Agent", WEB_UA,
                "Accept-Language", "de-DE,de;q=0.9",
                "Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"));
        final var r = rocks.kavin.reqwest4j.ReqwestUtils.fetchWithProxy(
                "https://www.youtube.com/watch?v=" + videoId,
                "GET", null, kopf, family).get(25, java.util.concurrent.TimeUnit.SECONDS);
        if (r.status() / 100 != 2)
            throw new IllegalStateException("Watch-Seite HTTP " + r.status());
        final String html = new String(r.body(), StandardCharsets.UTF_8);
        final int a = html.indexOf("ytInitialPlayerResponse");
        if (a < 0) throw new IllegalStateException("ytInitialPlayerResponse fehlt");
        final int b = html.indexOf('{', a);
        if (b < 0) throw new IllegalStateException("ytInitialPlayerResponse ohne Rumpf");
        // Klammern zaehlen — die Antwort enthaelt beliebig verschachteltes JSON
        // und Strings mit geschweiften Klammern.
        int tiefe = 0;
        boolean imText = false, entwertet = false;
        for (int i = b; i < html.length(); i++) {
            final char c = html.charAt(i);
            if (entwertet) { entwertet = false; continue; }
            if (c == '\\') { entwertet = true; continue; }
            if (c == '"') { imText = !imText; continue; }
            if (imText) continue;
            if (c == '{') tiefe++;
            else if (c == '}' && --tiefe == 0)
            {
                final JsonNode pr = Constants.mapper.readTree(html.substring(b, i + 1));
                final var sd = pr.path("streamingData");
                System.out.println("[Sabr] Watch-Seite: status="
                        + pr.path("playabilityStatus").path("status").asText()
                        + " formate=" + sd.path("adaptiveFormats").size()
                        + " abrUrl=" + sd.path("serverAbrStreamingUrl").asText().length()
                        + " ust=" + findFirst(pr, "videoPlaybackUstreamerConfig").length()
                        + " html=" + html.length());
                return pr;
            }
        }
        throw new IllegalStateException("ytInitialPlayerResponse unvollstaendig");
    }

    private static JsonNode webPlayer(String videoId, String visitorData, String family,
                                      String attestationPoToken) throws Exception {
        return webPlayer(videoId, visitorData, family, attestationPoToken, null);
    }

    /// `reloadToken`: der Wert aus UMP-Teil 46. YouTube verlangt damit eine
    /// Player-Antwort, die den Reload-Kontext mitführt — ohne ihn kommt beim
    /// nächsten Versuch dasselbe Teil 46 zurück.
    private static JsonNode webPlayer(String videoId, String visitorData, String family,
                                      String attestationPoToken, String reloadToken) throws Exception {
        final Map<String, Object> client = new HashMap<>(Map.of(
                "clientName", "WEB", "clientVersion", WEB_VERSION,
                "hl", "en", "gl", "US", "userAgent", WEB_UA));
        // ⚠️ Beim ANGEMELDETEN Aufruf keine visitorData mitgeben. Sie beschreibt
        // einen anonymen Besucher und verdrängt die Kontositzung: YouTube
        // vergibt dann kein `siu` in der Streaming-URL. Gemessen 2026-07-31 —
        // identischer Aufruf ohne visitorData liefert siu (signiert in sparams),
        // mit visitorData nicht. `siu` ist der einzige Unterschied zwischen
        // unserer Sitzung und der eines echten Browsers.
        if (!angemeldet() && visitorData != null && !visitorData.isEmpty())
            client.put("visitorData", visitorData);
        final Map<String, Object> req = new HashMap<>(Map.of(
                "context", Map.of("client", client),
                "videoId", videoId, "contentCheckOk", true, "racyCheckOk", true,
                // ⚠️ OHNE signatureTimestamp antwortet der WEB-Client mit
                // UNPLAYABLE/"Video unavailable" — und zwar für JEDES Video, auch
                // ganz normale. Genau daran ist mein erster WEB-Versuch
                // gescheitert (und ich hatte daraus faelschlich geschlossen,
                // Made-for-Kids sperre den Web-Client). Mit sts: status=OK,
                // serverAbrStreamingUrl + 26 Formate, auch fuer Kids-Videos.
                "playbackContext", Map.of("contentPlaybackContext",
                        Map.of("signatureTimestamp", signatureTimestamp(),
                               "html5Preference", "HTML5_PREF_WANTS"))));
        if (attestationPoToken != null)
            req.put("serviceIntegrityDimensions", Map.of("poToken", attestationPoToken));
        if (reloadToken != null && !reloadToken.isBlank())
            req.put("playbackContext", Map.of(
                    "contentPlaybackContext", Map.of(
                            "signatureTimestamp", signatureTimestamp(),
                            "html5Preference", "HTML5_PREF_WANTS"),
                    "reloadPlaybackContext", Map.of(
                            "reloadPlaybackParams", Map.of("token", reloadToken))));
        final String body = Constants.mapper.writeValueAsString(req);
        final var r = rocks.kavin.reqwest4j.ReqwestUtils.fetchWithProxy(
                "https://www.youtube.com/youtubei/v1/player?prettyPrint=false",
                "POST", body.getBytes(StandardCharsets.UTF_8),
                angemeldeteKopfzeilen(Map.of("Content-Type", "application/json",
                        "Accept-Encoding", "identity",
                        "User-Agent", WEB_UA,
                        "X-Youtube-Client-Name", "1",
                        "X-Youtube-Client-Version", WEB_VERSION,
                        "Origin", "https://www.youtube.com",
                        "Referer", "https://www.youtube.com/")),
                family).get(20, java.util.concurrent.TimeUnit.SECONDS);
        return Constants.mapper.readTree(r.body());
    }

    /// Meldet den Player-Call an. WARUM: Der Vergleich unserer Sitzungs-URL mit
    /// der eines echten Chromium (2026-07-31) zeigte genau einen signierten
    /// Unterschied — Chromium bekommt `siu=1`, und zwar INNERHALB von `sparams`,
    /// also von YouTube mitsigniert. Wir bekommen es nicht, weil unser
    /// Player-Call bisher komplett anonym lief: kein Cookie, keine Signatur.
    /// Damit gewährt YouTube uns eine anonyme Streaming-Sitzung — und genau die
    /// wird bei Made-for-Kids-Inhalten nach ~60 s auf Attestierung gestellt
    /// (STREAM_PROTECTION_STATUS 2 -> 3), während die angemeldete Sitzung des
    /// Browsers dasselbe Video komplett ausliefert.
    ///
    /// ⚠️ Cookies allein genügen youtubei nicht — es braucht zusätzlich die
    /// SAPISIDHASH-Signatur über Zeitstempel, SAPISID und Origin. Ohne sie
    /// behandelt Google den Aufruf weiterhin als abgemeldet.
    /// Kill-Switch: YT_SABR_PLAYER_LOGIN=0.
    /// Können und sollen wir den Player-Call anmelden? Kill-Switch:
    /// YT_SABR_PLAYER_LOGIN=0.
    private static boolean angemeldet() {
        if ("0".equals(System.getenv("YT_SABR_PLAYER_LOGIN"))) return false;
        try {
            final String c = me.kavin.piped.utils.BgPoTokenProvider.loadCookieHeader();
            return c != null && !c.isEmpty()
                    && (keksWert(c, "SAPISID") != null || keksWert(c, "__Secure-3PAPISID") != null);
        } catch (Throwable e) {
            return false;
        }
    }

    private static Map<String, String> angemeldeteKopfzeilen(Map<String, String> basis) {
        if (!angemeldet()) return basis;
        final String cookies;
        try {
            cookies = me.kavin.piped.utils.BgPoTokenProvider.loadCookieHeader();
        } catch (Throwable e) {
            return basis;
        }
        if (cookies == null || cookies.isEmpty()) return basis;
        final java.util.Map<String, String> out = new java.util.HashMap<>(basis);
        out.put("Cookie", cookies);
        final String sapisid = keksWert(cookies, "SAPISID") != null
                ? keksWert(cookies, "SAPISID") : keksWert(cookies, "__Secure-3PAPISID");
        if (sapisid != null) {
            final long ts = System.currentTimeMillis() / 1000L;
            final String roh = ts + " " + sapisid + " https://www.youtube.com";
            try {
                final var md = java.security.MessageDigest.getInstance("SHA-1");
                final byte[] h = md.digest(roh.getBytes(StandardCharsets.UTF_8));
                final StringBuilder hex = new StringBuilder();
                for (byte b : h) hex.append(String.format("%02x", b));
                out.put("Authorization", "SAPISIDHASH " + ts + "_" + hex);
                out.put("X-Origin", "https://www.youtube.com");
                out.put("X-Goog-AuthUser", "0");
            } catch (Exception ignored) { /* dann eben ohne Signatur */ }
        }
        return out;
    }

    /// Liest einen einzelnen Wert aus einem "a=1; b=2"-Kopf.
    private static String keksWert(String header, String name) {
        for (String teil : header.split(";")) {
            final String s = teil.trim();
            if (s.startsWith(name + "=")) return s.substring(name.length() + 1);
        }
        return null;
    }

    private static JsonNode webEmbedPlayer(String videoId, String visitorData, String family,
                                           String attestationPoToken) throws Exception {
        final Map<String, Object> client = new HashMap<>(Map.of(
                "clientName", "WEB_EMBEDDED_PLAYER", "clientVersion", WEB_EMBEDDED_VERSION,
                "clientScreen", "EMBED", "hl", "en", "gl", "US", "userAgent", WEB_UA));
        if (visitorData != null && !visitorData.isEmpty()) client.put("visitorData", visitorData);
        final Map<String, Object> req = new HashMap<>(Map.of(
                "context", Map.of("client", client,
                        "thirdParty", Map.of("embedUrl", "https://www.youtube.com/")),
                "videoId", videoId, "contentCheckOk", true, "racyCheckOk", true));
        if (attestationPoToken != null)
            req.put("serviceIntegrityDimensions", Map.of("poToken", attestationPoToken));
        final String body = Constants.mapper.writeValueAsString(req);
        final var r = rocks.kavin.reqwest4j.ReqwestUtils.fetchWithProxy(
                "https://www.youtube.com/youtubei/v1/player?prettyPrint=false",
                "POST", body.getBytes(StandardCharsets.UTF_8),
                Map.of("Content-Type", "application/json",
                        "Accept-Encoding", "identity",
                        "User-Agent", WEB_UA,
                        "X-Youtube-Client-Name", "56",
                        "X-Youtube-Client-Version", WEB_EMBEDDED_VERSION,
                        "Origin", "https://www.youtube.com",
                        "Referer", "https://www.youtube.com/"),
                family != null ? family : me.kavin.piped.utils.EgressManager.activeEgress())
                .get(20, java.util.concurrent.TimeUnit.SECONDS);
        if (r.status() / 100 != 2)
            throw new IllegalStateException("WEB_EMBEDDED player HTTP " + r.status());
        return Constants.mapper.readTree(r.body());
    }

    private static JsonNode androidPlayer(String videoId, String visitorData, String family,
                                          String attestationPoToken, boolean vrClient) throws Exception {
        final Map<String, Object> client = vrClient
                ? new HashMap<>(Map.of(
                        "clientName", "ANDROID_VR", "clientVersion", "1.62.27",
                        "androidSdkVersion", 32, "hl", "en", "gl", "US",
                        "osName", "Android", "osVersion", "12L", "userAgent", ANDROID_VR_UA))
                : new HashMap<>(Map.of(
                        "clientName", "ANDROID", "clientVersion", "20.10.38",
                        "androidSdkVersion", 34, "hl", "en", "gl", "US",
                        "osName", "Android", "osVersion", "14", "userAgent", ANDROID_UA));
        if (visitorData != null && !visitorData.isEmpty()) client.put("visitorData", visitorData);
        final Map<String, Object> req = new HashMap<>(Map.of(
                "context", Map.of("client", client),
                "videoId", videoId, "contentCheckOk", true, "racyCheckOk", true));
        // Attestation in the PLAYER request (serviceIntegrityDimensions): for
        // kids content the ustreamerConfig/abrUrl the player hands out limits
        // SABR readahead to one window unless the player call itself carried a
        // po_token — the streamerContext token alone is ignored there.
        if (attestationPoToken != null)
            req.put("serviceIntegrityDimensions", Map.of("poToken", attestationPoToken));
        final String body = Constants.mapper.writeValueAsString(req);
        final byte[] resp;
        if (family != null) {
            // Explicit-family attempt: same reqwest4j family-pinned socket the
            // SABR session will use, so the returned serverAbrStreamingUrl is
            // signed for the family we actually stream on. No gzip — reqwest4j
            // returns raw bytes and the JSON is small.
            final var r = rocks.kavin.reqwest4j.ReqwestUtils.fetchWithProxy(
                    "https://www.youtube.com/youtubei/v1/player?prettyPrint=false",
                    "POST", body.getBytes(StandardCharsets.UTF_8),
                    Map.of("Content-Type", "application/json",
                            "Accept-Encoding", "identity",
                            "User-Agent", vrClient ? ANDROID_VR_UA : ANDROID_UA,
                            "X-Youtube-Client-Name", vrClient ? "28" : "3",
                            "X-Youtube-Client-Version", vrClient ? "1.62.27" : "20.10.38"),
                    family).get(20, java.util.concurrent.TimeUnit.SECONDS);
            if (r.status() / 100 != 2)
                throw new IllegalStateException("ANDROID player HTTP " + r.status()
                        + " (egress=" + family + ")");
            resp = r.body();
        } else {
            resp = httpPost(
                    "https://www.youtube.com/youtubei/v1/player?prettyPrint=false",
                    body.getBytes(StandardCharsets.UTF_8), "application/json", true,
                    Map.of("User-Agent", vrClient ? ANDROID_VR_UA : ANDROID_UA,
                            "X-Youtube-Client-Name", vrClient ? "28" : "3",
                            "X-Youtube-Client-Version", vrClient ? "1.62.27" : "20.10.38"));
        }
        return Constants.mapper.readTree(resp);
    }

    private static byte[] httpPost(String url, byte[] body, String contentType,
                                   boolean gunzip, Map<String, String> headers) throws Exception {
        final HttpURLConnection c = (HttpURLConnection) new URL(url).openConnection();
        try {
            c.setRequestMethod("POST");
            c.setDoOutput(true);
            c.setConnectTimeout(10_000);
            c.setReadTimeout(30_000);
            c.setRequestProperty("Content-Type", contentType);
            c.setRequestProperty("Accept-Encoding", gunzip ? "gzip" : "identity");
            if (headers != null) headers.forEach(c::setRequestProperty);
            try (OutputStream os = c.getOutputStream()) {
                os.write(body);
            }
            final String enc = c.getContentEncoding();
            InputStream in = c.getInputStream();
            if ("gzip".equalsIgnoreCase(enc)) in = new GZIPInputStream(in);
            try (InputStream is = in) {
                return is.readAllBytes();
            }
        } finally {
            c.disconnect();
        }
    }

    private static byte[] b64(String s) {
        return Base64.getUrlDecoder().decode(s.replace('+', '-').replace('/', '_'));
    }

    private static String findFirst(JsonNode node, String field) {
        if (node.has(field) && node.get(field).isTextual()) return node.get(field).asText();
        for (JsonNode child : node) {
            final String r = findFirst(child, field);
            if (r != null) return r;
        }
        return null;
    }
}
