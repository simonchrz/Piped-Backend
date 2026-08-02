package me.kavin.piped.server.handlers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.grack.nanojson.JsonObject;
import com.grack.nanojson.JsonWriter;
import io.sentry.ITransaction;
import io.sentry.Sentry;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import me.kavin.piped.consts.Constants;
import me.kavin.piped.utils.*;
import me.kavin.piped.utils.obj.*;
import me.kavin.piped.utils.obj.federation.FederatedGeoBypassRequest;
import me.kavin.piped.utils.obj.federation.FederatedGeoBypassResponse;
import me.kavin.piped.utils.obj.federation.FederatedVideoInfo;
import me.kavin.piped.utils.resp.InvalidRequestResponse;
import me.kavin.piped.utils.resp.ThrottledResponse;
import me.kavin.piped.utils.sabr.SabrCache;
import me.kavin.piped.utils.sabr.SabrHandlers;
import me.kavin.piped.utils.resp.VideoResolvedResponse;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.schabi.newpipe.extractor.ListExtractor;
import org.schabi.newpipe.extractor.Page;
import org.schabi.newpipe.extractor.comments.CommentsInfo;
import org.schabi.newpipe.extractor.comments.CommentsInfoItem;
import org.schabi.newpipe.extractor.exceptions.ContentNotAvailableException;
import org.schabi.newpipe.extractor.exceptions.GeographicRestrictionException;
import org.schabi.newpipe.extractor.stream.Description;
import org.schabi.newpipe.extractor.stream.StreamInfo;
import org.schabi.newpipe.extractor.stream.VideoStream;
import org.schabi.newpipe.extractor.services.youtube.extractors.YoutubeStreamExtractor;
import java.net.HttpURLConnection;
import java.net.URL;
import org.schabi.newpipe.extractor.utils.JsonUtils;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static me.kavin.piped.consts.Constants.YOUTUBE_SERVICE;
import static me.kavin.piped.consts.Constants.mapper;
import static me.kavin.piped.utils.URLUtils.*;
import static org.schabi.newpipe.extractor.NewPipe.getPreferredContentCountry;
import static org.schabi.newpipe.extractor.NewPipe.getPreferredLocalization;
import static org.schabi.newpipe.extractor.services.youtube.YoutubeParsingHelper.getJsonPostResponse;
import static org.schabi.newpipe.extractor.services.youtube.YoutubeParsingHelper.prepareDesktopJsonBuilder;

public class StreamHandlers {
    // videoIds known (from a prior resolve) to return audio=0 from ANDROID and need
    // the WebEmbed fallback (ARD/WDR-OER uploads). A re-resolve of such a videoId
    // skips the ANDROID detection attempt and goes straight to WebEmbed (~1s saved).
    // NOTE: keyed by videoId, not channel — channelId is only known AFTER a resolve,
    // and /streams carries only the videoId, so this helps re-resolves/re-prefetches
    // of the SAME video, not first-resolves of new videos from a known-OER channel.
    // ⚠️ Frueher zwei In-Memory-Sets/Maps hier. Beide liegen jetzt in
    // ResolveMemo (persistent im sabr-cache-Volume): der stuendliche
    // Cookie-Refresh startet den Container neu, sobald ein Auth-Cookie rotiert —
    // danach war alles Gelernte weg und jedes Kids-Video zahlte wieder die volle
    // Kaskade (4-5s statt ~1,5s).

    // ── Hebel A: Throttle-Signatur pro Video merken (2026-07-24) ────────────
    // Gemessen: ein sauberer Resolve haelt einen der NUR ZWEI YT_RESOLVE_LIMITER-
    // Slots 1,4s; ein gedrosseltes Made-for-Kids-Video 4,1-6,7s (ANDROID_VR-URLs
    // 403 -> Probe -> egress-flip -> WebEmbed-Re-Resolve). Zwei parallele
    // Kaskaden sperren damit beide Slots und der naechste Tap kriegt 503.
    // Gegentest in derselben Sekunde: Nicht-Kids-Video 1375ms sauber via
    // ANDROID_VR, Kids-Video 4116ms mit 403 auf demselben Pfad -> die Drosselung
    // ist VIDEO-spezifisch (Made-for-Kids), nicht IP-weit.
    // Also: 403-Signatur merken und den naechsten Resolve direkt auf WebEmbed
    // schicken (der Client, der in diesen Fenstern haelt). TTL, weil die
    // Drosselung transient ist (kommt/geht ueber Minuten) und ANDROID_VR der
    // schnellere Happy-Path bleibt, sobald das Fenster durch ist.
    /// videoId -> channelId OHNE Resolve: die DB kennt die Zuordnung bereits
    /// (Abo-Feed + ChannelHandlers tragen Videos beim Kanal-Aufruf ein).
    /// Indizierter Primaerschluessel-Treffer, ~1ms. null = unbekannt (z.B. reiner
    /// Such-Treffer) -> dann greift nur der Pro-Video-Merker wie bisher.
    private static final java.util.concurrent.ConcurrentHashMap<String, String> CHANNEL_OF =
            new java.util.concurrent.ConcurrentHashMap<>();
    private static volatile boolean channelLookupWarned = false;

    private static String channelOf(String videoId) {
        final String hit = CHANNEL_OF.get(videoId);
        if (hit != null) return hit.isEmpty() ? null : hit;
        String out = null;
        // ⚠️ NICHT ueber getVideoFromId(..).getChannel() gehen: die @ManyToOne-
        // Beziehung ist lazy und laesst sich in einer StatelessSession nicht
        // nachladen -> immer null. Stattdessen die FK-Spalte direkt abfragen.
        try (org.hibernate.StatelessSession st =
                     me.kavin.piped.utils.DatabaseSessionFactory.createStatelessSession()) {
            out = st.createQuery(
                    "select v.channel.uploader_id from Video v where v.id = :id", String.class)
                    .setParameter("id", videoId).uniqueResult();
        } catch (Exception e) {
            if (!channelLookupWarned) {
                channelLookupWarned = true;
                System.out.println("[StreamHandlers] Kanal-Lookup nicht verfuegbar: " + e);
            }
        }
        CHANNEL_OF.put(videoId, out == null ? "" : out);
        if (CHANNEL_OF.size() > 5000) CHANNEL_OF.clear();
        return out;
    }

    private static boolean isThrottleMemoized(String videoId) {
        if ("0".equals(System.getenv("YT_THROTTLE_MEMO"))) return false;
        return me.kavin.piped.utils.ResolveMemo.isThrottled(videoId);
    }

    // ── Hebel B: Egress-Flip-Back-off (2026-07-24) ─────────────────────────
    // Der Flip hat am 2026-06-20 nachweislich geholfen (Kids-Content 403 auf v6,
    // sauber auf v4) — bleibt also drin. ABER: Bilanz im heutigen Sturmfenster
    // 13x versucht / 0x geholfen, und jeder Versuch ist ein VOLLER Extra-Resolve
    // auf dem gehaltenen Limiter-Slot. Nach FAIL_STREAK erfolglosen Flips also
    // fuer BACKOFF_MS pausieren; ein erfolgreicher Flip setzt sofort zurueck.
    private static final java.util.concurrent.atomic.AtomicInteger FLIP_FAILS =
            new java.util.concurrent.atomic.AtomicInteger();
    private static final java.util.concurrent.atomic.AtomicLong FLIP_BACKOFF_UNTIL =
            new java.util.concurrent.atomic.AtomicLong();
    private static final int FLIP_FAIL_STREAK = 3;
    private static final long FLIP_BACKOFF_MS = 10 * 60_000L;

    private static boolean flipAllowed() {
        if ("0".equals(System.getenv("YT_FLIP_BACKOFF"))) return true;   // Back-off aus
        return FLIP_BACKOFF_UNTIL.get() < System.currentTimeMillis();
    }

    private static void flipOutcome(String videoId, boolean helped) {
        if (helped) {
            FLIP_FAILS.set(0);
            FLIP_BACKOFF_UNTIL.set(0);
            return;
        }
        if (FLIP_FAILS.incrementAndGet() >= FLIP_FAIL_STREAK) {
            FLIP_FAILS.set(0);
            FLIP_BACKOFF_UNTIL.set(System.currentTimeMillis() + FLIP_BACKOFF_MS);
            System.out.println("[StreamHandlers] egress-flip back-off aktiv ("
                    + FLIP_FAIL_STREAK + " Fehlschlaege, " + (FLIP_BACKOFF_MS / 60_000)
                    + "min) — ausgeloest von " + videoId);
        }
    }

    public static byte[] streamsResponse(String videoId) throws Exception {
        return streamsResponse(videoId, false, 1080, SynthHlsHandlers.DEFAULT_VIDEO_CODECS);
    }

    public static byte[] streamsResponse(String videoId, boolean light) throws Exception {
        return streamsResponse(videoId, light, 1080, SynthHlsHandlers.DEFAULT_VIDEO_CODECS);
    }

    // light=true (/streams?light=1): skip the ~900ms /next call (related videos +
    // chapters + metaInfo) for the cold-tap playback path. relatedStreams come back
    // empty; the app loads them lazily after playback start. ageLimit stays correct
    // (microformat isFamilySafe, not /next — see YoutubeStreamExtractor.getAgeLimit).
    // maxH/codecs select which rendition the sidx-prewarm warms — must match the
    // synth-hls tap (the app sends the same ?maxh=&codecs= on both).
    public static byte[] streamsResponse(String videoId, boolean light, int maxH, String[] codecs) throws Exception {

        Sentry.setExtra("videoId", videoId);

        // Cold-tap Lever #1 (2026-06-27): a /streams?light tap reuses the resolve
        // a prior /streams?light prefetch already seeded into the synth
        // streamsCache, instead of a full StreamInfo.getInfo re-resolve. The seed
        // is urlsVerified=true and cpn freshness is handled downstream at
        // segment-serve time (synth swapCpn), so serving the cached Streams is
        // safe. Only for light (the cold-tap path; a related-less response is
        // expected there). Re-warm sidx+first-segment (idempotent, non-blocking)
        // so the tap still primes Lever #2's disk cache.
        if (light) {
            Streams cachedStreams = SynthHlsHandlers.getFreshVerifiedStreams(videoId);
            if (cachedStreams != null) {
                SynthHlsHandlers.cacheStreams(videoId, cachedStreams, true, false, maxH, codecs);
                System.out.println("[StreamHandlers] " + videoId
                        + " /streams RESOLVE-CACHE HIT (prefetch-seeded, skipped re-resolve)");
                return mapper.writeValueAsBytes(cachedStreams);
            }
        }

        final var futureStream = Multithreading.supplyAsync(() -> {
            Sentry.setExtra("videoId", videoId);
            ITransaction transaction = Sentry.startTransaction("StreamInfo fetch", "fetch");
            if (light) YoutubeStreamExtractor.SKIP_NEXT_FOR_THREAD.set(Boolean.TRUE);
            try {
                final long tResolve0 = System.currentTimeMillis();
                StreamInfo info = null;

                // C: known-audio=0 (OER) videoId from a prior resolve → skip the ANDROID
                // detection attempt and resolve directly via WebEmbed (~1s saved). If it
                // doesn't come back healthy, the marker is stale → drop it + fall through.
                if (me.kavin.piped.utils.ResolveMemo.isAudioZero(videoId)) {
                    // audio0 fallback: WEB_EMBEDDED is the ONLY client that serves
                    // audio for these videos (iOS/WEB/TVHTML5 return audio=0/"only
                    // images"; verified 2026-06-09). The June-09 segment-403s were a
                    // transient googlevideo throttle storm, not a permanent block, so
                    // WebEmbed remains the audio0 path. (TVHTML5 was tried and rejected
                    // by YT: WATCH=bot-gated, EMBED="no longer supported".)
                    YoutubeStreamExtractor.FORCE_WEB_EMBED_FOR_THREAD.set(Boolean.TRUE);
                    try {
                        StreamInfo we = StreamInfo.getInfo("https://www.youtube.com/watch?v=" + videoId);
                        if (we != null && !we.getAudioStreams().isEmpty()
                                && (!we.getVideoStreams().isEmpty() || !we.getVideoOnlyStreams().isEmpty())) {
                            info = we;
                            System.out.println("[StreamHandlers] " + videoId
                                    + " known-audio0 -> direct WebEmbed HIT (skipped ANDROID)");
                        } else {
                            me.kavin.piped.utils.ResolveMemo.clearAudioZero(videoId);
                        }
                    } catch (Exception e) {
                        me.kavin.piped.utils.ResolveMemo.clearAudioZero(videoId);
                    } finally {
                        YoutubeStreamExtractor.FORCE_WEB_EMBED_FOR_THREAD.remove();
                    }
                }

                // Hebel A: dieses Video war eben noch gedrosselt (ANDROID_VR-URLs 403)
                // -> direkt WebEmbed, ohne VR-Versuch + Probe + egress-flip. Spart auf
                // dem gehaltenen Limiter-Slot ~2,5-5s. Zusaetzlich wird die Probe unten
                // uebersprungen (throttleChecked), denn WebEmbed IST hier bereits die
                // Antwort auf die Drosselung — 403 auf WebEmbed faellt weiter in die
                // TVHTML5/SABR-Kaskade wie bisher. Nicht-healthy -> Memo war stale.
                boolean throttleFastPath = false;
                // Kanal-Urteil ergaenzt den Pro-Video-Merker: der hilft erst beim
                // ZWEITEN Antippen desselben Videos, das Kanal-Wissen schon beim
                // ersten eines NEUEN Videos.
                final String channelId = channelOf(videoId);
                final boolean channelSlow =
                        me.kavin.piped.utils.ResolveMemo.channelPrefersSlowPath(channelId);
                if (info == null && (isThrottleMemoized(videoId) || channelSlow)) {
                    if (channelSlow && !isThrottleMemoized(videoId))
                        System.out.println("[StreamHandlers] " + videoId
                                + " Kanal-Urteil: direkt WebEmbed (Kanal " + channelId + ")");
                    YoutubeStreamExtractor.FORCE_WEB_EMBED_FOR_THREAD.set(Boolean.TRUE);
                    try {
                        StreamInfo we = StreamInfo.getInfo("https://www.youtube.com/watch?v=" + videoId);
                        if (we != null && !we.getAudioStreams().isEmpty()
                                && (!we.getVideoStreams().isEmpty() || !we.getVideoOnlyStreams().isEmpty())) {
                            info = we;
                            throttleFastPath = true;
                            System.out.println("[StreamHandlers] " + videoId
                                    + " known-throttled -> direct WebEmbed HIT (skipped ANDROID_VR+probe+flip)");
                        } else {
                            me.kavin.piped.utils.ResolveMemo.clearThrottled(videoId);
                        }
                    } catch (Exception e) {
                        me.kavin.piped.utils.ResolveMemo.clearThrottled(videoId);
                    } finally {
                        YoutubeStreamExtractor.FORCE_WEB_EMBED_FOR_THREAD.remove();
                    }
                }

                // NPE returns degraded streams (1 muxed video, 0 audio) non-deterministically
                // for some videos. Up to 3 attempts; accept the first that has both audio
                // and video. Same retry-on-degraded pattern as SynthHlsHandlers.fetchStreams.
                if (info == null) {
                    for (int attempt = 0; attempt < 3; attempt++) {
                        try {
                            info = StreamInfo.getInfo("https://www.youtube.com/watch?v=" + videoId);
                        } catch (org.schabi.newpipe.extractor.exceptions.SignInConfirmNotBotException sie) {
                            // YouTube blocks ANONYMOUS watch access for all three
                            // anonymous clients (VR/ANDROID/WebEmbed) — seen as an
                            // IP-wide wave 2026-06-11. The authenticated TVHTML5
                            // session is the designed escape hatch, but until now it
                            // hung only off the throttle branch (which needs info !=
                            // null). Try it before surfacing the 500.
                            System.out.println("[StreamHandlers] " + videoId
                                    + " anonymous clients sign-in-blocked -> trying authenticated TVHTML5");
                            YoutubeStreamExtractor.FORCE_WEB_EMBED_FOR_THREAD.remove();
                            YoutubeStreamExtractor.FORCE_TVHTML5_FOR_THREAD.set(Boolean.TRUE);
                            try {
                                StreamInfo tv = StreamInfo.getInfo(
                                        "https://www.youtube.com/watch?v=" + videoId);
                                if (tv != null && !tv.getAudioStreams().isEmpty()
                                        && (!tv.getVideoStreams().isEmpty()
                                            || !tv.getVideoOnlyStreams().isEmpty())) {
                                    // Same TV-only legacy itag filter as the storm path.
                                    tv.getAudioStreams().removeIf(a -> a.getItagItem() != null
                                            && (a.getItagItem().id == 148 || a.getItagItem().id == 149));
                                    info = tv;
                                    System.out.println("[StreamHandlers] " + videoId
                                            + " TVHTML5 sign-in-block fallback OK (audio="
                                            + tv.getAudioStreams().size() + " client=TVHTML5)");
                                }
                            } catch (Exception tvE) {
                                System.out.println("[StreamHandlers] " + videoId
                                        + " TVHTML5 sign-in-block fallback failed: " + tvE.getMessage());
                            } finally {
                                YoutubeStreamExtractor.FORCE_TVHTML5_FOR_THREAD.remove();
                            }
                            if (info == null && me.kavin.piped.utils.EgressManager.flipOnBotFlag()) {
                                try {
                                    StreamInfo flipped = StreamInfo.getInfo("https://www.youtube.com/watch?v=" + videoId);
                                    if (flipped != null && !flipped.getAudioStreams().isEmpty()
                                            && (!flipped.getVideoStreams().isEmpty() || !flipped.getVideoOnlyStreams().isEmpty())) {
                                        info = flipped;
                                        System.out.println("[StreamHandlers] " + videoId
                                                + " egress-flip resolve OK (" + me.kavin.piped.utils.EgressManager.activeLabel() + ")");
                                    }
                                } catch (Exception fe) {
                                    System.out.println("[StreamHandlers] " + videoId
                                            + " egress-flip retry failed: " + fe.getMessage());
                                }
                            }
                            if (info == null) throw sie; // anonymous AND authenticated dead
                            break;
                        } catch (ContentNotAvailableException perm) {
                            throw perm; // permanent (unavailable/geo) — retrying can't help
                        } catch (Exception rex) {
                            // Transient resolve failure (innertube timeout/IO during a
                            // googlevideo throttle wave — observed 2026-07-05: one attempt
                            // timed out at 10s, the next landed in 2s). ONE paused retry;
                            // a second 10s-failure would blow the outer 18s budget anyway
                            // (the TimeoutException catch there has the serve-stale net).
                            if (attempt >= 1) throw rex;
                            System.out.println("[StreamHandlers] " + videoId + " attempt " + (attempt + 1)
                                    + " resolve failed transient (" + rex.getClass().getSimpleName()
                                    + ") -> retry in 1.5s");
                            Thread.sleep(1500);
                            continue;
                        }
                        final boolean hasAudio = info != null && !info.getAudioStreams().isEmpty();
                        final boolean hasVideo = info != null && (!info.getVideoStreams().isEmpty()
                                || !info.getVideoOnlyStreams().isEmpty());
                        if (hasAudio && hasVideo) {
                            break;                       // healthy
                        }
                        // audio=0 is the deterministic ARD/WDR-OER signature: the ANDROID
                        // client returns 0 adaptive audio for those uploads, so retrying
                        // ANDROID can NEVER add audio — only the WebEmbed fallback below
                        // can (verified). Break after ONE attempt instead of burning 2
                        // more ~1s resolves (OER cold-resolve was ~4-7s). Transient null /
                        // video=0 (genuinely non-deterministic) keep the full retry budget.
                        if (info != null && !hasAudio) {
                            System.out.println("[StreamHandlers] " + videoId + " attempt " + (attempt + 1)
                                    + " audio=0 (OER signature) -> skip ANDROID retries, straight to WebEmbed");
                            break;
                        }
                        System.out.println("[StreamHandlers] " + videoId + " attempt " + (attempt + 1)
                                + " degraded (audio=" + (info == null ? -1 : info.getAudioStreams().size())
                                + " video=" + (info == null ? -1 : info.getVideoStreams().size())
                                + " videoOnly=" + (info == null ? -1 : info.getVideoOnlyStreams().size()) + "), retrying");
                    }
                }

                // Throttle-Check: ANDROID-URLs werden von googlevideo per-IP-
                // Pattern fuer deep byte-range-fetches 403'd (= Scrub-forward =
                // frozen frame) auch wenn Response strukturell healthy aussieht.
                // HEAD-test gegen die clen/2-Mitte der highest-quality video-URL
                // deckt das auf. Bei 403 setzen wir NPE's force-WebEmbed
                // ThreadLocal und rufen StreamInfo nochmal -- WebEmbed-modern-URLs
                // werden nicht so throttled.
                // WebEmbed-Fallback bei (a) Throttle (HEAD=403) ODER (b) persistent
                // degraded (audio=0 nach den 3 Android-Versuchen). Manche Videos —
                // v.a. ARD/WDR-OER-Uploads (z.B. "Die Maus") — liefern dem Android-
                // Client 0 adaptive Audio-Streams; web_embedded liefert sie (verifiziert
                // per yt-dlp: android=0 audio, web_embedded=4 inkl. m4a). Ohne diesen
                // Fallback dreht die 3x-Retry-Schleife leer (~5s Cold-Start) und das
                // Ergebnis bleibt audiolos. Der degraded-Pfad short-circuitet den
                // HEAD-Probe (kein throttle-Check noetig wenn eh schon audio=0).
                boolean degraded = info != null && info.getAudioStreams().isEmpty();
                // Wurde TVHTML5 in diesem Resolve schon einmal befragt? Dann ist ein
                // zweiter Anlauf im Storm-Fallback verschwendete Zeit — gemessen ~1s
                // von 4s Gesamt-Resolve auf einem kalten Kids-Video.
                boolean tvHtml5AlreadyTried = false;

                // Kids/OER (audio=0) additive fast-path (2026-07-24): prefer the
                // authenticated TVHTML5 ("tv") client for made-for-kids/OER content
                // — yt-dlp-verified to yield clean nsig-solved DIRECT URLs where the
                // ANDROID client returns audio=0 — BEFORE the WebEmbed cascade below.
                // Purely additive + regression-proof: only fires on the audio=0
                // signature, and only commits `info` when TVHTML5 returns healthy
                // audio+video; on anything else (throw / non-healthy) it leaves `info`
                // untouched so the existing egress-flip + WebEmbed + storm path runs
                // exactly as before. throttleSuspect is computed AFTER this on the
                // (possibly TV) info, so if the TV URLs are themselves throttled in a
                // storm the normal storm-fallback still engages. Kill-switch
                // YT_TV_FIRST_FOR_AUDIO0=0. The TV client already existed only as a
                // sign-in/storm fallback (getTvHtml5PlayerResponse); this just lets
                // kids content reach it on the happy path.
                if (degraded && !"0".equals(System.getenv("YT_TV_FIRST_FOR_AUDIO0"))) {
                    System.out.println("[StreamHandlers] " + videoId
                            + " audio=0 (kids/OER) -> trying authenticated TVHTML5 first");
                    YoutubeStreamExtractor.FORCE_TVHTML5_FOR_THREAD.set(Boolean.TRUE);
                    try {
                        StreamInfo tv = StreamInfo.getInfo(
                                "https://www.youtube.com/watch?v=" + videoId);
                        if (tv != null && !tv.getAudioStreams().isEmpty()
                                && (!tv.getVideoStreams().isEmpty()
                                    || !tv.getVideoOnlyStreams().isEmpty())) {
                            // Same TV-only legacy itag filter as the storm/sign-in paths
                            // (148/149 are TV-client-only formats other stages can't serve).
                            tv.getAudioStreams().removeIf(a -> a.getItagItem() != null
                                    && (a.getItagItem().id == 148 || a.getItagItem().id == 149));
                            info = tv;
                            degraded = false;
                            tvHtml5AlreadyTried = true;   // s. Storm-Fallback weiter unten
                            System.out.println("[StreamHandlers] " + videoId
                                    + " TVHTML5-first HIT (audio=" + tv.getAudioStreams().size()
                                    + " video=" + tv.getVideoStreams().size()
                                    + " videoOnly=" + tv.getVideoOnlyStreams().size() + ")");
                        } else {
                            System.out.println("[StreamHandlers] " + videoId
                                    + " TVHTML5-first not healthy -> WebEmbed cascade");
                        }
                    } catch (Exception e) {
                        System.out.println("[StreamHandlers] " + videoId
                                + " TVHTML5-first failed: " + e.getMessage() + " -> WebEmbed cascade");
                    } finally {
                        YoutubeStreamExtractor.FORCE_TVHTML5_FOR_THREAD.remove();
                    }
                }

                // One HEAD probe decides the suspect path (degraded short-circuits it,
                // so audio0 videos skip the probe). Healthy videos: exactly this one
                // probe, same as before; the final liveness gate below only re-probes
                // when this is true, keeping the extra HEAD off the fast path.
                // Hebel A: auf dem known-throttled-Fast-Path ist WebEmbed bereits die
                // Antwort auf die Drosselung — die Probe (und damit Flip + WebEmbed-
                // Retry) waere hier reine Wiederholung. Die Liveness-Gate unten prueft
                // trotzdem noch, ob DIESE URLs leben.
                boolean throttleSuspect = !throttleFastPath
                        && info != null && (degraded || isVideoStreamThrottled(info));
                // Signatur merken (nicht bei degraded — dafuer gibt es das audioZero-Memo).
                if (throttleSuspect && !degraded) {
                    me.kavin.piped.utils.ResolveMemo.markThrottled(videoId);
                }
                // Kanal-Statistik fuehren: brauchte dieses Video den langsamen Weg?
                // (Fast-Path zaehlt als „langsam", denn er IST das Urteil selbst.)
                if (!degraded)
                    me.kavin.piped.utils.ResolveMemo.recordChannelOutcome(
                            channelId, throttleSuspect || throttleFastPath);

                // EGRESS-FLIP FIRST (made-for-kids / content-specific throttle):
                // googlevideo often 403s the media on only ONE egress family (e.g. the
                // v6 /64) while the OTHER (v4) serves the very same video fine via
                // ANDROID_VR. The bot-flag autoflip cant see this (its canary is a
                // normal video that plays on both families). So on throttle/degraded,
                // TRY a re-resolve on the other family before falling to the WebEmbed
                // path (which is ALSO throttled on the bad family for these videos).
                // The trial uses a thread-local egress override; only a CLEAN result
                // commits the family globally (so yt-proxy segment fetches follow).
                // A still-bad result (genuine OER audio=0, or a hard-throttle on both
                // families) leaves the global family untouched and falls through.
                // Verified 2026-06-20: Jakobs kids content (LEGO/DFB-cards) 403d
                // wholesale on v6, played on v4. Kill-switch EGRESS_FLIP_ON_THROTTLE=0.
                // Hebel B: nach FLIP_FAIL_STREAK erfolglosen Flips fuer FLIP_BACKOFF_MS
                // aussetzen — jeder Versuch kostet einen VOLLEN Extra-Resolve auf dem
                // gehaltenen Limiter-Slot (Bilanz 2026-07-24: 13x versucht, 0x geholfen).
                if (throttleSuspect && !flipAllowed()) {
                    System.out.println("[StreamHandlers] " + videoId
                        + " egress-flip uebersprungen (Back-off aktiv) -> WebEmbed path");
                } else if (throttleSuspect && !"0".equals(System.getenv("EGRESS_FLIP_ON_THROTTLE"))) {
                    String before = me.kavin.piped.utils.EgressManager.activeEgress();
                    String other = me.kavin.piped.utils.EgressManager.otherFamily();
                    if (!other.equals(before)) {
                        me.kavin.piped.utils.EgressManager.beginTrial(other);
                        boolean committed = false;
                        try {
                            StreamInfo flipInfo = StreamInfo.getInfo(
                                "https://www.youtube.com/watch?v=" + videoId);
                            boolean flipHealthy = flipInfo != null
                                && !flipInfo.getAudioStreams().isEmpty()
                                && (!flipInfo.getVideoStreams().isEmpty()
                                    || !flipInfo.getVideoOnlyStreams().isEmpty());
                            if (flipHealthy && !isVideoStreamThrottled(flipInfo)) {
                                info = flipInfo;
                                throttleSuspect = false;
                                committed = true;
                                System.out.println("[StreamHandlers] " + videoId
                                    + " egress-flip " + before + "->" + other
                                    + " CLEAN (audio=" + info.getAudioStreams().size()
                                    + " video=" + info.getVideoStreams().size()
                                    + " videoOnly=" + info.getVideoOnlyStreams().size() + ")");
                            }
                        } catch (Exception e) {
                            System.out.println("[StreamHandlers] " + videoId
                                + " egress-flip re-resolve failed: " + e.getMessage());
                        } finally {
                            me.kavin.piped.utils.EgressManager.endTrial();
                            if (committed)
                                me.kavin.piped.utils.EgressManager.commitFamily(other);
                        }
                        flipOutcome(videoId, committed);
                        if (!committed)
                            System.out.println("[StreamHandlers] " + videoId
                                + " egress-flip " + before + "->" + other
                                + " did not help -> WebEmbed path");
                    }
                }

                if (throttleSuspect) {
                    System.out.println("[StreamHandlers] " + videoId + " "
                            + (degraded ? "degraded (audio=0)" : throttleNoted(videoId))
                            + ", retry mit force-WebEmbed");
                    YoutubeStreamExtractor.FORCE_WEB_EMBED_FOR_THREAD.set(Boolean.TRUE);
                    try {
                        StreamInfo retryInfo = StreamInfo.getInfo(
                            "https://www.youtube.com/watch?v=" + videoId);
                        if (retryInfo != null && !retryInfo.getAudioStreams().isEmpty()
                                && (!retryInfo.getVideoStreams().isEmpty()
                                    || !retryInfo.getVideoOnlyStreams().isEmpty())) {
                            info = retryInfo;
                            if (degraded) me.kavin.piped.utils.ResolveMemo.markAudioZero(videoId); // remember for next re-resolve
                            System.out.println("[StreamHandlers] " + videoId + " WebEmbed-retry success (audio="
                                + info.getAudioStreams().size() + " video="
                                + info.getVideoStreams().size() + " videoOnly="
                                + info.getVideoOnlyStreams().size() + ")");
                        } else {
                            System.out.println("[StreamHandlers] " + videoId + " WebEmbed-retry returned non-healthy, sticking with original");
                        }
                    } catch (Exception e) {
                        System.out.println("[StreamHandlers] " + videoId + " WebEmbed-retry failed: " + e.getMessage());
                    } finally {
                        YoutubeStreamExtractor.FORCE_WEB_EMBED_FOR_THREAD.remove();
                    }
                }
                boolean testForceThrottle = info != null
                        && videoId.equals(System.getenv("YT_TEST_FORCE_THROTTLE"));
                // Test hook for stage 4: forces the chain PAST TVHTML5 straight
                // into the SABR storm-fallback (healthy video, fake storm).
                boolean testForceSabrStorm = info != null
                        && videoId.equals(System.getenv("YT_TEST_FORCE_SABR_STORM"));
                // throttleFastPath MUSS hier mitzaehlen: der Fast-Path setzt
                // throttleSuspect=false (Flip/WebEmbed-Retry waeren Wiederholung),
                // aber die Liveness-Kontrolle darf er NICHT umgehen — sonst reichen
                // wir in einem harten Fenster (WebEmbed selbst tot) ein 200 mit toten
                // URLs durch, statt in TVHTML5/SABR zu fallen. Kostet eine Probe
                // (~200ms) — dieselbe, die der Normalpfad ohnehin bezahlt.
                boolean stillThrottled = testForceThrottle || testForceSabrStorm
                        || ((throttleSuspect || throttleFastPath)
                            && info != null && isVideoStreamThrottled(info));
                boolean servedViaSabr = false;

                // Storm-fallback: WebEmbed segments are 403, so try the
                // authenticated TVHTML5 (TV) client. Its URLs carry ratebypass and
                // survive googlevideo throttle storms that kill WebEmbed's. Only
                // reached on the suspect path during an actual 403 (or the test env).
                if (stillThrottled && !testForceSabrStorm && tvHtml5AlreadyTried) {
                    // TVHTML5 lief in DIESEM Resolve bereits (audio=0-Fast-Path) und
                    // seine URLs wurden vom Throttle-Check verworfen. Ein zweiter,
                    // identischer Anlauf kostet ~1s und liefert dasselbe Ergebnis.
                    System.out.println("[StreamHandlers] " + videoId
                            + " TVHTML5 bereits geprueft (gedrosselt) -> zweiten Anlauf uebersprungen");
                } else if (stillThrottled && !testForceSabrStorm) {
                    System.out.println("[StreamHandlers] " + videoId
                            + " WebEmbed still throttled -> trying authenticated TVHTML5");
                    // Defensive: clear any WebEmbed force (ThreadLocals persist on
                    // pooled threads) so the TV resolve is pure TVHTML5 -- otherwise
                    // audio itags dedupe onto leftover WebEmbed URLs.
                    YoutubeStreamExtractor.FORCE_WEB_EMBED_FOR_THREAD.remove();
                    YoutubeStreamExtractor.FORCE_TVHTML5_FOR_THREAD.set(Boolean.TRUE);
                    try {
                        StreamInfo tv = StreamInfo.getInfo(
                            "https://www.youtube.com/watch?v=" + videoId);
                        if (tv != null && !tv.getAudioStreams().isEmpty()
                                && (!tv.getVideoStreams().isEmpty()
                                    || !tv.getVideoOnlyStreams().isEmpty())) {
                            // Drop TV-only legacy itags 148/149 (HE-AAC dups of 140)
                            // that 500 on fetch; keep the standard playable formats.
                            tv.getAudioStreams().removeIf(a -> a.getItagItem() != null
                                    && (a.getItagItem().id == 148 || a.getItagItem().id == 149));
                            if (testForceThrottle || !isVideoStreamThrottled(tv)) {
                                info = tv;
                                stillThrottled = false;
                                System.out.println("[StreamHandlers] " + videoId
                                        + " TVHTML5 storm-fallback OK (audio="
                                        + tv.getAudioStreams().size() + " client=TVHTML5)");
                            } else {
                                System.out.println("[StreamHandlers] " + videoId
                                        + " TVHTML5 also throttled");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("[StreamHandlers] " + videoId
                                + " TVHTML5 fallback failed: " + e.getMessage());
                    } finally {
                        YoutubeStreamExtractor.FORCE_TVHTML5_FOR_THREAD.remove();
                    }
                }

                // Sequential-playability confirmation before the CAPPED SABR
                // fallback (2026-07-24). isVideoStreamThrottled probes a clen/2
                // DEEP-SEEK, which googlevideo 403s during transient throttle
                // waves even on URLs that serve SEQUENTIAL ranges from offset 0
                // perfectly — verified live: yt-dlp AND our own yt-proxy download
                // these videos in full (206) while this path reported "storm".
                // Playback IS sequential (yt-proxy fills via bounded ranges from
                // the start; scrub-forward degrades to a slow fill, never breaks),
                // so a deep-seek-only 403 must NOT commit the video to the 60s-
                // capped SABR path — that's a strict downgrade from a fully
                // playable direct URL. Re-probe from offset 0 (same egress as the
                // real fetch): serves -> keep the direct URLs, skip SABR. Only a
                // sequential 403 (genuine total block) falls through to SABR.
                // Not run under the test hooks (they intentionally force the path).
                // Kill-switch YT_SEQ_PLAYABILITY_GATE=0.
                if (stillThrottled && !testForceSabrStorm && !testForceThrottle
                        && !"0".equals(System.getenv("YT_SEQ_PLAYABILITY_GATE"))
                        && canServeSequentially(info)) {
                    stillThrottled = false;
                    System.out.println("[StreamHandlers] " + videoId
                            + " deep-seek 403 but SEQUENTIAL 206 -> playable, keeping"
                            + " direct URLs (no SABR)");
                }

                // Stage 4 — SABR storm-fallback: WebEmbed AND TVHTML5 segment
                // URLs are 403, but SABR media travels as POST/UMP against
                // serverAbrStreamingUrl — a different googlevideo request shape
                // the per-URL throttle may not cover. Cheap viability probe (one
                // ANDROID player call); on success mark the video so synth-hls
                // serves it from /sabr and warm the download async (the variant
                // build blocks on ensureFile until it lands). Double-failure
                // (storm AND SABR dead) surfaces as a playlist error instead of
                // this 503 — logged, mark expires after 30 min.
                // Kill switch: YT_SABR_STORM_FALLBACK=false.
                // ⚠️ Liegt bereits SABR-Material auf Platte, braucht es KEINE Probe:
                // wir koennen sofort daraus ausliefern. Ohne diese Abkuerzung
                // scheiterte die frische Probe im Drosselfenster und wir gaben
                // 503 zurueck, obwohl /sabr/<id>/137 in 16 ms ein 206 lieferte —
                // in der App der Wiederholungs-Dialog statt eines spielenden
                // Videos (2026-07-31 gemessen an klbPE-HNmIE).
                final boolean sabrCacheReady = SabrCache.hasCache(videoId);
                if (stillThrottled
                        && !"false".equalsIgnoreCase(System.getenv("YT_SABR_STORM_FALLBACK"))
                        && (sabrCacheReady || SabrHandlers.sabrViable(videoId))) {
                    SabrCache.markStorm(videoId);
                    // 🔑 Den MWEB-Ersatz JETZT schon warmlaufen lassen. Kann
                    // dieses Video kein SABR (403 auf die POST), braucht der
                    // Playlist-Bau den Direktweg — und der Abruf dauert rund 2 s.
                    // Faengt er erst an, wenn der Playlist-Bau ihn vermisst, ist
                    // das Rennen gegen das 12-s-Budget des Servers knapp und
                    // ging gemessen jedes zweite Mal verloren. Aus dem
                    // Kurzzeit-Cache (60 s) kostet er danach 0 ms.
                    Multithreading.runAsync(() -> {
                        try {
                            me.kavin.piped.utils.MwebStreams.hole(videoId);
                        } catch (Exception ignored) { }
                    });
                    Multithreading.runAsync(() -> {
                        try {
                            // itagsFor triggers the one-time session download and
                            // returns the ACTUAL picked itags (not always 140/137).
                            final int[] itags = SabrCache.itagsFor(videoId);
                            System.out.println("[StreamHandlers] " + videoId
                                    + " SABR storm-warm complete (audio=" + itags[0]
                                    + " video=" + itags[1] + ")");
                        } catch (Exception e) {
                            System.out.println("[StreamHandlers] " + videoId
                                    + " SABR storm-warm failed: " + e.getMessage());
                        }
                    });
                    stillThrottled = false;
                    servedViaSabr = true;
                    System.out.println("[ResolvePath] " + videoId
                            + " -> SABR-STORM (WebEmbed+TVHTML5 403, serving via /sabr"
                            + (sabrCacheReady ? ", Cache vorhanden" : ", Probe ok") + ")");
                }

                // Final segment-liveness gate: WebEmbed AND TVHTML5 both 403 -> don't
                // hand the app a 200 with dead URLs; surface a distinct 503 so it can
                // show "YouTube is rate-limiting" instead of a generic timeout.
                if (stillThrottled) {
                    // Merken: dieses Video ist gedrosselt. Der Playlist-Bau
                    // bevorzugt danach den Cache statt kurzlebiger Direkt-URLs.
                    SabrCache.noteThrottled(videoId);
                    System.out.println("[StreamHandlers] " + videoId
                            + " STILL throttled after WebEmbed + TVHTML5 + SABR (segments 403) -> 503 throttled");
                    // ⚠️ NICHT den Resolve hier in den synth-hls-Cache seeden
                    // (2026-07-25 versucht, vom Smoke-Test sofort gefangen): die
                    // Streams tragen genau die URLs, die wir eben als 403 erkannt
                    // haben. Der Master-Build (requireVerified=false) haette sie
                    // wiederverwendet und eine DIREKT-Playlist mit toten URLs
                    // ausgeliefert -> Segmente 403 -> AVPlayer -12660. Ein
                    // gesparter Resolve ist das nicht wert; der Kalt-Tap-Gewinn
                    // kommt ohnehin aus dem Storm-Kurzschluss in SynthHlsHandlers.
                    ExceptionHandler.throwErrorResponse(new ThrottledResponse(
                            "YouTube is rate-limiting playback for this video (segment URLs return "
                            + "403). Transient googlevideo throttle - try again shortly."));
                }
                // Self-healing storm-mark: this resolve produced healthy direct
                // URLs WITHOUT falling to SABR (either not throttle-suspect, or the
                // sequential-playability gate confirmed the URLs serve) — so if the
                // video was still storm-marked from an earlier bad window, the
                // window has recovered; drop the mark so synth-hls stops serving the
                // capped /sabr path and returns to the direct URLs. Closes the
                // "yt-dlp plays it fully but we keep SABR-ing for 30 min" gap: the
                // block is transient (whole-URL, comes and goes) but the mark was
                // sticky. next SynthHls poll re-checks isStormMarked -> false -> direct.
                if (!servedViaSabr && info != null && !info.getAudioStreams().isEmpty()
                        && (!info.getVideoStreams().isEmpty() || !info.getVideoOnlyStreams().isEmpty())
                        && SabrCache.isStormMarked(videoId)) {
                    SabrCache.clearStorm(videoId);
                    System.out.println("[StreamHandlers] " + videoId
                            + " storm-mark cleared (healthy direct resolve — window recovered)");
                }
                // Hebel A, selbstheilend: ein NICHT-Fast-Path-Resolve, dessen URLs die
                // Probe ueberlebt haben, heisst „Drosselungsfenster durch" -> Memo weg,
                // naechster Resolve nimmt wieder den schnelleren ANDROID_VR-Happy-Path.
                // (Nach dem Fast-Path selbst NICHT loeschen — dort wurde VR nie geprueft.)
                if (!throttleFastPath && !throttleSuspect && !degraded
                        && me.kavin.piped.utils.ResolveMemo.isThrottled(videoId)) {
                    me.kavin.piped.utils.ResolveMemo.clearThrottled(videoId);
                    System.out.println("[StreamHandlers] " + videoId
                            + " throttle-memo cleared (URLs wieder sauber)");
                }
                logResolvePath(videoId, info);
                System.out.println("[NPE-timing] " + videoId + " total-resolve "
                        + (System.currentTimeMillis() - tResolve0) + "ms"
                        + " (nsig/post = total - max(VR,ANDROID) from the [NPE-timing] fetch lines)");
                return info;
            } catch (Exception e) {
                if (e instanceof GeographicRestrictionException) {
                    return null;
                }
                transaction.setThrowable(e);
                ExceptionUtils.rethrow(e);
            } finally {
                if (light) YoutubeStreamExtractor.SKIP_NEXT_FOR_THREAD.remove();
                transaction.finish();
            }
            return null;
        });

        final var futureLbryId = Multithreading.supplyAsync(() -> {
            Sentry.setExtra("videoId", videoId);
            try {
                return LbryHelper.getLBRYId(videoId);
            } catch (Exception e) {
                ExceptionHandler.handle(e);
            }
            return null;
        });

        final var futureLBRY = Multithreading.supplyAsync(() -> {
            Sentry.setExtra("videoId", videoId);
            ITransaction transaction = Sentry.startTransaction("LBRY Stream fetch", "fetch");
            try {
                var childTask = transaction.startChild("fetch", "LBRY ID fetch");
                String lbryId = futureLbryId.get(2, TimeUnit.SECONDS);
                Sentry.setExtra("lbryId", lbryId);
                childTask.finish();

                return LbryHelper.getLBRYStreamURL(lbryId);
            } catch (TimeoutException ignored) {
            } catch (Exception e) {
                ExceptionHandler.handle(e);
            } finally {
                transaction.finish();
            }
            return null;
        });

        final var futureLBRYHls = Multithreading.supplyAsync(() -> {
            Sentry.setExtra("videoId", videoId);
            ITransaction transaction = Sentry.startTransaction("LBRY HLS fetch", "fetch");
            try {
                var childTask = transaction.startChild("fetch", "LBRY Stream URL fetch");
                String lbryUrl = futureLBRY.get(2, TimeUnit.SECONDS);
                Sentry.setExtra("lbryUrl", lbryUrl);
                childTask.finish();

                return LbryHelper.getLBRYHlsUrl(lbryUrl);
            } catch (TimeoutException ignored) {
            } catch (Exception e) {
                ExceptionHandler.handle(e);
            } finally {
                transaction.finish();
            }
            return null;
        });

        final var futureDislikeRating = Multithreading.supplyAsync(() -> {
            Sentry.setExtra("videoId", videoId);
            ITransaction transaction = Sentry.startTransaction("Dislike Rating", "fetch");
            try {
                return RydHelper.getDislikeRating(videoId);
            } catch (Exception e) {
                ExceptionHandler.handle(e);
            } finally {
                transaction.finish();
            }
            return null;
        });

        StreamInfo info = null;
        Throwable exception = null;

        try {
            // 18s (was 10s): the degraded-audio WebEmbed fallback adds a second
            // full resolve on top of the 3 Android attempts (~5s) + WebEmbed
            // (~5s) -- the chain finishes ~10s, right at the old budget, so
            // WebEmbed-success videos (e.g. ARD/WDR uploads that only the embed
            // client serves audio for) raced the timeout and 500'd despite a good
            // result. Only the rare degraded path uses the headroom; healthy
            // videos still return in ~2s.
            info = futureStream.get(18, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            exception = e.getCause();
            if (
                // Some videos, like topic channel videos are not available everywhere
                    !(exception instanceof ContentNotAvailableException contentNotAvailableException && (contentNotAvailableException.getMessage().equals("This video is not available") || contentNotAvailableException.getMessage().equals("Got error: \"Video unavailable\""))) &&
                            !(e.getCause() instanceof GeographicRestrictionException)
            ) {
                // Serve-stale net (2026-07-05): the resolve died transiently (throttle
                // wave), but a previous resolve's googlevideo URLs are still
                // signature-valid (~6h) — serve those instead of erroring. cpn is
                // re-stamped per play downstream (synth swapCpn), so this is safe.
                Streams stale = SynthHlsHandlers.getStaleServableStreams(videoId);
                if (stale != null) {
                    System.out.println("[StreamHandlers] " + videoId + " resolve failed ("
                            + (exception == null ? "?" : exception.getClass().getSimpleName())
                            + ") -> serving STALE cached resolve");
                    return mapper.writeValueAsBytes(stale);
                }
                ExceptionUtils.rethrow(e);
            }
        } catch (TimeoutException te) {
            // The 18s outer budget fired (both cascade attempts hung on a throttle
            // wave). Same stale net as above; without it this propagated as a raw
            // 500 to the app ("Video nicht verfügbar").
            Streams stale = SynthHlsHandlers.getStaleServableStreams(videoId);
            if (stale != null) {
                System.out.println("[StreamHandlers] " + videoId
                        + " resolve timed out (18s) -> serving STALE cached resolve");
                return mapper.writeValueAsBytes(stale);
            }
            throw te;
        }

        if (info == null) {
            // We might be geo restricted

            if (Constants.MATRIX_TOKEN != null && Constants.GEO_RESTRICTION_CHECKER_URL != null) {

                List<String> allowedCountries = new ObjectArrayList<>();

                {
                    var restrictedTree = RequestUtils.sendGetJson(Constants.GEO_RESTRICTION_CHECKER_URL + "/api/region/check?video_id=" + videoId).get();
                    if (!restrictedTree.get("restricted").asBoolean()) {
                        assert exception != null;
                        throw (Exception) exception;
                    }
                    var it = restrictedTree.get("regions").elements();
                    while (it.hasNext()) {
                        var region = it.next();
                        allowedCountries.add(region.textValue());
                    }
                }

                if (allowedCountries.isEmpty())
                    throw new GeographicRestrictionException("Federated bypass failed, video not available in any region");

                MatrixHelper.sendEvent("video.piped.stream.bypass.request", new FederatedGeoBypassRequest(videoId, allowedCountries));

                var listener = new WaitingListener(10_000);
                GeoRestrictionBypassHelper.makeRequest(videoId, listener);
                listener.waitFor();
                FederatedGeoBypassResponse federatedGeoBypassResponse = GeoRestrictionBypassHelper.getResponse(videoId);

                if (federatedGeoBypassResponse == null)
                    throw new GeographicRestrictionException("Federated bypass failed, likely not authorized or no suitable instances found for country");

                Streams streams = federatedGeoBypassResponse.getData();

                // re-rewrite image URLs
                streams.chapters.forEach(chapter -> chapter.image = rewriteURL(chapter.image));
                streams.relatedStreams.forEach(contentItem -> {
                    if (contentItem instanceof StreamItem streamItem) {
                        streamItem.thumbnail = rewriteURL(streamItem.thumbnail);
                        streamItem.uploaderAvatar = rewriteURL(streamItem.uploaderAvatar);
                    } else if (contentItem instanceof ChannelItem channelItem) {
                        channelItem.thumbnail = rewriteURL(channelItem.thumbnail);
                    } else if (contentItem instanceof PlaylistItem playlistItem) {
                        playlistItem.thumbnail = rewriteURL(playlistItem.thumbnail);
                    }
                });
                streams.subtitles.forEach(subtitle -> subtitle.url = rewriteURL(subtitle.url));
                streams.thumbnailUrl = rewriteURL(streams.thumbnailUrl);
                streams.uploaderAvatar = rewriteURL(streams.uploaderAvatar);

                String lbryId;

                try {
                    lbryId = futureLbryId.get(2, TimeUnit.SECONDS);
                } catch (Exception e) {
                    lbryId = null;
                }

                if (lbryId != null) {
                    streams.lbryId = lbryId;
                }

                String lbryURL;

                try {
                    lbryURL = futureLBRY.get(3, TimeUnit.SECONDS);
                } catch (Exception e) {
                    lbryURL = null;
                }

                if (lbryURL != null)
                    streams.videoStreams.add(0, new PipedStream(-1, lbryURL, "MP4", "LBRY", "video/mp4", false, -1));

                // Attempt to get dislikes calculating with the RYD API rating
                if (streams.dislikes < 0 && streams.likes >= 0) {
                    double rating;
                    try {
                        rating = futureDislikeRating.get(3, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        rating = -1;
                    }

                    if (rating > 1 && rating <= 5) {
                        streams.dislikes = Math.round(streams.likes * ((5 - rating) / (rating - 1)));
                    }
                }

                return mapper.writeValueAsBytes(streams);
            } else if (Constants.GEO_RESTRICTION_CHECKER_URL == null) {
                throw new GeographicRestrictionException("This instance does not have a geo restriction checker set in its configuration");
            }

            if (exception == null)
                throw new GeographicRestrictionException("Geo restricted content, this instance is not part of the Matrix Federation protocol");
            else
                throw (Exception) exception;
        }

        Streams streams = CollectionUtils.collectStreamInfo(info);

        // Resolve-Reuse: seed the synth-hls cache so a follow-up
        // /synth-hls/<id>/master build reuses this resolve instead of doing a
        // 2nd YouTube resolve (~1.2s saved on a cold tap, no extra YT load).
        // `info` is already throttle-checked + WebEmbed-upgraded above, so the
        // collected streams are URL-verified.
        SynthHlsHandlers.cacheStreams(videoId, streams, true, light, maxH, codecs);

        String lbryURL = null;

        try {
            lbryURL = futureLBRY.get(3, TimeUnit.SECONDS);
        } catch (Exception e) {
            // ignored
        }

        String lbryHlsURL = null;

        try {
            lbryHlsURL = futureLBRYHls.get(4, TimeUnit.SECONDS);
        } catch (Exception e) {
            // ignored
        }

        if (lbryHlsURL != null)
            streams.videoStreams.add(0, new PipedStream(-1, lbryHlsURL, "HLS", "LBRY HLS", "application/x-mpegurl", false, -1));

        if (lbryURL != null)
            streams.videoStreams.add(0, new PipedStream(-1, lbryURL, "MP4", "LBRY", "video/mp4", false, -1));

        long time = info.getUploadDate() != null ? info.getUploadDate().offsetDateTime().toInstant().toEpochMilli()
                : System.currentTimeMillis();

        if (info.getUploadDate() != null && System.currentTimeMillis() - time < TimeUnit.DAYS.toMillis(Constants.FEED_RETENTION)) {
            VideoHelpers.updateVideo(info.getId(), info, time);
            StreamInfo finalInfo = info;
            Multithreading.runAsync(() -> {
                try {
                    MatrixHelper.sendEvent("video.piped.stream.info", new FederatedVideoInfo(
                            finalInfo.getId(), StringUtils.substring(finalInfo.getUploaderUrl(), -24),
                            finalInfo.getName(),
                            finalInfo.getDuration(), finalInfo.getViewCount())
                    );
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        String lbryId;

        try {
            lbryId = futureLbryId.get(2, TimeUnit.SECONDS);
        } catch (Exception e) {
            lbryId = null;
        }

        streams.lbryId = lbryId;

        // Attempt to get dislikes calculating with the RYD API rating
        if (streams.dislikes < 0 && streams.likes >= 0) {
            double rating;
            try {
                rating = futureDislikeRating.get(3, TimeUnit.SECONDS);
            } catch (Exception e) {
                rating = -1;
            }

            if (rating > 1 && rating <= 5) {
                streams.dislikes = Math.round(streams.likes * ((5 - rating) / (rating - 1)));
            }
        }

        return mapper.writeValueAsBytes(streams);

    }

    public static byte[] resolveClipId(String clipId) throws Exception {

        final byte[] body = JsonWriter.string(prepareDesktopJsonBuilder(
                        getPreferredLocalization(), getPreferredContentCountry())
                        .value("url", "https://www.youtube.com/clip/" + clipId)
                        .done())
                .getBytes(UTF_8);

        final JsonObject jsonResponse = getJsonPostResponse("navigation/resolve_url",
                body, getPreferredLocalization());

        final String videoId = JsonUtils.getString(jsonResponse, "endpoint.watchEndpoint.videoId");

        return mapper.writeValueAsBytes(new VideoResolvedResponse(videoId));
    }

    public static byte[] commentsResponse(String videoId) throws Exception {

        Sentry.setExtra("videoId", videoId);

        CommentsInfo info = CommentsInfo.getInfo("https://www.youtube.com/watch?v=" + videoId);

        List<Comment> comments = new ObjectArrayList<>();

        info.getRelatedItems().forEach(comment -> {
            try {
                String repliespage = null;
                if (comment.getReplies() != null)
                    repliespage = mapper.writeValueAsString(comment.getReplies());

                comments.add(new Comment(comment.getUploaderName(), getLastThumbnail(comment.getUploaderAvatars()),
                        comment.getCommentId(), Optional.ofNullable(comment.getCommentText()).map(Description::getContent).orElse(null), comment.getTextualUploadDate(),
                        substringYouTube(comment.getUploaderUrl()), repliespage, comment.getLikeCount(), comment.getReplyCount(),
                        comment.isHeartedByUploader(), comment.isPinned(), comment.isUploaderVerified(), comment.hasCreatorReply(), comment.isChannelOwner()));
            } catch (JsonProcessingException e) {
                ExceptionHandler.handle(e);
            }
        });

        String nextpage = null;
        if (info.hasNextPage()) {
            Page page = info.getNextPage();
            nextpage = mapper.writeValueAsString(page);
        }

        CommentsPage commentsItem = new CommentsPage(comments, nextpage, info.isCommentsDisabled(), info.getCommentsCount());

        return mapper.writeValueAsBytes(commentsItem);

    }

    public static byte[] commentsPageResponse(String videoId, String prevpageStr) throws Exception {

        if (StringUtils.isEmpty(prevpageStr))
            ExceptionHandler.throwErrorResponse(new InvalidRequestResponse("nextpage is a required parameter"));

        Page prevpage = mapper.readValue(prevpageStr, Page.class);

        ListExtractor.InfoItemsPage<CommentsInfoItem> info = CommentsInfo.getMoreItems(YOUTUBE_SERVICE, "https://www.youtube.com/watch?v=" + videoId, prevpage);

        List<Comment> comments = new ObjectArrayList<>();

        info.getItems().forEach(comment -> {
            try {
                String repliespage = null;
                if (comment.getReplies() != null)
                    repliespage = mapper.writeValueAsString(comment.getReplies());

                comments.add(new Comment(comment.getUploaderName(), getLastThumbnail(comment.getUploaderAvatars()),
                        comment.getCommentId(), Optional.ofNullable(comment.getCommentText()).map(Description::getContent).orElse(null), comment.getTextualUploadDate(),
                        substringYouTube(comment.getUploaderUrl()), repliespage, comment.getLikeCount(), comment.getReplyCount(),
                        comment.isHeartedByUploader(), comment.isPinned(), comment.isUploaderVerified(), comment.hasCreatorReply(), comment.isChannelOwner()));
            } catch (JsonProcessingException e) {
                ExceptionHandler.handle(e);
            }
        });

        String nextpage = null;
        if (info.hasNextPage()) {
            Page page = info.getNextPage();
            nextpage = mapper.writeValueAsString(page);
        }

        CommentsPage commentsItem = new CommentsPage(comments, nextpage, false, -1);

        return mapper.writeValueAsBytes(commentsItem);

    }

    /**
     * HEAD-Test gegen die clen/2-Mitte der hoechsten Qualitaets-Video-URL.
     * Returns true wenn googlevideo 403 antwortet (= per-IP-pattern-throttle
     * fuer deep byte-range-fetches aktiv).
     *
     * Timeout 2s connect + 3s read damit der Test bei langsamem upstream
     * den Stream-Extract nicht zu lange blockt. Bei Timeout/Exception:
     * false (= trust the streams, defensiver default).
     *
     * Skipt Videos mit contentLength < 10MB (= zu kurz fuer relevanten
     * Throttle-Risk) und Videos ohne erreichbare URL im VideoStream.
     */
    /// Logs which resolve path/client actually produced the served streams
    /// (ANDROID_VR / WEB_EMBEDDED_PLAYER / TVHTML5 / ...), or SABR-ONLY when the
    /// resolve yielded no direct-URL streams at all (= the day YouTube forces
    /// SABR for this client). One line per resolve for observability.
    private static void logResolvePath(String videoId, StreamInfo info) {
        if (info == null) return;
        String client;
        try {
            String url = null;
            if (!info.getAudioStreams().isEmpty()) url = info.getAudioStreams().get(0).getContent();
            else if (!info.getVideoStreams().isEmpty()) url = info.getVideoStreams().get(0).getContent();
            else if (!info.getVideoOnlyStreams().isEmpty()) url = info.getVideoOnlyStreams().get(0).getContent();
            if (url == null || url.isEmpty()) {
                client = "SABR-ONLY(no-direct-urls)";
            } else {
                java.util.regex.Matcher m = java.util.regex.Pattern
                        .compile("[?&]c=([A-Z_0-9]+)").matcher(url);
                client = m.find() ? m.group(1) : "no-c-param";
            }
        } catch (Exception e) {
            client = "err:" + e.getClass().getSimpleName();
        }
        System.out.println("[ResolvePath] " + videoId + " -> " + client);
    }

    private static boolean isVideoStreamThrottled(StreamInfo info) {
        if (info == null) return false;
        VideoStream best = null;
        for (VideoStream vs : info.getVideoOnlyStreams()) {
            if (best == null
                || (vs.getBitrate() > 0 && vs.getBitrate() > best.getBitrate())) {
                best = vs;
            }
        }
        if (best == null) return false;
        String url = best.getContent();
        if (url == null || url.isEmpty() || !url.startsWith("http")) return false;
        long clen = best.getItagItem() != null
            ? best.getItagItem().getContentLength() : 0;
        if (clen < 10_000_000L) return false;
        try {
            long offset = clen / 2;
            // Probe via the SAME egress family the resolve used (EgressManager),
            // NOT the JVM default (preferIPv6): a v4-locked URL probed from v6
            // returns a false 403 and trips a spurious 503. This closes the
            // v4<->v6 inconsistency (resolves use reqwest4j/activeEgress; this
            // probe used raw HttpURLConnection = JVM default family).
            var resp = rocks.kavin.reqwest4j.ReqwestUtils.fetchWithProxy(
                    url, "GET", new byte[0],
                    java.util.Map.of("Range", "bytes=" + offset + "-" + (offset + 1000)),
                    me.kavin.piped.utils.EgressManager.activeEgress()).join();
            return resp.status() == 403;
        } catch (Exception e) {
            return false;
        }
    }

    // Playability probe: can the best video-only URL serve a SEQUENTIAL range
    // from offset 0? googlevideo 403s deep mid-file seeks (see
    // isVideoStreamThrottled's clen/2 probe) during transient throttle waves
    // while still serving sequential ranges from the start — which is exactly
    // how the yt-proxy downloader (and yt-dlp) actually stream. A true here
    // means the video PLAYS via the direct URLs (yt-proxy fills sequentially),
    // so it must not be downgraded to the capped SABR fallback. Same egress +
    // a real Chrome UA as the yt-proxy fetch so the probe mirrors playback.
    private static boolean canServeSequentially(StreamInfo info) {
        if (info == null) return false;
        VideoStream best = null;
        for (VideoStream vs : info.getVideoOnlyStreams()) {
            if (best == null
                || (vs.getBitrate() > 0 && vs.getBitrate() > best.getBitrate())) {
                best = vs;
            }
        }
        if (best == null) return false;
        String url = best.getContent();
        if (url == null || url.isEmpty() || !url.startsWith("http")) return false;
        try {
            var resp = rocks.kavin.reqwest4j.ReqwestUtils.fetchWithProxy(
                    url, "GET", new byte[0],
                    java.util.Map.of("Range", "bytes=0-65535",
                        "User-Agent",
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        + "AppleWebKit/537.36 (KHTML, like Gecko) "
                        + "Chrome/131.0.0.0 Safari/537.36"),
                    me.kavin.piped.utils.EgressManager.activeEgress()).join();
            final int s = resp.status();
            return s == 206 || s == 200;
        } catch (Exception e) {
            return false;
        }
    }


    /// Drossel-Marke setzen und Text fuers Log liefern (s. SabrCache.isSabrMode:
    /// der Playlist-Bau bevorzugt danach den Cache).
    private static String throttleNoted(String videoId) {
        SabrCache.noteThrottled(videoId);
        return "URLs throttled (HEAD=403 auf clen/2)";
    }
}
