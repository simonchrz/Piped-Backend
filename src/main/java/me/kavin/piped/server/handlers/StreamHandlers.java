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
    private static final java.util.Set<String> KNOWN_AUDIO_ZERO =
            java.util.concurrent.ConcurrentHashMap.newKeySet();

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
                if (KNOWN_AUDIO_ZERO.contains(videoId)) {
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
                            KNOWN_AUDIO_ZERO.remove(videoId);
                        }
                    } catch (Exception e) {
                        KNOWN_AUDIO_ZERO.remove(videoId);
                    } finally {
                        YoutubeStreamExtractor.FORCE_WEB_EMBED_FOR_THREAD.remove();
                    }
                }

                // NPE returns degraded streams (1 muxed video, 0 audio) non-deterministically
                // for some videos. Up to 3 attempts; accept the first that has both audio
                // and video. Same retry-on-degraded pattern as SynthHlsHandlers.fetchStreams.
                if (info == null) {
                    for (int attempt = 0; attempt < 3; attempt++) {
                        info = StreamInfo.getInfo("https://www.youtube.com/watch?v=" + videoId);
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
                // One HEAD probe decides the suspect path (degraded short-circuits it,
                // so audio0 videos skip the probe). Healthy videos: exactly this one
                // probe, same as before; the final liveness gate below only re-probes
                // when this is true, keeping the extra HEAD off the fast path.
                boolean throttleSuspect = info != null && (degraded || isVideoStreamThrottled(info));
                if (throttleSuspect) {
                    System.out.println("[StreamHandlers] " + videoId + " "
                            + (degraded ? "degraded (audio=0)" : "URLs throttled (HEAD=403 auf clen/2)")
                            + ", retry mit force-WebEmbed");
                    YoutubeStreamExtractor.FORCE_WEB_EMBED_FOR_THREAD.set(Boolean.TRUE);
                    try {
                        StreamInfo retryInfo = StreamInfo.getInfo(
                            "https://www.youtube.com/watch?v=" + videoId);
                        if (retryInfo != null && !retryInfo.getAudioStreams().isEmpty()
                                && (!retryInfo.getVideoStreams().isEmpty()
                                    || !retryInfo.getVideoOnlyStreams().isEmpty())) {
                            info = retryInfo;
                            if (degraded) KNOWN_AUDIO_ZERO.add(videoId); // remember for next re-resolve
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
                // Final segment-liveness gate. If, after the WebEmbed upgrade, the
                // served segment URLs are STILL 403 (googlevideo throttle storm),
                // don't hand the app a 200 with dead URLs (the bug from the audio0
                // report) — surface a distinct 503 ThrottledResponse so the app shows
                // "YouTube is rate-limiting" instead of a generic timeout. Scoped to
                // the suspect path so healthy videos pay no extra probe.
                if (throttleSuspect && info != null && isVideoStreamThrottled(info)) {
                    System.out.println("[StreamHandlers] " + videoId
                            + " STILL throttled after WebEmbed upgrade (segments 403) -> 503 throttled");
                    ExceptionHandler.throwErrorResponse(new ThrottledResponse(
                            "YouTube is rate-limiting playback for this video (segment URLs return "
                            + "403). Transient googlevideo throttle - try again shortly."));
                }
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
                ExceptionUtils.rethrow(e);
            }
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
            HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setRequestMethod("HEAD");
            long offset = clen / 2;
            conn.setRequestProperty("Range", "bytes=" + offset + "-" + (offset + 1000));
            conn.setConnectTimeout(2000);
            conn.setReadTimeout(3000);
            int code = conn.getResponseCode();
            conn.disconnect();
            return code == 403;
        } catch (Exception e) {
            return false;
        }
    }

}
