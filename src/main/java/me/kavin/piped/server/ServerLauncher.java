package me.kavin.piped.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.activej.config.Config;
import io.activej.http.*;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.launchers.http.MultithreadedHttpServerLauncher;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import me.kavin.piped.consts.Constants;
import me.kavin.piped.server.handlers.*;
import me.kavin.piped.server.handlers.auth.AuthPlaylistHandlers;
import me.kavin.piped.server.handlers.auth.FeedHandlers;
import me.kavin.piped.server.handlers.auth.HistoryHandlers;
import me.kavin.piped.server.handlers.auth.StorageHandlers;
import me.kavin.piped.server.handlers.auth.UserHandlers;
import me.kavin.piped.utils.*;
import me.kavin.piped.utils.resp.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.hibernate.Session;
import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.activej.config.converter.ConfigConverters.ofInetSocketAddress;
import static io.activej.http.HttpHeaders.*;
import static io.activej.http.HttpMethod.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static me.kavin.piped.consts.Constants.mapper;

public class ServerLauncher extends MultithreadedHttpServerLauncher {

    private static final HttpHeader FILE_NAME = HttpHeaders.of("x-file-name");
    private static final HttpHeader LAST_ETAG = HttpHeaders.of("x-last-etag");

    // All routes share one virtual-thread executor. YT stream-resolves block on
    // I/O inside synchronized sections (synth-hls cache, NPE PoToken/nsig, OkHttp
    // pool) and PIN their carrier platform-thread. With only availableProcessors()
    // carriers (4 on the Pi5), a few stuck resolves under a googlevideo IP-block
    // pin them all -> the entire backend (incl. /healthcheck + non-YT routes)
    // starves. Cap concurrent video-resolves (/streams, /synth-hls) BELOW the
    // carrier count so a YT-block can never take the whole backend down; excess
    // resolves fast-reject with 503 instead of piling up and pinning carriers.
    private static final Semaphore YT_RESOLVE_LIMITER =
            new Semaphore(Math.max(2, Runtime.getRuntime().availableProcessors() / 2));

    /** tryAcquire a resolve slot (500ms), false on saturation/interrupt.
     *  FOREGROUND path (tap / playback): may use any of the slots. */
    private static boolean ytResolveAcquire() {
        return ytResolveAcquire("?");
    }

    /// `who` = Route, die abgelehnt wurde. Ohne diese Zuordnung sieht man nur DASS
    /// gedrosselt wurde, nicht WEN es trifft — und ein 503 auf einem Playlist-/
    /// Segment-Abruf killt die Wiedergabe, waehrend einer auf /streams nur einen
    /// Retry kostet.
    private static boolean ytResolveAcquire(String who) {
        return ytResolveAcquire(who, 500);
    }

    /// AUSLIEFERUNGS-Routen (Playlist/Segment) warten deutlich laenger als
    /// Resolves: ein 503 auf /streams kostet einen Retry, ein 503 auf einer
    /// Playlist oder einem Segment BRICHT die laufende Wiedergabe ab
    /// (app-seitig -1008/-16849, „Video nicht abspielbar"). Lieber 5s warten.
    private static boolean ytResolveAcquire(String who, int timeoutMs) {
        try {
            final boolean got = YT_RESOLVE_LIMITER.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS);
            if (!got) System.out.println("[Limiter] YT-Resolve-Slot belegt -> 503 (" + who
                    + ", " + timeoutMs + "ms)");
            return got;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    // Background (prefetch) resolves must additionally hold YT_BG_LIMITER, which is
    // sized to leave ≥1 of the YT_RESOLVE_LIMITER slots ALWAYS free for a foreground
    // tap. Without this, a burst of ~4 prefetch-resolves/tap filled both slots and the
    // tap-resolve waited ~850ms median in the queue (app-dev correlation 2026-06-07).
    private static final Semaphore YT_BG_LIMITER =
            new Semaphore(Math.max(1, (Math.max(2, Runtime.getRuntime().availableProcessors() / 2)) - 1));

    /** Background prefetch resolve slot: hold the bg permit (caps bg concurrency so a
     *  foreground slot stays reserved) AND a main slot. 503 on saturation = fine for
     *  best-effort prefetch. */
    private static boolean ytResolveAcquireBackground() {
        try {
            if (!YT_BG_LIMITER.tryAcquire(500, TimeUnit.MILLISECONDS)) return false;
            if (!YT_RESOLVE_LIMITER.tryAcquire(500, TimeUnit.MILLISECONDS)) {
                YT_BG_LIMITER.release();
                return false;
            }
            return true;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static void ytResolveReleaseBackground() {
        YT_RESOLVE_LIMITER.release();
        YT_BG_LIMITER.release();
    }

    // Total per-resolve deadline. A throttled resolve otherwise stacks
    // sequential bounded fallbacks (Android cascade 6s + WebEmbed ~20s + HEAD
    // 5s + force-WebEmbed retry ~20s) into ~30-50s; this caps the wait so the
    // client gets a fast error instead of a stuck spinner, and the caller's
    // semaphore slot is freed on return. Normal resolves are ~2s (≤~8s with a
    // WebEmbed fallback), well under the budget.
    private static final int RESOLVE_BUDGET_S = 12;

    /** Run a YT resolve under RESOLVE_BUDGET_S; on timeout cancel + throw so
     *  the handler's catch returns an error fast. Runs on the same cached
     *  virtual-thread executor; the abandoned task is bounded by the
     *  Downloader's own 10s per-call timeout. */
    private static byte[] withResolveBudget(Callable<byte[]> resolve) throws Exception {
        Future<byte[]> f = Multithreading.getCachedExecutor().submit(resolve);
        try {
            return f.get(RESOLVE_BUDGET_S, TimeUnit.SECONDS);
        } catch (TimeoutException te) {
            f.cancel(true);
            throw new RuntimeException("YT resolve exceeded " + RESOLVE_BUDGET_S + "s budget (throttled?)");
        } catch (ExecutionException ee) {
            final Throwable c = ee.getCause();
            if (c instanceof Exception) throw (Exception) c;
            throw ee;
        }
    }

    @Provides
    Executor executor() {
        return Multithreading.getCachedExecutor();
    }

    @Provides
    AsyncServlet mainServlet(Executor executor) {

        RoutingServlet router = RoutingServlet.create()
                .map(GET, "/healthcheck", AsyncServlet.ofBlocking(executor, request -> {
                    try (Session ignored = DatabaseSessionFactory.createSession()) {
                        return getRawResponse("OK".getBytes(UTF_8), "text/plain", "no-store");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(GET, "/config", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        return getJsonResponse(GenericHandlers.configResponse(), "public, max-age=86400");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                }))
                .map(GET, "/version", AsyncServlet.ofBlocking(executor, request -> getRawResponse(Constants.VERSION.getBytes(UTF_8), "text/plain", "no-store")))
                // DEBUG: takes ?url=<videoplayback url> and returns the URL after
                // URLUtils.rewriteVideoURL (incl. nsig-decoder n-rewrite if enabled).
                .map(GET, "/debug/test_nsig", AsyncServlet.ofBlocking(executor, request -> {
                    String url = request.getQueryParameter("url");
                    if (url == null) return getJsonResponse("{\"error\":\"missing url\"}".getBytes(UTF_8), "no-store");
                    String rewritten = me.kavin.piped.utils.URLUtils.rewriteVideoURL(url, java.util.Map.of());
                    String body = "{\"input\":\"" + url.replace("\\", "\\\\").replace("\"", "\\\"") + "\",\"rewritten\":\"" + (rewritten == null ? "" : rewritten).replace("\\", "\\\\").replace("\"", "\\\"") + "\"}";
                    return getJsonResponse(body.getBytes(UTF_8), "no-store");
                }))
                .map(HttpMethod.OPTIONS, "/*", request -> HttpResponse.ofCode(200))
                .map(GET, "/webhooks/pubsub", AsyncServlet.ofBlocking(executor, request -> {
                    var topic = request.getQueryParameter("hub.topic");
                    if (topic != null)
                        Multithreading.runAsyncLimited(() -> {
                            String channelId = StringUtils.substringAfter(topic, "channel_id=");
                            PubSubHelper.updatePubSub(channelId);
                        });

                    var challenge = request.getQueryParameter("hub.challenge");
                    return HttpResponse.ok200()
                            .withPlainText(Objects.requireNonNullElse(challenge, "ok"));
                })).map(POST, "/webhooks/pubsub", AsyncServlet.ofBlocking(executor, request -> {
                    try {

                        PubSubHandlers.handlePubSub(request.loadBody().getResult().asArray());

                        return HttpResponse.ofCode(204);

                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(GET, "/sponsors/:videoId", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        return getJsonResponse(
                                SponsorBlockUtils.getSponsors(request.getPathParameter("videoId"),
                                        request.getQueryParameter("category"), request.getQueryParameter("actionType")).getBytes(UTF_8),
                                "public, max-age=3600");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(GET, "/dearrow", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        var videoIds = getArray(request.getQueryParameter("videoIds"));

                        return getJsonResponse(
                                SponsorBlockUtils.getDeArrowedInfo(videoIds)
                                        .thenApplyAsync(json -> {
                                            try {
                                                return mapper.writeValueAsBytes(json);
                                            } catch (JsonProcessingException e) {
                                                throw new RuntimeException(e);
                                            }
                                        }).get(),
                                "public, max-age=3600");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(GET, "/streams/:videoId", AsyncServlet.ofBlocking(executor, request -> {
                    // priority=1 = foreground (tap): full pool. Otherwise background
                    // (prefetch): capped so a foreground slot stays reserved.
                    final boolean priority = "1".equals(request.getQueryParameter("priority"));
                    // Same rendition selection as the synth-hls URL, so the sidx-prewarm
                    // warms exactly the rendition the tap will request (cold-tap preserved).
                    final int maxH = SynthHlsHandlers.parseMaxH(request.getQueryParameter("maxh"));
                    final String[] codecs = SynthHlsHandlers.parseCodecs(request.getQueryParameter("codecs"));
                    final boolean light = "1".equals(request.getQueryParameter("light"));
                    if (priority ? !ytResolveAcquire() : !ytResolveAcquireBackground())
                        return io.activej.http.HttpResponse.ofCode(503);
                    try {
                        return getJsonResponse(withResolveBudget(
                                () -> StreamHandlers.streamsResponse(request.getPathParameter("videoId"),
                                        light, maxH, codecs)),
                                "public, s-maxage=21540, max-age=30", true);
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    } finally {
                        if (priority) YT_RESOLVE_LIMITER.release();
                        else ytResolveReleaseBackground();
                    }
                })).map(GET, "/synth-hls/:videoId/:filename", AsyncServlet.ofBlocking(executor, request -> {
                    // Aus dem Cache ausliefern ist KEIN Resolve → kein Slot. Sonst
                    // verdraengen ein Hintergrund-Prefetch + ein Vordergrund-Resolve
                    // den Playlist-Abruf desselben Taps (503 → -16849 in der App).
                    final boolean needsSlot = !SynthHlsHandlers.canServeWithoutResolve(
                            request.getPathParameter("videoId"));
                    if (needsSlot && !ytResolveAcquire("synth-hls", 5000))
                        return io.activej.http.HttpResponse.ofCode(503);
                    // Der Playlist-Bau darf den Slot FRUEH zurueckgeben, sobald er nur
                    // noch auf lokale SABR-Daten wartet (s. SynthHlsHandlers.SLOT_RELEASE).
                    final java.util.concurrent.atomic.AtomicBoolean slotHeld =
                            new java.util.concurrent.atomic.AtomicBoolean(needsSlot);
                    SynthHlsHandlers.SLOT_RELEASE.set(() -> {
                        if (slotHeld.compareAndSet(true, false)) YT_RESOLVE_LIMITER.release();
                    });
                    try {
                        String videoId = request.getPathParameter("videoId");
                        String filename = request.getPathParameter("filename");
                        // Rendition selection (default 1080/avc). masterPlaylist re-encodes
                        // these onto the variant URIs so video<i>.m3u8 serves the same pick.
                        final int maxH = SynthHlsHandlers.parseMaxH(request.getQueryParameter("maxh"));
                        final String[] codecs = SynthHlsHandlers.parseCodecs(request.getQueryParameter("codecs"));
                        byte[] body;
                        if (filename.equals("master.m3u8")) {
                            body = withResolveBudget(() -> SynthHlsHandlers.masterPlaylist(videoId, maxH, codecs));
                        } else if (filename.equals("audio.m3u8")) {
                            body = withResolveBudget(() -> SynthHlsHandlers.audioPlaylist(videoId));
                        } else if (filename.startsWith("video") && filename.endsWith(".m3u8")) {
                            final int idx = Integer.parseInt(filename.substring(5, filename.length() - 5));
                            body = withResolveBudget(() -> SynthHlsHandlers.videoPlaylist(videoId, idx, maxH, codecs));
                        } else {
                            return io.activej.http.HttpResponse.ofCode(404);
                        }
                        return getRawResponse(body, "application/vnd.apple.mpegurl", "public, max-age=300");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    } finally {
                        SynthHlsHandlers.SLOT_RELEASE.remove();
                        if (slotHeld.compareAndSet(true, false)) YT_RESOLVE_LIMITER.release();
                    }
                })).map(GET, "/yt-proxy/*",
                        request -> YtProxyHandlers.handleAsync(request, executor)
                ).map(HttpMethod.HEAD, "/yt-proxy/*", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        return YtProxyHandlers.handle(request);
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(GET, "/sabr/:videoId/:itag", AsyncServlet.ofBlocking(executor, request -> {
                    final String sabrVid = request.getPathParameter("videoId");
                    final int sabrItag;
                    try {
                        sabrItag = Integer.parseInt(request.getPathParameter("itag"));
                    } catch (NumberFormatException nfe) {
                        return io.activej.http.HttpResponse.ofCode(400);
                    }
                    // Liegt die Datei schon da, ist das hier reines Range-Lesen — KEIN
                    // Resolve-Slot. Sonst löst ensureFile eine SABR-Session aus (echte
                    // YT-Arbeit) und wird wie bisher gedeckelt.
                    // ⚠️ Ohne diese Ausnahme verhungern die Segment-Abrufe hinter den
                    // beiden Playlist-Buildern, die während des Downloads beide Slots
                    // halten → 503 → AVPlayer -16849 direkt nach dem Start.
                    final boolean sabrCached = me.kavin.piped.utils.sabr.SabrCache.isCached(sabrVid, sabrItag);
                    if (!sabrCached && !ytResolveAcquire("sabr-seg", 5000))
                        return io.activej.http.HttpResponse.ofCode(503);
                    try {
                        return me.kavin.piped.utils.sabr.SabrCache.handle(
                                sabrVid, sabrItag,
                                request.getHeader(io.activej.http.HttpHeaders.RANGE), false);
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    } finally {
                        if (!sabrCached) YT_RESOLVE_LIMITER.release();
                    }
                })).map(HttpMethod.HEAD, "/sabr/:videoId/:itag", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        final int itag = Integer.parseInt(request.getPathParameter("itag"));
                        return me.kavin.piped.utils.sabr.SabrCache.handle(
                                request.getPathParameter("videoId"), itag,
                                request.getHeader(io.activej.http.HttpHeaders.RANGE), true);
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(GET, "/clips/:clipId", AsyncServlet.ofBlocking(executor, request -> {
                    // resolveClipId resolves the underlying video = a YT player-resolve,
                    // same starvation risk as /streams + /synth-hls. Cap it on the same
                    // limiter so a YT block cannot pin carriers via /clips either.
                    if (!ytResolveAcquire())
                        return io.activej.http.HttpResponse.ofCode(503);
                    try {
                        return getJsonResponse(withResolveBudget(
                                () -> StreamHandlers.resolveClipId(request.getPathParameter("clipId"))),
                                "public, max-age=31536000, immutable");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    } finally {
                        YT_RESOLVE_LIMITER.release();
                    }
                })).map(GET, "/channel/:channelId", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        return getJsonResponse(
                                ChannelHandlers.channelResponse("channel/" + request.getPathParameter("channelId")),
                                "public, max-age=600", true);
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(GET, "/c/:name", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        return getJsonResponse(ChannelHandlers.channelResponse("c/" + request.getPathParameter("name")),
                                "public, max-age=600", true);
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(GET, "/user/:name", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        return getJsonResponse(
                                ChannelHandlers.channelResponse("user/" + request.getPathParameter("name")),
                                "public, max-age=600", true);
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(GET, "/@/:handle", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        return getJsonResponse(
                                ChannelHandlers.channelResponse("@" + request.getPathParameter("handle")),
                                "public, max-age=600", true);
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(GET, "/nextpage/channel/:channelId", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        return getJsonResponse(ChannelHandlers.channelPageResponse(request.getPathParameter("channelId"),
                                request.getQueryParameter("nextpage")), "public, max-age=3600", true);
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(GET, "/channels/tabs", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        String nextpage = request.getQueryParameter("nextpage");
                        if (StringUtils.isEmpty(nextpage))
                            return getJsonResponse(ChannelHandlers.channelTabResponse(request.getQueryParameter("data")), "public, max-age=3600", true);
                        else
                            return getJsonResponse(ChannelHandlers.channelTabPageResponse(request.getQueryParameter("data"), nextpage), "public, max-age=3600", true);
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(GET, "/playlists/:playlistId", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        var playlistId = request.getPathParameter("playlistId");
                        var cache = StringUtils.isBlank(playlistId) || playlistId.length() != 36 ?
                                "public, max-age=600" : "private";
                        return getJsonResponse(me.kavin.piped.server.handlers.PlaylistHandlers.playlistResponse(playlistId), cache, true);
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(GET, "/nextpage/playlists/:playlistId", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        return getJsonResponse(
                                me.kavin.piped.server.handlers.PlaylistHandlers.playlistPageResponse(request.getPathParameter("playlistId"),
                                        request.getQueryParameter("nextpage")),
                                "public, max-age=3600", true);
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(GET, "/rss/playlists/:playlistId", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        return getRawResponse(
                                me.kavin.piped.server.handlers.PlaylistHandlers.playlistRSSResponse(request.getPathParameter("playlistId")),
                                "application/atom+xml", "public, s-maxage=600");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                    // TODO: Replace with opensearch, below, for caching reasons.
                })).map(GET, "/suggestions", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        return getJsonResponse(SearchHandlers.suggestionsResponse(request.getQueryParameter("query")),
                                "public, max-age=600");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(GET, "/opensearch/suggestions", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        return getJsonResponse(
                                SearchHandlers.opensearchSuggestionsResponse(request.getQueryParameter("query")),
                                "public, max-age=600");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(GET, "/search", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        return getJsonResponse(SearchHandlers.searchResponse(request.getQueryParameter("q"),
                                request.getQueryParameter("filter")), "public, max-age=600", true);
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(GET, "/nextpage/search", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        return getJsonResponse(
                                SearchHandlers.searchPageResponse(request.getQueryParameter("q"),
                                        request.getQueryParameter("filter"), request.getQueryParameter("nextpage")),
                                "public, max-age=3600", true);
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(GET, "/trending", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        return getJsonResponse(TrendingHandlers.trendingResponse(request.getQueryParameter("region")),
                                "public, max-age=3600", true);
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(GET, "/comments/:videoId", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        return getJsonResponse(StreamHandlers.commentsResponse(request.getPathParameter("videoId")),
                                "public, max-age=1200", true);
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(GET, "/nextpage/comments/:videoId", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        return getJsonResponse(StreamHandlers.commentsPageResponse(request.getPathParameter("videoId"),
                                request.getQueryParameter("nextpage")), "public, max-age=3600", true);
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(POST, "/register", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        LoginRequest body = mapper.readValue(request.loadBody().getResult().asArray(),
                                LoginRequest.class);
                        return getJsonResponse(UserHandlers.registerResponse(body.username, body.password),
                                "private");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(POST, "/login", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        LoginRequest body = mapper.readValue(request.loadBody().getResult().asArray(),
                                LoginRequest.class);
                        return getJsonResponse(UserHandlers.loginResponse(body.username, body.password), "private");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(POST, "/subscribe", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        SubscriptionUpdateRequest body = mapper
                                .readValue(request.loadBody().getResult().asArray(), SubscriptionUpdateRequest.class);
                        return getJsonResponse(
                                FeedHandlers.subscribeResponse(request.getHeader(AUTHORIZATION), body.channelId),
                                "private");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(POST, "/unsubscribe", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        SubscriptionUpdateRequest body = mapper
                                .readValue(request.loadBody().getResult().asArray(), SubscriptionUpdateRequest.class);
                        return getJsonResponse(
                                FeedHandlers.unsubscribeResponse(request.getHeader(AUTHORIZATION), body.channelId),
                                "private");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(GET, "/subscribed", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        return getJsonResponse(FeedHandlers.isSubscribedResponse(request.getHeader(AUTHORIZATION),
                                request.getQueryParameter("channelId")), "private");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(GET, "/feed", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        String beforeStr = request.getQueryParameter("before");
                        String limitStr = request.getQueryParameter("limit");
                        Long before = null;
                        Integer limit = null;
                        if (beforeStr != null && !beforeStr.isEmpty()) {
                            try { before = Long.parseLong(beforeStr); } catch (NumberFormatException ignored) {}
                        }
                        if (limitStr != null && !limitStr.isEmpty()) {
                            try { limit = Integer.parseInt(limitStr); } catch (NumberFormatException ignored) {}
                        }
                        return getJsonResponse(FeedHandlers.feedResponse(request.getQueryParameter("authToken"), before, limit),
                                "private");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(GET, "/feed/rss", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        return getRawResponse(FeedHandlers.feedResponseRSS(request.getQueryParameter("authToken"),
                                        request.getQueryParameter("filter")),
                                "application/atom+xml", "public, s-maxage=120");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(GET, "/feed/unauthenticated", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        return getJsonResponse(FeedHandlers.unauthenticatedFeedResponse(
                                getArray(request.getQueryParameter("channels"))
                        ), "public, s-maxage=120");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(POST, "/feed/unauthenticated", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        String[] subscriptions = mapper.readValue(request.loadBody().getResult().asArray(),
                                String[].class);
                        return getJsonResponse(FeedHandlers.unauthenticatedFeedResponse(subscriptions), "public, s-maxage=120");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(GET, "/feed/unauthenticated/rss", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        return getRawResponse(FeedHandlers.unauthenticatedFeedResponseRSS(
                                getArray(request.getQueryParameter("channels")),
                                request.getQueryParameter("filter")
                        ), "application/atom+xml", "public, s-maxage=120");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(POST, "/import", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        String[] subscriptions = mapper.readValue(request.loadBody().getResult().asArray(),
                                String[].class);
                        return getJsonResponse(FeedHandlers.importResponse(request.getHeader(AUTHORIZATION),
                                subscriptions, Boolean.parseBoolean(request.getQueryParameter("override"))), "private");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(GET, "/user/history", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        String limitStr = request.getQueryParameter("limit");
                        String beforeStr = request.getQueryParameter("before");
                        Integer limit = (limitStr != null && !limitStr.isEmpty()) ? Integer.parseInt(limitStr) : null;
                        Long before = (beforeStr != null && !beforeStr.isEmpty()) ? Long.parseLong(beforeStr) : null;
                        return getJsonResponse(HistoryHandlers.getHistoryResponse(request.getHeader(AUTHORIZATION), limit, before), "private");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(POST, "/user/history", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        return getJsonResponse(HistoryHandlers.upsertHistoryResponse(request.getHeader(AUTHORIZATION),
                                request.loadBody().getResult().asArray()), "private");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(DELETE, "/user/history", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        return getJsonResponse(HistoryHandlers.deleteHistoryResponse(request.getHeader(AUTHORIZATION),
                                request.getQueryParameter("videoId")), "private");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(POST, "/import/playlist", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        var json = mapper.readTree(request.loadBody().getResult().asArray());
                        var playlistId = json.get("playlistId").textValue();
                        return getJsonResponse(AuthPlaylistHandlers.importPlaylistResponse(request.getHeader(AUTHORIZATION), playlistId), "private");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(GET, "/subscriptions", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        return getJsonResponse(FeedHandlers.subscriptionsResponse(request.getHeader(AUTHORIZATION)),
                                "private");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(GET, "/subscriptions/unauthenticated", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        return getJsonResponse(FeedHandlers.unauthenticatedSubscriptionsResponse(
                                Objects.requireNonNull(request.getQueryParameter("channels")).split(",")
                        ), "public, s-maxage=120");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(POST, "/subscriptions/unauthenticated", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        String[] subscriptions = mapper.readValue(request.loadBody().getResult().asArray(),
                                String[].class);
                        return getJsonResponse(FeedHandlers.unauthenticatedSubscriptionsResponse(subscriptions), "public, s-maxage=120");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(POST, "/user/playlists/create", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        var name = mapper.readTree(request.loadBody().getResult().asArray()).get("name").textValue();
                        return getJsonResponse(AuthPlaylistHandlers.createPlaylist(request.getHeader(AUTHORIZATION), name), "private");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(PATCH, "/user/playlists/description", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        var json = mapper.readTree(request.loadBody().getResult().asArray());
                        var playlistId = json.get("playlistId").textValue();
                        var description = json.get("description").textValue();
                        return getJsonResponse(
                                AuthPlaylistHandlers.editPlaylistDescriptionResponse(request.getHeader(AUTHORIZATION),
                                        playlistId, description),
                                "private");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(GET, "/user/playlists", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        return getJsonResponse(AuthPlaylistHandlers.playlistsResponse(request.getHeader(AUTHORIZATION)), "private");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(POST, "/user/playlists/add", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        var json = mapper.readTree(request.loadBody().getResult().asArray());
                        var playlistId = json.get("playlistId").textValue();
                        var videoIds = new ObjectArrayList<String>();
                        // backwards compatibility
                        var videoIdField = json.get("videoId");
                        if (videoIdField != null) {
                            videoIds.add(videoIdField.textValue());
                        }
                        var videoIdsField = json.get("videoIds");
                        if (videoIdsField != null) {
                            for (JsonNode node : videoIdsField) {
                                videoIds.add(node.textValue());
                            }
                        }

                        return getJsonResponse(AuthPlaylistHandlers.addToPlaylistResponse(request.getHeader(AUTHORIZATION), playlistId, videoIds), "private");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(POST, "/user/playlists/remove", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        var json = mapper.readTree(request.loadBody().getResult().asArray());
                        var playlistId = json.get("playlistId").textValue();
                        var index = json.get("index").intValue();
                        return getJsonResponse(AuthPlaylistHandlers.removeFromPlaylistResponse(request.getHeader(AUTHORIZATION), playlistId, index), "private");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(POST, "/user/playlists/clear", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        var json = mapper.readTree(request.loadBody().getResult().asArray());
                        var playlistId = json.get("playlistId").textValue();
                        return getJsonResponse(AuthPlaylistHandlers.clearPlaylistResponse(request.getHeader(AUTHORIZATION), playlistId), "private");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(POST, "/user/playlists/rename", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        var json = mapper.readTree(request.loadBody().getResult().asArray());
                        var playlistId = json.get("playlistId").textValue();
                        var newName = json.get("newName").textValue();
                        return getJsonResponse(AuthPlaylistHandlers.renamePlaylistResponse(request.getHeader(AUTHORIZATION), playlistId, newName), "private");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(POST, "/user/playlists/delete", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        var json = mapper.readTree(request.loadBody().getResult().asArray());
                        var playlistId = json.get("playlistId").textValue();
                        return getJsonResponse(AuthPlaylistHandlers.deletePlaylistResponse(request.getHeader(AUTHORIZATION), playlistId), "private");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(GET, "/registered/badge", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        return HttpResponse.ofCode(302).withHeader(LOCATION, GenericHandlers.registeredBadgeRedirect())
                                .withHeader(CACHE_CONTROL, "public, max-age=30");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(POST, "/user/delete", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        DeleteUserRequest body = mapper.readValue(request.loadBody().getResult().asArray(),
                                DeleteUserRequest.class);
                        return getJsonResponse(UserHandlers.deleteUserResponse(request.getHeader(AUTHORIZATION), body.password),
                                "private");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(POST, "/logout", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        return getJsonResponse(UserHandlers.logoutResponse(request.getHeader(AUTHORIZATION)), "private");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(GET, "/storage/stat", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        var file = request.getQueryParameter("file");
                        return getJsonResponse(StorageHandlers.statFile(request.getHeader(AUTHORIZATION), file), "private");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(POST, "/storage/put", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        var data = request.loadBody().getResult().asArray();

                        String fileName = request.getHeader(FILE_NAME);
                        String etag = request.getHeader(LAST_ETAG);

                        return getJsonResponse(StorageHandlers.putFile(request.getHeader(AUTHORIZATION), fileName, etag, data), "private");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                })).map(GET, "/storage/get", AsyncServlet.ofBlocking(executor, request -> {
                    try {
                        var file = request.getQueryParameter("file");
                        return getRawResponse(StorageHandlers.getFile(request.getHeader(AUTHORIZATION), file), "application/octet-stream", "private");
                    } catch (Exception e) {
                        return getErrorResponse(e, request.getPath());
                    }
                }))
                .map(GET, "/", AsyncServlet.ofBlocking(executor, request -> HttpResponse.redirect302(Constants.FRONTEND_URL)));

        return new CustomServletDecorator(router);
    }

    private static String[] getArray(String s) {

        if (s == null) {
            ExceptionHandler.throwErrorResponse(new InvalidRequestResponse());
        }

        return s.split(",");
    }

    @Override
    protected Module getOverrideModule() {
        return new AbstractModule() {
            @Provides
            Config config() {
                return Config.create()
                        .with("http.listenAddresses",
                                Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(Constants.PORT)))
                        .with("bytebuf.useWatchdog", String.valueOf(true))
                        .with("workers", Constants.HTTP_WORKERS);
            }
        };
    }

    private @NotNull HttpResponse getJsonResponse(byte[] body, String cache) {
        return getJsonResponse(200, body, cache, false);
    }

    private @NotNull HttpResponse getJsonResponse(byte[] body, String cache, boolean prefetchProxy) {
        return getJsonResponse(200, body, cache, prefetchProxy);
    }

    private @NotNull HttpResponse getJsonResponse(int code, byte[] body, String cache) {
        return getJsonResponse(code, body, cache, false);
    }

    private @NotNull HttpResponse getJsonResponse(int code, byte[] body, String cache, boolean prefetchProxy) {
        return getRawResponse(code, body, "application/json", cache, prefetchProxy);
    }

    private @NotNull HttpResponse getRawResponse(byte[] body, String contentType, String cache) {
        return getRawResponse(200, body, contentType, cache, false);
    }

    private @NotNull HttpResponse getRawResponse(int code, byte[] body, String contentType, String cache,
                                                 boolean prefetchProxy) {
        HttpResponse response = HttpResponse.ofCode(code).withBody(body).withHeader(CONTENT_TYPE, contentType)
                .withHeader(CACHE_CONTROL, cache);
        if (prefetchProxy)
            response = response.withHeader(LINK, String.format("<%s>; rel=preconnect", Constants.IMAGE_PROXY_PART));
        return response;
    }

    private @NotNull HttpResponse getErrorResponse(Exception e, String path) {

        e = ExceptionHandler.handle(e, path);

        if (e instanceof ErrorResponse error) {
            return getJsonResponse(error.getCode(), error.getContent(), "private");
        }

        try {
            return getJsonResponse(500, mapper
                    .writeValueAsBytes(new StackTraceResponse(ExceptionUtils.getStackTrace(e), e.getMessage())), "private");
        } catch (JsonProcessingException ex) {
            return HttpResponse.ofCode(500);
        }
    }
}
