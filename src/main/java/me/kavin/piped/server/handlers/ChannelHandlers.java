package me.kavin.piped.server.handlers;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.sentry.Sentry;
import me.kavin.piped.consts.Constants;
import me.kavin.piped.utils.*;
import me.kavin.piped.utils.obj.*;
import me.kavin.piped.utils.obj.db.Video;
import me.kavin.piped.utils.obj.federation.FederatedChannelInfo;
import me.kavin.piped.utils.obj.federation.FederatedVideoInfo;
import me.kavin.piped.utils.resp.InvalidRequestResponse;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.StatelessSession;
import org.schabi.newpipe.extractor.InfoItem;
import org.schabi.newpipe.extractor.ListExtractor;
import org.schabi.newpipe.extractor.Page;
import org.schabi.newpipe.extractor.channel.ChannelInfo;
import org.schabi.newpipe.extractor.channel.tabs.ChannelTabInfo;
import org.schabi.newpipe.extractor.channel.tabs.ChannelTabs;
import org.schabi.newpipe.extractor.exceptions.ExtractionException;
import org.schabi.newpipe.extractor.exceptions.ParsingException;
import org.schabi.newpipe.extractor.linkhandler.ListLinkHandler;
import org.schabi.newpipe.extractor.stream.StreamInfoItem;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static me.kavin.piped.consts.Constants.YOUTUBE_SERVICE;
import static me.kavin.piped.consts.Constants.mapper;
import static me.kavin.piped.utils.CollectionUtils.collectPreloadedTabs;
import static me.kavin.piped.utils.CollectionUtils.collectRelatedItems;
import static me.kavin.piped.utils.URLUtils.getLastThumbnail;

public class ChannelHandlers {
    public static byte[] channelResponse(String channelPath) throws Exception {

        Sentry.setExtra("channelPath", channelPath);

        final ChannelInfo info = ChannelInfo.getInfo("https://youtube.com/" + channelPath);

        final ChannelTabInfo tabInfo = ChannelHelpers.videosTabInfo(info);

        final List<ContentItem> relatedStreams = tabInfo != null ? collectRelatedItems(tabInfo.getRelatedItems()) : List.of();

        if (tabInfo != null)
            Multithreading.runAsync(() -> ChannelHelpers.federateChannelVideos(tabInfo));

        Multithreading.runAsync(() -> ChannelHelpers.federateChannelInfo(info));

        if (tabInfo != null)
            Multithreading.runAsync(() -> ChannelHelpers.updateChannelVideos(info, tabInfo));

        String nextpage = null;
        if (tabInfo != null && tabInfo.hasNextPage()) {
            Page page = tabInfo.getNextPage();
            nextpage = mapper.writeValueAsString(page);
        }

        List<ChannelTab> tabs = info.getTabs()
                .stream()
                .filter(tab -> !tab.getContentFilters().contains(ChannelTabs.VIDEOS))
                .map(tab -> {
                    try {
                        return new ChannelTab(tab.getContentFilters().get(0), mapper.writeValueAsString(tab));
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                }).toList();

        final Channel channel = new Channel(info.getId(), info.getName(), getLastThumbnail(info.getAvatars()),
                getLastThumbnail(info.getBanners()), info.getDescription(), info.getSubscriberCount(), info.isVerified(),
                nextpage, relatedStreams, tabs);

        return mapper.writeValueAsBytes(channel);

    }

    public static byte[] channelPageResponse(String channelId, String prevpageStr)
            throws IOException, ExtractionException {

        if (StringUtils.isEmpty(prevpageStr))
            ExceptionHandler.throwErrorResponse(new InvalidRequestResponse("nextpage is a required parameter"));

        Page prevpage = mapper.readValue(prevpageStr, Page.class);

        if (prevpage == null)
            ExceptionHandler.throwErrorResponse(new InvalidRequestResponse("nextpage is a required parameter"));

        String channelUrl = "https://youtube.com/channel/" + channelId;
        String url = channelUrl + "/videos";

        ListExtractor.InfoItemsPage<InfoItem> info = ChannelTabInfo.getMoreItems(YOUTUBE_SERVICE,
                new ListLinkHandler(url, url, channelId, List.of("videos"), ""), prevpage);

        final List<ContentItem> relatedStreams = collectRelatedItems(info.getItems());

        String nextpage = null;
        if (info.hasNextPage()) {
            Page page = info.getNextPage();
            nextpage = mapper.writeValueAsString(page);
        }

        final StreamsPage streamspage = new StreamsPage(nextpage, relatedStreams);

        return mapper.writeValueAsBytes(streamspage);

    }

    public static byte[] channelTabResponse(String data)
            throws IOException, ExtractionException {

        if (StringUtils.isEmpty(data))
            ExceptionHandler.throwErrorResponse(new InvalidRequestResponse("data is a required parameter"));

        ListLinkHandler tabHandler = mapper.readValue(data, ListLinkHandler.class);

        var info = ChannelTabInfo.getInfo(YOUTUBE_SERVICE, tabHandler);

        List<ContentItem> items = collectRelatedItems(info.getRelatedItems());

        Multithreading.runAsync(() -> {

            var channel = DatabaseHelper.getChannelFromId(info.getId());

            if (channel != null) {
                try (StatelessSession s = DatabaseSessionFactory.createStatelessSession()) {
                    var streamInfoItems = info.getRelatedItems()
                            .stream()
                            .parallel()
                            .filter(StreamInfoItem.class::isInstance)
                            .map(StreamInfoItem.class::cast)
                            .toList();

                    var channelIds = streamInfoItems
                            .stream()
                            .map(item -> {
                                try {
                                    return YOUTUBE_SERVICE.getStreamLHFactory().getId(item.getUrl());
                                } catch (ParsingException e) {
                                    throw new RuntimeException(e);
                                }
                            }).collect(Collectors.toUnmodifiableSet());

                    List<String> videoIdsPresent = DatabaseHelper.getVideosFromIds(s, channelIds)
                            .stream()
                            .map(Video::getId)
                            .toList();

                    // Sequential statt parallel: handleNewVideo fetcht das
                    // Video (= YT-call). Bei einem Channel mit N neuen Videos
                    // war .parallel() bisher ein N-fach-burst — selbe Failure-
                    // shape wie der gefixte PubSub-burst. 250ms sleep zwischen
                    // calls spreizt die Anfragen.
                    for (var item : streamInfoItems) {
                        try {
                            String id = YOUTUBE_SERVICE.getStreamLHFactory().getId(item.getUrl());
                            if (videoIdsPresent.contains(id)) {
                                VideoHelpers.updateVideo(id, item);
                            } else if (item.getUploadDate() != null) {
                                // shorts tab doesn't have upload date
                                // we don't want to fetch each video's upload date
                                long time = item.getUploadDate().offsetDateTime().toInstant().toEpochMilli();
                                if ((System.currentTimeMillis() - time) < TimeUnit.DAYS.toMillis(Constants.FEED_RETENTION)) {
                                    VideoHelpers.handleNewVideo(item.getUrl(), time, channel);
                                    Thread.sleep(250);
                                }
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        });

        String nextpage = null;
        if (info.hasNextPage()) {
            Page page = info.getNextPage();
            nextpage = mapper.writeValueAsString(page);
        }

        return mapper.writeValueAsBytes(new ChannelTabData(nextpage, items));
    }

    public static byte[] channelTabPageResponse(String data, String prevPageStr) throws Exception {

        if (StringUtils.isEmpty(data))
            ExceptionHandler.throwErrorResponse(new InvalidRequestResponse("data is a required parameter"));

        ListLinkHandler tabHandler = mapper.readValue(data, ListLinkHandler.class);

        Page prevPage = mapper.readValue(prevPageStr, Page.class);

        if (prevPage == null)
            ExceptionHandler.throwErrorResponse(new InvalidRequestResponse("nextpage is a required parameter"));

        var info = ChannelTabInfo.getMoreItems(YOUTUBE_SERVICE, tabHandler, prevPage);

        String nextpage = null;
        if (info.hasNextPage()) {
            Page page = info.getNextPage();
            nextpage = mapper.writeValueAsString(page);
        }

        List<ContentItem> items = collectRelatedItems(info.getItems());

        return mapper.writeValueAsBytes(new ChannelTabData(nextpage, items));
    }
}
