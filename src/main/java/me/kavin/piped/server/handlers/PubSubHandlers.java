package me.kavin.piped.server.handlers;

import com.rometools.rome.feed.synd.SyndFeed;
import com.rometools.rome.io.SyndFeedInput;
import io.sentry.Sentry;
import me.kavin.piped.consts.Constants;
import me.kavin.piped.utils.*;
import me.kavin.piped.utils.obj.MatrixHelper;
import me.kavin.piped.utils.obj.federation.FederatedVideoInfo;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.StatelessSession;
import org.schabi.newpipe.extractor.exceptions.ParsingException;
import org.schabi.newpipe.extractor.localization.DateWrapper;
import org.xml.sax.InputSource;

import java.io.ByteArrayInputStream;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static me.kavin.piped.consts.Constants.YOUTUBE_SERVICE;

public class PubSubHandlers {

    private static final LinkedBlockingQueue<String> pubSubQueue = new LinkedBlockingQueue<>();

    public static void handlePubSub(byte[] body) throws Exception {
        SyndFeed feed = new SyndFeedInput().build(new InputSource(new ByteArrayInputStream(body)));


        for (var entry : feed.getEntries()) {
            String url = entry.getLinks().get(0).getHref();
            String videoId = StringUtils.substring(url, -11);

            long publishedDate = entry.getPublishedDate().getTime();

            String str = videoId + ":" + publishedDate;

            if (pubSubQueue.contains(str))
                continue;

            pubSubQueue.put(str);
        }
    }

    static {
        // Single worker + 250ms sleep between fetches. Original code spawned
        // availableProcessors() (= 4 on Pi5) parallel workers, each calling
        // extractor.fetchPage() = full /streams-equivalent InnerTube call.
        // When 50+ subbed channels publish around the same time (morning
        // upload-burst), 4 simultaneous /streams hits triggered YouTube's
        // anti-bot SignInConfirm IP-flag — same failure mode that the
        // PubSub-renewal patch in Main.java fixed. Cap to 1 worker so a
        // burst of N videos serialises into ~250ms*N spread instead of
        // sub-second parallel firing.
        for (int i = 0; i < 1; i++) {
            new Thread(() -> {
                try {
                    while (true) {
                        String str = pubSubQueue.take();

                        String videoId = StringUtils.substringBefore(str, ":");
                        long publishedDate = Long.parseLong(StringUtils.substringAfter(str, ":"));

                        try (StatelessSession s = DatabaseSessionFactory.createStatelessSession()) {
                            if (DatabaseHelper.doesVideoExist(s, videoId))
                                continue;
                        }

                        try {
                            Sentry.setExtra("videoId", videoId);
                            var extractor = YOUTUBE_SERVICE.getStreamExtractor("https://youtube.com/watch?v=" + videoId);
                            extractor.fetchPage();
                            // Spread bursts so YouTube doesn't flag us.
                            Thread.sleep(250);

                            Multithreading.runAsync(() -> {

                                DateWrapper uploadDate;

                                try {
                                    uploadDate = extractor.getUploadDate();
                                } catch (ParsingException e) {
                                    throw new RuntimeException(e);
                                }

                                if (uploadDate != null && System.currentTimeMillis() - uploadDate.offsetDateTime().toInstant().toEpochMilli() < TimeUnit.DAYS.toMillis(Constants.FEED_RETENTION)) {
                                    try {
                                        MatrixHelper.sendEvent("video.piped.stream.info", new FederatedVideoInfo(
                                                StringUtils.substring(extractor.getUrl(), -11), StringUtils.substring(extractor.getUploaderUrl(), -24),
                                                extractor.getName(),
                                                extractor.getLength(), extractor.getViewCount())
                                        );
                                    } catch (Exception e) {
                                        ExceptionHandler.handle(e);
                                    }
                                }
                            });

                            VideoHelpers.handleNewVideo(extractor, publishedDate, null);
                        } catch (Exception e) {
                            ExceptionHandler.handle(e);
                        }
                    }
                } catch (Exception e) {
                    ExceptionHandler.handle(e);
                }
            }, "PubSub-Worker-" + i).start();
        }
    }

}
