package me.kavin.piped.server.handlers;

import me.kavin.piped.utils.CollectionUtils;
import me.kavin.piped.utils.obj.PipedStream;
import me.kavin.piped.utils.obj.Streams;
import me.kavin.piped.utils.Multithreading;
import org.schabi.newpipe.extractor.stream.StreamInfo;
import org.schabi.newpipe.extractor.services.youtube.extractors.YoutubeStreamExtractor;
import java.net.HttpURLConnection;
import java.net.URL;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.List;

/// Synthesizes an HLS manifest from Piped's DASH streams.
/// AVPlayer receives an external http:// URL pointing at Pi5; native HLS engine
/// handles it (aggressive prefetching). Segments are routed through /yt-proxy to
/// work around piped-proxy's HEAD-request and large-range limitations.
public class SynthHlsHandlers {

    public static byte[] masterPlaylist(String videoId) throws Exception {
        // requireVerified=false: the master only lists variant playlist names,
        // never segment URLs, so it doesn't need the throttle HEAD-probe /
        // WebEmbed re-resolve. Skipping that keeps this (the dominant
        // FILE_LOADED cost) fast; the variant/audio fetches verify before any
        // segment URL is served.
        Streams streams = fetchStreams(videoId, false);
        List<PipedStream> videos = pickedVideoStreams(streams);
        PipedStream audio = pickedAudioStream(streams);
        if (videos.isEmpty() || audio == null) {
            return "#ERROR: no playable streams".getBytes(StandardCharsets.UTF_8);
        }
        StringBuilder sb = new StringBuilder();
        sb.append("#EXTM3U\n");
        sb.append("#EXT-X-VERSION:7\n");
        sb.append("#EXT-X-INDEPENDENT-SEGMENTS\n");
        String audioCodec = audio.codec != null ? audio.codec : "mp4a.40.2";
        sb.append("#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio\",NAME=\"Audio\",DEFAULT=YES,AUTOSELECT=YES,URI=\"audio.m3u8\"\n");
        for (int i = 0; i < videos.size(); i++) {
            PipedStream v = videos.get(i);
            int bw = (v.bitrate > 0 ? v.bitrate : 5_000_000) + (audio.bitrate > 0 ? audio.bitrate : 128_000);
            int w = v.width > 0 ? v.width : 1920;
            int h = v.height > 0 ? v.height : 1080;
            int fps = v.fps > 0 ? v.fps : 30;
            String vCodec = v.codec != null ? v.codec : "avc1.64002a";
            sb.append(String.format("#EXT-X-STREAM-INF:BANDWIDTH=%d,AVERAGE-BANDWIDTH=%d,RESOLUTION=%dx%d,FRAME-RATE=%d,CODECS=\"%s,%s\",AUDIO=\"audio\"\n",
                    bw, bw, w, h, fps, vCodec, audioCodec));
            sb.append("video").append(i).append(".m3u8\n");
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] audioPlaylist(String videoId) throws Exception {
        Streams streams = fetchStreams(videoId);
        PipedStream audio = pickedAudioStream(streams);
        if (audio == null) return "#ERROR".getBytes(StandardCharsets.UTF_8);
        return streamPlaylist(audio, streams.duration);
    }

    public static byte[] videoPlaylist(String videoId, int idx) throws Exception {
        Streams streams = fetchStreams(videoId);
        List<PipedStream> videos = pickedVideoStreams(streams);
        if (idx < 0 || idx >= videos.size()) return "#ERROR".getBytes(StandardCharsets.UTF_8);
        return streamPlaylist(videos.get(idx), streams.duration);
    }

    /// Builds an HLS playlist for one DASH stream. Uses sidx fragment boundaries
    /// for multi-segment ranges (~1-2MB each). Falls back to single-segment if
    /// sidx fetch fails — that path may 403 on YouTube's CDN for large files.
    private static byte[] streamPlaylist(PipedStream stream, long durationSeconds) {
        int initLen = stream.initEnd - stream.initStart + 1;
        int mediaStart = stream.indexEnd + 1;
        long mediaLen = stream.contentLength - mediaStart;
        double dur = (double) durationSeconds;
        String segUrl = rewriteToYtProxy(stream.url);

        SidxParserJava.Data sidx = null;
        if (stream.indexStart > 0 && stream.indexEnd > stream.indexStart) {
            sidx = SidxParserJava.fetch(stream.url, stream.indexStart, stream.indexEnd, YOUTUBE_COOKIES);
        }

        StringBuilder sb = new StringBuilder();
        sb.append("#EXTM3U\n");
        sb.append("#EXT-X-VERSION:7\n");

        if (sidx != null && !sidx.entries.isEmpty()) {
            int maxSegDur = 0;
            for (SidxParserJava.Entry e : sidx.entries) {
                int d = (int) Math.ceil(e.duration);
                if (d > maxSegDur) maxSegDur = d;
            }
            sb.append("#EXT-X-TARGETDURATION:").append(Math.max(maxSegDur, 1)).append('\n');
            sb.append("#EXT-X-MEDIA-SEQUENCE:0\n");
            // PLAYLIST-TYPE:VOD removed — triggers Apple HLS conservative ~60s buffer cap
            sb.append("#EXT-X-INDEPENDENT-SEGMENTS\n");
            sb.append(String.format("#EXT-X-MAP:URI=\"%s\",BYTERANGE=\"%d@%d\"\n",
                    segUrl, initLen, stream.initStart));
            long cursor = stream.indexEnd + 1L + sidx.firstOffset;
            for (SidxParserJava.Entry e : sidx.entries) {
                sb.append(String.format("#EXTINF:%.3f,\n", e.duration));
                sb.append(String.format("#EXT-X-BYTERANGE:%d@%d\n", e.byteSize, cursor));
                sb.append(segUrl).append('\n');
                cursor += e.byteSize;
            }
        } else {
            int durInt = (int) Math.ceil(dur);
            sb.append("#EXT-X-TARGETDURATION:").append(durInt).append('\n');
            sb.append("#EXT-X-MEDIA-SEQUENCE:0\n");
            // PLAYLIST-TYPE:VOD removed — triggers Apple HLS conservative ~60s buffer cap
            sb.append("#EXT-X-INDEPENDENT-SEGMENTS\n");
            sb.append(String.format("#EXT-X-MAP:URI=\"%s\",BYTERANGE=\"%d@%d\"\n",
                    segUrl, initLen, stream.initStart));
            sb.append(String.format("#EXTINF:%.3f,\n", dur));
            sb.append(String.format("#EXT-X-BYTERANGE:%d@%d\n", mediaLen, mediaStart));
            sb.append(segUrl).append('\n');
        }
        sb.append("#EXT-X-ENDLIST");
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static final String YOUTUBE_COOKIES = loadCookies();

    private static String loadCookies() {
        String p = System.getenv("YOUTUBE_COOKIES_FILE");
        if (p == null || p.isEmpty()) p = "/app/youtube-cookies.txt";
        java.io.File f = new java.io.File(p);
        if (!f.exists()) return null;
        StringBuilder sb = new StringBuilder();
        try (java.io.BufferedReader r = new java.io.BufferedReader(new java.io.FileReader(f))) {
            String line;
            while ((line = r.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;
                String[] parts = line.split("\t");
                if (parts.length < 7) continue;
                if (sb.length() > 0) sb.append("; ");
                sb.append(parts[5]).append("=").append(parts[6]);
            }
        } catch (Exception e) { return null; }
        return sb.length() == 0 ? null : sb.toString();
    }

    private static final ConcurrentMap<String, CacheEntry> streamsCache = new ConcurrentHashMap<>();
    // 10s — DELIBERATELY short: a longer TTL reuses the same cpn (client
    // playback nonce) across separate plays, which googlevideo per-IP throttles
    // (single-use-cpn). A 5min experiment (2026-05-29) correlated with a
    // server-side resolve-throttle and was reverted. A safe pre-warm needs a
    // SPLIT — cache the stable master METADATA long, but resolve FRESH cpn
    // segment URLs per play — not a blanket TTL bump.
    private static final long CACHE_TTL_MS = 10_000L;

    private static class CacheEntry {
        final Streams streams;
        final long createdAt;
        final boolean urlsVerified;   // throttle-checked (+ WebEmbed-upgraded if needed)
        CacheEntry(Streams s, boolean verified) {
            this.streams = s; this.createdAt = System.currentTimeMillis(); this.urlsVerified = verified;
        }
        boolean fresh() { return System.currentTimeMillis() - createdAt < CACHE_TTL_MS; }
    }

    /// Default to verified URLs — used by the segment-serving paths.
    private static Streams fetchStreams(String videoId) throws Exception {
        return fetchStreams(videoId, true);
    }

    /// requireVerified=false (master playlist) skips the throttle HEAD-probe +
    /// WebEmbed re-resolve. The master lists only variant names, so no segment
    /// URL is ever served from an unverified entry. requireVerified=true
    /// (variant/audio) guarantees the throttle check ran before segment URLs go
    /// out. A master fetch that just resolved+cached the streams lets the
    /// follow-up variant fetch verify from cache WITHOUT a second
    /// StreamInfo.getInfo (only the cheap HEAD; WebEmbed only if actually 403'd).
    private static Streams fetchStreams(String videoId, boolean requireVerified) throws Exception {
        CacheEntry e = streamsCache.get(videoId);
        if (e != null && e.fresh() && (!requireVerified || e.urlsVerified)) return e.streams;
        synchronized (streamsCache) {
            e = streamsCache.get(videoId);
            if (e != null && e.fresh() && (!requireVerified || e.urlsVerified)) return e.streams;

            // Reuse fresh-but-unverified streams (just cached by a master fetch)
            // so we don't pay a second StreamInfo.getInfo just to verify.
            Streams s = (e != null && e.fresh()) ? e.streams : resolveStreams(videoId);

            boolean verified = false;
            if (requireVerified) {
                // Throttle-Check: ANDROID-URLs werden von googlevideo per-IP-Pattern
                // fuer deep byte-range-fetches 403'd. HEAD-Test gegen clen/2 deckt das
                // auf -- bei 403 retry mit force-WebEmbed (= WebEmbed-modern-URLs werden
                // nicht so throttled). Siehe StreamHandlers fuer Detail-Notes.
                if (isStreamsThrottled(s)) {
                    System.out.println("[SynthHls] " + videoId + " URLs throttled (HEAD=403 auf clen/2), retry mit force-WebEmbed");
                    Streams retryS = resolveStreamsWebEmbed(videoId);
                    if (retryS != null && !retryS.audioStreams.isEmpty() && !retryS.videoStreams.isEmpty()) {
                        s = retryS;
                        System.out.println("[SynthHls] " + videoId + " WebEmbed-retry success");
                    } else {
                        System.out.println("[SynthHls] " + videoId + " WebEmbed-retry non-healthy, behalte original");
                    }
                }
                verified = true;
            }
            streamsCache.put(videoId, new CacheEntry(s, verified));
            return s;
        }
    }

    /// Resolve StreamInfo via NewPipe, retrying up to 3x on a degraded result
    /// (0 video or 0 audio — happens non-deterministically). Wrapped in
    /// Multithreading.supplyAsync to match StreamHandlers' threading context
    /// (a direct getInfo() can return degraded streams).
    private static Streams resolveStreams(String videoId) throws Exception {
        Streams s = null;
        for (int attempt = 0; attempt < 3; attempt++) {
            StreamInfo info = Multithreading.supplyAsync(() -> {
                try { return StreamInfo.getInfo("https://www.youtube.com/watch?v=" + videoId); }
                catch (Exception ex) { throw new RuntimeException(ex); }
            }).get();
            s = CollectionUtils.collectStreamInfo(info);
            if (!s.audioStreams.isEmpty() && !s.videoStreams.isEmpty()) break;
            System.out.println("[SynthHls] " + videoId + " attempt " + (attempt + 1) + " degraded (v=" + s.videoStreams.size() + " a=" + s.audioStreams.size() + "), retrying");
        }
        return s;
    }

    /// Re-resolve with force-WebEmbed (modern URLs that googlevideo throttles
    /// less). Returns null on failure — caller keeps the original streams.
    private static Streams resolveStreamsWebEmbed(String videoId) {
        try {
            StreamInfo retryInfo = Multithreading.supplyAsync(() -> {
                // ThreadLocal muss INSIDE des Lambdas gesetzt werden -- supplyAsync
                // laeuft auf Worker-Thread, ThreadLocals propagieren nicht ueber den
                // Thread-Pool-Hop hinweg.
                YoutubeStreamExtractor.FORCE_WEB_EMBED_FOR_THREAD.set(Boolean.TRUE);
                try {
                    return StreamInfo.getInfo("https://www.youtube.com/watch?v=" + videoId);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                } finally {
                    YoutubeStreamExtractor.FORCE_WEB_EMBED_FOR_THREAD.remove();
                }
            }).get();
            return CollectionUtils.collectStreamInfo(retryInfo);
        } catch (Exception ex) {
            System.out.println("[SynthHls] " + videoId + " WebEmbed-retry failed: " + ex.getMessage());
            return null;
        }
    }

    private static List<PipedStream> pickedVideoStreams(Streams streams) {
        // Single H.264 1080p-or-less variant — avoids ABR-switching confusion
        // in Apple HLS engine which made buffer-ahead shrink under high variant count.
        PipedStream best = null;
        for (PipedStream s : streams.videoStreams) {
            if (s.codec == null || !s.codec.startsWith("avc")) continue;
            int h = s.height;
            if (h <= 0 || h > 1080) continue;
            if (best == null || h > best.height || (h == best.height && s.bitrate > best.bitrate)) best = s;
        }
        List<PipedStream> out = new ArrayList<>();
        if (best != null) out.add(best);
        return out;
    }

    private static PipedStream pickedAudioStream(Streams streams) {
        PipedStream best = null;
        for (PipedStream s : streams.audioStreams) {
            if (s.codec == null || !s.codec.startsWith("mp4a")) continue;
            if (s.audioTrackType != null && !s.audioTrackType.equals("ORIGINAL")) continue;
            if (best == null || s.bitrate > best.bitrate) best = s;
        }
        return best;
    }

    private static String rewriteToYtProxy(String pipedProxyUrl) {
        int q = pipedProxyUrl.indexOf('?');
        if (q < 0) return pipedProxyUrl;
        String query = pipedProxyUrl.substring(q + 1);
        String host = null;
        for (String pair : query.split("&")) {
            if (pair.startsWith("host=")) { host = pair.substring(5); break; }
        }
        if (host == null) return pipedProxyUrl;
        int pathStart = pipedProxyUrl.indexOf('/', 8);
        String path = (pathStart >= 0 && pathStart < q) ? pipedProxyUrl.substring(pathStart, q) : "/";
        return pipedProxyUrl; // bypass yt-proxy — googlevideo per-video throttle currently blocks yt-proxy upstream
    }

    /**
     * HEAD-byte-range-Check fuer Throttle-Detection — auf dem tatsaechlich
     * servierten Video-Stream (pickedVideoStreams). Vorher StreamInfo-basiert;
     * jetzt auf den collected Streams, damit der Check aus dem Cache heraus
     * laufen kann, ohne StreamInfo erneut aufzuloesen.
     */
    private static boolean isStreamsThrottled(Streams s) {
        if (s == null) return false;
        List<PipedStream> vids = pickedVideoStreams(s);
        if (vids.isEmpty()) return false;
        PipedStream v = vids.get(0);
        String url = v.url;
        if (url == null || url.isEmpty() || !url.startsWith("http")) return false;
        long clen = v.contentLength;
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
