package me.kavin.piped.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.sentry.Sentry;
import me.kavin.piped.consts.Constants;
import me.kavin.piped.utils.resp.InvalidRequestResponse;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.schabi.newpipe.extractor.exceptions.ContentNotAvailableException;
import org.schabi.newpipe.extractor.exceptions.ExtractionException;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

public class ExceptionHandler {

    public static Exception handle(Exception e) {
        return handle(e, null);
    }

    public static Exception handle(Exception e, String path) {

        if (e.getCause() != null && (e instanceof ExecutionException || e instanceof CompletionException))
            e = (Exception) e.getCause();

        // Members-only / private / removed / geo / age-gated videos surface as
        // NPE's generic "Could not get any stream" (the anon ANDROID/VR clients
        // return zero streams without a clear reason). Probe the web
        // playabilityStatus and return YouTube's own localized reason as a clean
        // 403 so the app shows a meaningful message instead of httpStatus(500).
        {
            String m = e.getMessage();
            Throwable rc = ExceptionUtils.getRootCause(e);
            String rm = rc != null ? rc.getMessage() : null;
            boolean noStream = (m != null && m.contains("Could not get any stream"))
                    || (rm != null && rm.contains("Could not get any stream"));
            if (noStream && path != null
                    && (path.startsWith("/streams/") || path.startsWith("/synth-hls/"))) {
                String videoId = videoIdFromPath(path);
                if (videoId != null) {
                    String reason = YoutubeUnplayable.probeReason(videoId);
                    if (reason != null) {
                        try {
                            return new ErrorResponse(403,
                                    java.util.Map.of("error", reason, "unplayable", true));
                        } catch (JsonProcessingException ignored) {
                            // fall through to default handling
                        }
                    }
                }
            }
        }

        if (e instanceof ContentNotAvailableException || e instanceof ErrorResponse)
            return e;

        if ((e instanceof ExtractionException extractionException && extractionException.getMessage().contains("No service can handle the url")))
            try {
                return new ErrorResponse(new InvalidRequestResponse("Invalid parameter provided, unknown service"), extractionException);
            } catch (JsonProcessingException jsonProcessingException) {
                throw new RuntimeException(jsonProcessingException);
            }

        Sentry.captureException(e);
        if (Constants.SENTRY_DSN.isEmpty()) {
            if (path != null)
                System.err.println("An error occoured in the path: " + path);
            e.printStackTrace();
        }

        return e;
    }

    private static String videoIdFromPath(String path) {
        String p = path;
        int q = p.indexOf('?');
        if (q >= 0) p = p.substring(0, q);
        String id = null;
        if (p.startsWith("/streams/")) {
            id = p.substring("/streams/".length());
        } else if (p.startsWith("/synth-hls/")) {
            String rest = p.substring("/synth-hls/".length());
            int slash = rest.indexOf('/');
            id = slash >= 0 ? rest.substring(0, slash) : rest;
        }
        return (id != null && id.matches("[A-Za-z0-9_-]{11}")) ? id : null;
    }

    public static void throwErrorResponse(IStatusCode statusObj) {
        try {
            ExceptionUtils.rethrow(new ErrorResponse(statusObj));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static void throwErrorResponse(int code, Object content) {
        try {
            ExceptionUtils.rethrow(new ErrorResponse(code, content));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
