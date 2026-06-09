package me.kavin.piped.utils.resp;

import me.kavin.piped.utils.IStatusCode;

/// Returned by /streams when a video resolves but its segment URLs are still
/// 403 after the WebEmbed upgrade — i.e. googlevideo is rate-limiting the
/// playback path (a transient throttle storm), not a permanent failure.
///
/// Distinct from the generic 500/timeout so the app can tell the user
/// "YouTube is throttling, try again shortly" instead of "server not
/// responding". `throttled:true` is the stable flag to branch on; `error` is a
/// human-readable message. HTTP 503 = temporary, retryable.
public class ThrottledResponse implements IStatusCode {

    public String error;
    public final boolean throttled = true;

    public ThrottledResponse(String error) {
        this.error = error;
    }

    public ThrottledResponse() {
        this.error = "YouTube is rate-limiting playback for this video. Try again shortly.";
    }

    @Override
    public int getStatusCode() {
        return 503;
    }
}
