# CLAUDE.md — piped-backend (YouTube stack for the Kuckuck app)

Fork of Piped-Backend that resolves + serves YouTube for the family's Kuckuck iOS app
(synth-HLS over a Pi5). Heavily patched around YouTube's anti-bot/streaming changes.

- **This repo**: `simonchrz/Piped-Backend`, branch `ios-streaming-patches`. Source lives
  ONLY on the Pi (`~/piped-backend-src`); there is no GitHub checkout on the Mac.
- **NewPipeExtractor fork**: `simonchrz/NewPipeExtractor`, branch `ios-innertube-fallback`,
  source at `~/NewPipeExtractor`. Built into `libs/NewPipeExtractor-patched.jar`. It's an
  "eigenes getestetes Projekt" — do NOT rebase as a hotfix (see memory `newpipe_fork_rebase_deferred`).

## Build & deploy (no JDK on the Pi — everything via Docker)
- **NPE jar**: `bash ~/build-npe.sh` (docker `eclipse-temurin:21-jdk`; installs git + sets
  `safe.directory`; `./gradlew :extractor:jar`) → copies to `libs/NewPipeExtractor-patched.jar`.
  Run this whenever you touch `~/NewPipeExtractor`.
- **Backend**: `cd ~/piped-backend-src && make backend restart` (`docker build` compiles the
  piped source incl. the jar, then `docker compose up -d piped-backend`). `make deploy` = jar +
  backend + restart. Editing piped source alone → just `make backend restart` (no NPE rebuild).
- **Ports**: container binds `Constants.PORT` = **8080**; compose maps `8881:8080`, so the
  external/app URL is `:8881` but a self-call from inside the backend must use `localhost:8080`
  (`Constants.PORT`). `Constants.PUBLIC_URL` = the external API_URL.
- **NEVER restart `tv-receiver`** during recordings — different service, but be aware. piped-backend
  restart is fine (brief YT playback blip).
- Commit/push ONLY when asked. Build cycle is ~2-3 min — prototype protocol work in Python first
  (see SABR below), then port to Java.

## YouTube resolve + playback chain (StreamHandlers + SynthHlsHandlers)
ANDROID_VR (primary, pre-signed, nsig/poToken-free) → on audio=0 (ÖR/Nick) WebEmbed →
if WebEmbed segments still 403 (googlevideo throttle storm) **TVHTML5** (authenticated TV client)
→ else **503 `ThrottledResponse`** (`{"throttled":true}`, so the app shows "rate-limited" not a
timeout) → if a resolve yields video formats with NO url (SABR-only) **SABR auto-fallback**.
Each resolve logs `[ResolvePath] <id> -> CLIENT` (or `SABR-ONLY`). The 403s are usually a transient
storm, not a dead client (memory `youtube_audio0_403_is_transient_throttle`).

## TVHTML5 (authenticated TV client, storm fallback)
Plain TVHTML5 is bot-walled; the crack is a full signed-in TV session:
- scrape `youtube.com/tv` ytcfg (visitorData, clientVersion, appInstallData, rolloutToken,
  userSessionId) via **HttpURLConnection** (reqwest4j mishandles `/tv`'s brotli) + gzip-decode +
  **youtube-domain cookies only** (the file's `.google.com` SID/SAPISID dupes trigger the login page).
- DownloaderImpl: **preserve the Cobalt UA** for TV requests (forcing Chrome = clientName/UA mismatch
  = bot-wall) + add combined **SAPISIDHASH/SAPISID1PHASH/SAPISID3PHASH** auth with the user_session_id
  as the `_u` part (passed via the private `X-Yt-Auth-Session` header, stripped there).

## SABR engine (`me.kavin.piped.utils.sabr`) — insurance for SABR-only enforcement
Full YouTube Server-ABR/UMP streaming from scratch (no protobuf-java). `/sabr/<videoId>/<itag>`
download-once + file-cache + HTTP range. synth-hls auto-uses it when a client returns url-less
formats. Spec + Python prototype: `~/sabr-ref/SABR-SPEC.md`. Reference: LuanRT/googlevideo.
Hard-won SABR lessons:
- **No poToken needed** for ANDROID (STREAM_PROTECTION_STATUS=1=OK). Inputs from the ANDROID player
  response (`serverAbrStreamingUrl` + `videoPlaybackUstreamerConfig` + formatIds), no auth.
- **Format control**: `preferred_audio/video_format_ids` (fields 16/17) + send `selected_format_ids`
  ONLY after FORMAT_INIT arrives (else the server skips the init segment → unplayable). Codec via
  MediaCapabilities: **video_codec=2=avc, audio_codec=1=aac**.
- Drive `player_time` from FORMAT_INIT duration/totalSegments (MEDIA_HEADER has no time fields).
- **UMP varint ≠ protobuf varint** (first byte selects length). `pos += (int) readVarint()` is BUGGY
  (Java evals left `pos` before the side effect) — read len into a var first.
- SABR fmp4 is standard (ftyp/moov/sidx/moof/mdat), so the existing sidx→HLS works; synth-hls scans
  the sidx box from the `/sabr` file (SABR init ≠ DASH indexRange). Force-test: `YT_FORCE_SABR` env.
- yt-dlp still does NOT implement SABR (only detects it) — we are ahead; don't expect a yt-dlp reference.

## Gotchas
- **No em-dashes** in Pi Java/Python file writes (iso8859_15 locale → UnicodeEncodeError); use ASCII.
- yt-proxy (`YtProxyHandlers`) uses **sequential bounded Range chunks**, NOT a No-Range full GET
  (the latter is throttled to ~31KB/s). The stale class doc once said the opposite.
- Persistent project knowledge is the Claude **auto-memory** (`MEMORY.md` + topic files), kept current
  per session — read it before YouTube-stack work.
