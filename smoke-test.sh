#!/bin/bash
# Smoke test for the Piped-Backend fork (branch ios-streaming-patches).
# Validates the resolve -> master -> variant -> segment chain AND the
# YT_RESOLVE_LIMITER 503 carrier-protection. Run ONCE after `docker compose
# up` to catch fork regressions (semaphore, resolve budget, auto-WebEmbed
# fallback, resolve-reuse, ReentrantLock).
#
# ONE-SHOT ONLY — do NOT loop it. It does ~2-3 YouTube resolves; hammering
# the same resolve re-triggers googlevideo's per-IP throttle (see project
# memory synth_hls_cache_ttl_cpn_throttle). A failure during an active IP
# block is expected (resolve can't complete) and is NOT a fork regression.
set -u
BASE="${PIPED_BASE:-http://localhost:8881}"
VID="${1:-jNQXAC9IVRw}"   # "Me at the zoo" — 2005, 19s, tiny, always available
fail=0
say(){ printf '  %-30s %s\n' "$1" "$2"; }

echo "== Piped-Backend fork smoke ($BASE, video=$VID) =="

# 1. healthcheck — YT-independent, must always be up (proves carriers free)
code=$(curl -s -m 5 -o /dev/null -w '%{http_code}' "$BASE/healthcheck")
[ "$code" = 200 ] && say healthcheck "OK (200)" || { say healthcheck "FAIL ($code)"; fail=1; }

# 2. semaphore FIRST (while VID is still uncached → real contention):
#    3 concurrent /streams → cap is 2, so >=1 must fast-reject 503.
codes=$(for i in 1 2 3; do curl -s -m 20 -o /dev/null -w '%{http_code}\n' "$BASE/streams/$VID" & done; wait)
n503=$(printf '%s\n' "$codes" | grep -c '^503$')
n200=$(printf '%s\n' "$codes" | grep -c '^200$')
if [ "$n503" -ge 1 ] && [ "$n200" -ge 1 ]; then
  say "semaphore (3 concurrent)" "OK (${n200}x200 + ${n503}x503)"
else
  say "semaphore (3 concurrent)" "WARN (codes: $(echo $codes|tr '\n' ' ') — cap may have been free)"
fi

# 3. synth-hls master playlist (reuses the now-cached resolve)
master=$(curl -s -m 15 "$BASE/synth-hls/$VID/master.m3u8")
if printf '%s' "$master" | grep -q '#EXTM3U' && printf '%s' "$master" | grep -qE '(video[0-9]+|audio)\.m3u8'; then
  say "synth-hls master.m3u8" "OK (valid HLS + variants)"
else
  say "synth-hls master.m3u8" "FAIL"; printf '%s\n' "$master" | head -3 | sed 's/^/      /'; fail=1
fi

# 4. variant playlist -> must carry EXT-X-MAP + a segment URL
variant=$(printf '%s' "$master" | grep -oE '(video[0-9]+|audio)\.m3u8' | head -1)
vpl=$(curl -s -m 15 "$BASE/synth-hls/$VID/$variant")
# Segmente laufen regulaer als RELATIVE /yt-proxy/-URIs (rewriteToYtProxy,
# cachender Proxy); absolute piped-proxy-URLs erscheinen nur im Fallback.
# Beide Formen akzeptieren; relative fuer den Range-Check auf BASE aufloesen.
segurl=$(printf '%s' "$vpl" | grep -oE '(https?://|/yt-proxy/)[^"]+' | head -1)
case "$segurl" in /*) segurl="$BASE$segurl";; esac
if printf '%s' "$vpl" | grep -q 'EXT-X-MAP' && [ -n "$segurl" ]; then
  say "synth-hls $variant" "OK (EXT-X-MAP + segment URL)"
else
  say "synth-hls variant" "FAIL"; fail=1
fi

# 5. segment fetch (Range) -> 206 proves the signed media URL actually works
if [ -n "${segurl:-}" ]; then
  scode=$(curl -s -m 10 -o /dev/null -w '%{http_code}' -r 0-1023 "$segurl")
  case "$scode" in
    206) say "segment (Range 0-1023)" "OK (206)";;
    200) say "segment (Range 0-1023)" "OK (200, range ignored)";;
    *)   say "segment (Range 0-1023)" "FAIL ($scode)"; fail=1;;
  esac
fi

echo
[ "$fail" = 0 ] && { echo "SMOKE OK"; exit 0; } || { echo "SMOKE FAILED"; exit 1; }
