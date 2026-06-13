# reqwest4j fork — per-request family egress + HTTP/3

Why: YouTube bot-flags our egress IP, and the flagged family alternates between
the public IPv4 and the IPv6 /64 — the clean family changes over time. Upstream
reqwest4j 1.0.14 stores its HTTP client in a Rust `OnceLock` (set once, re-init
panics) and `fetch()` has no per-request control, so a single process can't switch
egress family at runtime. This fork adds
`fetchWithProxy(url, method, body, headers, family)` where `family` is "v4"/"v6":
each family has its own client whose source socket is pinned to that IP family via
`local_address`, so `EgressManager` routes each request over the currently-clean
family and auto-flips on a bot-flag (see `me.kavin.piped.utils.EgressManager`). No
CONNECT proxy — the old `yt-v6-proxy` was a pre-fork workaround for reqwest4j not
honouring v6; native `local_address` replaces it and also lets **HTTP/3 (QUIC)** run
on BOTH families (a CONNECT proxy can't tunnel UDP).

## Changes vs upstream tag `1.0.14`
- `reqwest-jni/src/lib.rs`:
  - `FAMILY_CLIENTS: OnceLock<Mutex<HashMap<String,Client>>>` keyed "v4"/"v6"(+"-h3");
    `get_family_client(family, h3)` builds a client with `.local_address(0.0.0.0)`
    or `(::)` to pin egress family. New JNI `Java_..._fetchWithProxy` (5th arg =
    family selector). Shared request path factored into `run_fetch`, which takes an
    ordered list of `(client, version)` attempts and completes the future on the
    first success — single attempt = upstream behaviour, multiple = h3→h2 fallback.
  - **HTTP/3** on both families: `fetchWithProxy(fam)` tries `[(fam-h3, HTTP_3),
    (fam-h2, none)]`. Two gotchas baked in: (1) clients are built inside
    `RUNTIME.enter()` — quinn needs a live tokio runtime at build time or `.build()`
    aborts "no async runtime found"; (2) `local_address` pins the QUIC socket family
    (else quinn picks Google's AAAA regardless of the intended family). Kill switch:
    env `R4J_HTTP3=0` → h2 only.
  - `init`/`fetch` keep using the single `CLIENT` (for non-YouTube callers); the old
    CONNECT-proxy per-URL cache is gone.
- `reqwest-jni/Cargo.toml` — `reqwest` **0.13.4**, features
  `rustls, http3, http2, stream, brotli, gzip, socks` (`default-features=false`);
  added `http = "1"` (for `http::Version`). 0.13 renamed `rustls-tls`→`rustls`, split
  out `http2`, uses aws-lc-rs; `http3` pulls h3/h3-quinn/quinn.
- `src/main/java/rocks/kavin/reqwest4j/ReqwestUtils.java` — declared the new
  `native CompletableFuture<Response> fetchWithProxy(...)`.

Container note: piped-backend needs IPv6 egress for the v6-pinned client — keep the
`enable_ipv6`/NAT66 network in docker-compose.yml (only the yt-v6-proxy *container*
was removed).

Vendored next to this README: `lib.rs`, `Cargo.toml`, `ReqwestUtils.java`.

## Rebuild → `../libs/reqwest4j-egress.jar`
No `cross`/gradle-rust-plugin needed: the Pi is aarch64, build the native lib
natively, then jar-surgery the official 1.0.14 jar.

```sh
# 0. clone upstream at the tag, drop the three vendored files in
git clone --branch 1.0.14 --depth 1 https://github.com/TeamPiped/reqwest4j.git r4j
cp lib.rs           r4j/reqwest-jni/src/lib.rs
cp Cargo.toml       r4j/reqwest-jni/Cargo.toml
cp ReqwestUtils.java r4j/src/main/java/rocks/kavin/reqwest4j/ReqwestUtils.java

# 1. native aarch64 build. HTTP/3 needs RUSTFLAGS=--cfg reqwest_unstable, and the
#    inherited Cargo.lock must be refreshed (http3 pulls futures-channel 0.3.32).
#    cmake+clang build the aws-lc-rs crypto provider. ~3min cold.
cd r4j/reqwest-jni
docker run --rm -e RUSTFLAGS='--cfg reqwest_unstable' -v "$PWD":/app -w /app \
  -v ~/.cargo-fork-cache:/usr/local/cargo/registry rust:1-bookworm bash -c \
  'apt-get update -qq && apt-get install -y -qq cmake clang && cargo update && cargo build --release'
#   -> target/release/libreqwest_jni.so   (exports Java_..._fetchWithProxy; ~6.9MB)

# 2. jar-surgery: official jar + new .so (renamed) + recompiled ReqwestUtils.class
cd ../.. && mkdir -p bf/nat/META-INF/natives/linux/aarch64 bf/out bf/src/rocks/kavin/reqwest4j
curl -sL -o bf/reqwest4j-egress.jar \
  https://repo1.maven.org/maven2/rocks/kavin/reqwest4j/1.0.14/reqwest4j-1.0.14.jar
cp r4j/reqwest-jni/target/release/libreqwest_jni.so bf/nat/META-INF/natives/linux/aarch64/libreqwest.so
cp r4j/src/main/java/rocks/kavin/reqwest4j/ReqwestUtils.java bf/src/rocks/kavin/reqwest4j/
docker run --rm -v "$PWD/bf":/w -w /w eclipse-temurin:21-jdk bash -c '
  javac -cp reqwest4j-egress.jar -d out src/rocks/kavin/reqwest4j/ReqwestUtils.java
  jar uf reqwest4j-egress.jar -C out rocks/kavin/reqwest4j/ReqwestUtils.class
  jar uf reqwest4j-egress.jar -C nat META-INF/natives/linux/aarch64/libreqwest.so'
cp bf/reqwest4j-egress.jar ../libs/reqwest4j-egress.jar   # consumed via build.gradle files()
```

Verify: `javap -cp ../libs/reqwest4j-egress.jar rocks.kavin.reqwest4j.ReqwestUtils`
lists `fetchWithProxy`. Runtime: `tcpdump -nn 'udp port 443'` during a resolve shows
QUIC to Google over the active family's IP (v4 by default; v6 when flipped).
