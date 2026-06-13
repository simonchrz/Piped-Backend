# reqwest4j fork — per-request egress proxy (`fetchWithProxy`)

Why: YouTube bot-flags our egress IP, and the flagged family alternates between
the public IPv4 (direct) and the IPv6 /64 (via `yt-v6-proxy`). Upstream reqwest4j
1.0.14 stores its HTTP client in a Rust `OnceLock` (set once, re-init panics) and
`fetch()` has no per-request proxy — so a single process can't switch egress family
at runtime. This fork adds `fetchWithProxy(url, method, body, headers, proxy)` with
a per-proxy client cache, so `EgressManager` can route each request through the
currently-clean family and auto-flip on a bot-flag (see
`me.kavin.piped.utils.EgressManager`).

## Changes vs upstream tag `1.0.14`
- `reqwest-jni/src/lib.rs` — added `CLIENTS: OnceLock<Mutex<HashMap<String,Client>>>`
  + `get_or_build_client(proxy)` + JNI `Java_..._fetchWithProxy`; factored the shared
  request path into `run_fetch`. `init`/`fetch` unchanged (backwards-compatible).
  (`""` proxy = direct egress.)
- `src/main/java/rocks/kavin/reqwest4j/ReqwestUtils.java` — declared the new
  `native CompletableFuture<Response> fetchWithProxy(...)`.

The two modified files are vendored next to this README (`lib.rs`, `ReqwestUtils.java`).

## Rebuild → `../libs/reqwest4j-egress.jar`
No `cross`/gradle-rust-plugin needed: the Pi is aarch64, so build the native lib
natively, then jar-surgery the official 1.0.14 jar (swap the aarch64 `.so` + the
recompiled `ReqwestUtils.class`).

```sh
# 0. clone upstream at the tag and drop the two vendored files in
git clone --branch 1.0.14 --depth 1 https://github.com/TeamPiped/reqwest4j.git r4j
cp lib.rs          r4j/reqwest-jni/src/lib.rs
cp ReqwestUtils.java r4j/src/main/java/rocks/kavin/reqwest4j/ReqwestUtils.java

# 1. native aarch64 build of libreqwest_jni.so (~90s)
cd r4j/reqwest-jni
docker run --rm -v "$PWD":/app -w /app -v ~/.cargo-fork-cache:/usr/local/cargo/registry \
  rust:1-bookworm cargo build --release
#   -> target/release/libreqwest_jni.so   (exports Java_..._fetchWithProxy)

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
must list `fetchWithProxy`.

## TODO
- Bump Rust `reqwest` 0.12.2 → 0.13.4 (not a drop-in: `rustls-tls`→`rustls`,
  aws-lc-rs TLS provider, `http2` now a separate feature; validate YouTube runtime
  behaviour). Tracked separately.
