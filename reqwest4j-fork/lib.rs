use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use http::Version;
use jni::objects::{JByteArray, JClass, JMap, JObject, JString};
use jni::sys::jobject;
use jni::JNIEnv;
use reqwest::{Client, Method, Url};
use tokio::runtime::Runtime;

const UA: &str = "Mozilla/5.0 (Windows NT 10.0; rv:102.0) Gecko/20100101 Firefox/102.0";

static RUNTIME: OnceLock<Runtime> = OnceLock::new();
static CLIENT: OnceLock<Client> = OnceLock::new();
// Family-pinned client cache for fetchWithProxy. Key = "v4"/"v6" (+ "-h3"). Each
// client binds its source socket to that IP family via local_address, so a single
// process can route individual requests over IPv4 OR IPv6 at runtime — no CONNECT
// proxy needed (the old yt-v6-proxy was a pre-fork workaround for reqwest4j not
// honouring v6). Clients are cheap to clone (Arc inside).
static FAMILY_CLIENTS: OnceLock<Mutex<HashMap<String, Client>>> = OnceLock::new();

fn http3_enabled() -> bool {
    !matches!(
        std::env::var("R4J_HTTP3").ok().as_deref(),
        Some("0") | Some("false") | Some("FALSE")
    )
}

// Build (and cache) a client pinned to the given IP family. h3=true gives an
// HTTP/3 (QUIC) client; QUIC works on either family over a direct socket (unlike
// the old HTTP-CONNECT proxy, which couldn't tunnel UDP — that's why dropping the
// proxy unlocks h3 on v6 too).
fn get_family_client(family: &str, h3: bool) -> Client {
    let key = if h3 {
        format!("{family}-h3")
    } else {
        family.to_string()
    };
    let map = FAMILY_CLIENTS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = map.lock().unwrap();
    if let Some(c) = guard.get(&key) {
        return c.clone();
    }

    let local: IpAddr = if family == "v6" {
        IpAddr::V6(Ipv6Addr::UNSPECIFIED) // ::  -> egress over IPv6
    } else {
        IpAddr::V4(Ipv4Addr::UNSPECIFIED) // 0.0.0.0 -> egress over IPv4
    };

    // quinn (h3) sets up a QUIC endpoint at build time and needs a live tokio
    // runtime; we're on the JNI thread, so enter ours (harmless for the h2 case).
    let _guard = RUNTIME
        .get()
        .expect("RUNTIME not initialized before client build")
        .enter();

    let mut builder = Client::builder()
        .user_agent(UA)
        .local_address(local)
        .timeout(Duration::from_secs(30));
    builder = if h3 {
        // prior-knowledge: no Alt-Svc discovery, no in-client h2 fallback — we do
        // the h3->h2 fallback ourselves. Short connect timeout so a blocked UDP
        // path fails over quickly.
        builder
            .http3_prior_knowledge()
            .connect_timeout(Duration::from_secs(3))
    } else {
        builder.connect_timeout(Duration::from_secs(10))
    };

    let client = builder.build().unwrap();
    guard.insert(key, client.clone());
    client
}

#[no_mangle]
pub extern "system" fn Java_rocks_kavin_reqwest4j_ReqwestUtils_init(
    mut env: JNIEnv,
    _: JClass,
    proxy: JString,
    user: JString,
    pass: JString,
) {
    let builder = Client::builder().user_agent(UA);

    let builder = match env.get_string(&proxy) {
        Ok(proxy) => {
            let proxy = proxy.to_str().unwrap();
            let proxy = reqwest::Proxy::all(proxy).unwrap();
            let proxy = match env.get_string(&user) {
                Ok(user) => {
                    let user = user.to_str().unwrap();
                    let pass = env.get_string(&pass).unwrap();
                    let pass = pass.to_str().unwrap();
                    proxy.basic_auth(user, pass)
                }
                Err(_) => proxy,
            };
            builder.proxy(proxy)
        }
        Err(_) => builder,
    };

    let client = builder
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    CLIENT.set(client).unwrap();
    RUNTIME.set(Runtime::new().unwrap()).unwrap();
}

#[no_mangle]
pub extern "system" fn Java_rocks_kavin_reqwest4j_ReqwestUtils_fetch(
    mut env: JNIEnv,
    _: JClass,
    url: JString,
    method: JString,
    body: JByteArray,
    headers: JObject,
) -> jobject {
    let client = match CLIENT.get() {
        Some(c) => c.clone(),
        None => {
            env.throw_new("java/lang/IllegalStateException", "Client not initialized")
                .unwrap();
            return JObject::null().into_raw();
        }
    };
    run_fetch(env, url, method, body, headers, vec![(client, None)])
}

// fetchWithProxy: the 5th arg is an EGRESS FAMILY selector now ("v6" => IPv6,
// anything else => IPv4), not a proxy URL. With HTTP/3 enabled, each request
// tries h3 (QUIC) on that family first, then falls back to h2 on the same family.
#[no_mangle]
pub extern "system" fn Java_rocks_kavin_reqwest4j_ReqwestUtils_fetchWithProxy(
    mut env: JNIEnv,
    _: JClass,
    url: JString,
    method: JString,
    body: JByteArray,
    headers: JObject,
    proxy: JString,
) -> jobject {
    let selector = match env.get_string(&proxy) {
        Ok(s) => s.to_str().unwrap_or("").to_string(),
        Err(_) => String::new(),
    };
    let family = if selector.contains("v6") { "v6" } else { "v4" };

    let attempts: Vec<(Client, Option<Version>)> = if http3_enabled() {
        vec![
            (get_family_client(family, true), Some(Version::HTTP_3)),
            (get_family_client(family, false), None),
        ]
    } else {
        vec![(get_family_client(family, false), None)]
    };

    run_fetch(env, url, method, body, headers, attempts)
}

// Shared request path. Parses url/method/headers/body once, then tries each
// (client, version) attempt in order, completing the CompletableFuture on the
// first success and only failing it (completeExceptionally) if every attempt
// errors. A single attempt = upstream's behaviour; multiple = h3-then-h2 fallback.
fn run_fetch(
    mut env: JNIEnv,
    url: JString,
    method: JString,
    body: JByteArray,
    headers: JObject,
    attempts: Vec<(Client, Option<Version>)>,
) -> jobject {
    let method = Method::from_bytes(env.get_string(&method).unwrap().to_bytes()).unwrap();

    let url = &env.get_string(&url).unwrap();
    let url = url.to_str();

    if url.is_err() {
        env.throw_new(
            "java/lang/IllegalArgumentException",
            "Invalid URL provided, couldn't get string as UTF-8",
        )
        .unwrap();
        return JObject::null().into_raw();
    }

    let url = Url::parse(url.unwrap()).unwrap();
    let body = env.convert_byte_array(body).unwrap_or_default();
    let java_headers: JMap = JMap::from_env(&mut env, &headers).unwrap();
    let mut java_headers = java_headers.iter(&mut env).unwrap();
    let mut headers = HashMap::new();
    while let Some((key, value)) = java_headers.next(&mut env).unwrap() {
        headers.insert(
            env.get_string(&JString::from(key))
                .unwrap()
                .to_str()
                .unwrap()
                .to_string(),
            env.get_string(&JString::from(value))
                .unwrap()
                .to_str()
                .unwrap()
                .to_string(),
        );
    }

    let jvm = env.get_java_vm().unwrap();
    let jvm = Arc::new(jvm);

    let _future = env
        .new_object("java/util/concurrent/CompletableFuture", "()V", &[])
        .unwrap();
    let future = env.new_global_ref(&_future).unwrap();
    let future = Arc::new(future);

    let runtime = RUNTIME.get().unwrap();

    {
        let jvm = Arc::clone(&jvm);
        let future = Arc::clone(&future);

        runtime.spawn(async move {
            let n = attempts.len();
            let mut last_error: Option<String> = None;

            for (i, (client, version)) in attempts.into_iter().enumerate() {
                let mut request = client.request(method.clone(), url.clone());
                if let Some(v) = version {
                    request = request.version(v);
                }
                request = headers
                    .iter()
                    .fold(request, |request, (key, value)| request.header(key, value));
                if !body.is_empty() {
                    request = request.body(body.clone());
                }

                match request.send().await {
                    Ok(response) => {
                        let status = response.status().as_u16() as i32;
                        let final_url = response.url().to_string();
                        let response_headers = response.headers().clone();
                        let resp_body = response.bytes().await.unwrap_or_default().to_vec();

                        let jvm = Arc::clone(&jvm);
                        let future = Arc::clone(&future);
                        runtime.spawn_blocking(move || {
                            let mut env = jvm.attach_current_thread().unwrap();

                            let final_url = env.new_string(final_url).unwrap();
                            let resp_body = env.byte_array_from_slice(&resp_body).unwrap();

                            let headers = env.new_object("java/util/HashMap", "()V", &[]).unwrap();
                            let headers: JMap = JMap::from_env(&mut env, &headers).unwrap();

                            response_headers.iter().for_each(|(key, value)| {
                                let key = env.new_string(key.as_str()).unwrap();
                                let value = env.new_string(value.to_str().unwrap()).unwrap();
                                headers
                                    .put(&mut env, &JObject::from(key), &JObject::from(value))
                                    .unwrap();
                            });

                            let response = env
                                .new_object(
                                    "rocks/kavin/reqwest4j/Response",
                                    "(ILjava/util/Map;[BLjava/lang/String;)V",
                                    &[
                                        status.into(),
                                        (&headers).into(),
                                        (&resp_body).into(),
                                        (&final_url).into(),
                                    ],
                                )
                                .unwrap();

                            let future = future.as_obj();
                            env.call_method(
                                future,
                                "complete",
                                "(Ljava/lang/Object;)Z",
                                &[(&response).into()],
                            )
                            .unwrap();
                        });
                        return; // first success wins
                    }
                    Err(error) => {
                        last_error = Some(error.to_string());
                        if i + 1 < n {
                            continue; // fall back to the next attempt (h3 -> h2)
                        }
                    }
                }
            }

            let jvm = Arc::clone(&jvm);
            let future = Arc::clone(&future);
            let msg = last_error.unwrap_or_else(|| "request failed".to_string());
            runtime.spawn_blocking(move || {
                let mut env = jvm.attach_current_thread().unwrap();
                let error = env.new_string(msg).unwrap();
                let exception = env
                    .new_object(
                        "java/lang/Exception",
                        "(Ljava/lang/String;)V",
                        &[(&error).into()],
                    )
                    .unwrap();
                let future = future.as_obj();
                env.call_method(
                    future,
                    "completeExceptionally",
                    "(Ljava/lang/Throwable;)Z",
                    &[(&exception).into()],
                )
                .unwrap();
            });
        });
    }

    _future.into_raw()
}
