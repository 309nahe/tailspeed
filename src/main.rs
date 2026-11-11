//! speed: a tiny speed test for a Tailscale node.
//!
//! Measures download/upload throughput and latency between the browser and this
//! server, and asks the local tailscaled how that device is connected: a direct
//! WireGuard path or a DERP relay. Results are kept in RAM only (last 100).
//!
//! Expects to sit behind `tailscale serve`, which sets X-Forwarded-For and the
//! Tailscale-User-* identity headers.

use axum::{
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use futures_util::{stream, StreamExt};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::VecDeque,
    process::Stdio,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{process::Command, sync::Semaphore, time::timeout};

/// Random payload reused for every download chunk (incompressible).
const CHUNK: usize = 1 << 20;
/// Largest single download or upload request.
const MAX_TRANSFER: u64 = 512 * 1024 * 1024;
/// Results kept in memory.
const HISTORY: usize = 100;

static PAYLOAD: OnceLock<Bytes> = OnceLock::new();

#[derive(Clone, Serialize, Deserialize)]
struct Result_ {
    #[serde(default)]
    at: u64,
    #[serde(default)]
    host: String,
    #[serde(default)]
    ip: String,
    #[serde(default)]
    path: String,
    down: f64,
    up: f64,
    ping: f64,
    jitter: f64,
}

struct AppState {
    results: Mutex<VecDeque<Result_>>,
    status: tokio::sync::Mutex<(Option<Instant>, Value)>,
    pings: Semaphore,
}

type St = Arc<AppState>;

fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

/// Client tailnet IP as reported by tailscale serve.
fn client_ip(h: &HeaderMap) -> String {
    h.get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.split(',').next())
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

fn hdr(h: &HeaderMap, k: &str) -> String {
    h.get(k).and_then(|v| v.to_str().ok()).unwrap_or("").to_string()
}

fn no_store() -> [(header::HeaderName, &'static str); 1] {
    [(header::CACHE_CONTROL, "no-store")]
}

#[derive(Deserialize)]
struct DownQ {
    bytes: Option<u64>,
}

/// GET /api/down?bytes=N: streams N incompressible bytes.
async fn down(Query(q): Query<DownQ>) -> Response {
    let total = q.bytes.unwrap_or(25 << 20).clamp(1, MAX_TRANSFER);
    let payload = PAYLOAD.get().unwrap().clone();
    let body = stream::unfold(total, move |left| {
        let payload = payload.clone();
        async move {
            if left == 0 {
                return None;
            }
            let n = left.min(CHUNK as u64) as usize;
            Some((Ok::<_, std::io::Error>(payload.slice(0..n)), left - n as u64))
        }
    });
    (
        [
            (header::CONTENT_TYPE, "application/octet-stream".to_string()),
            (header::CONTENT_LENGTH, total.to_string()),
            (header::CACHE_CONTROL, "no-store".to_string()),
            (header::CONTENT_ENCODING, "identity".to_string()),
        ],
        Body::from_stream(body),
    )
        .into_response()
}

/// POST /api/up: reads and discards the body, returns the byte count.
async fn up(body: Body) -> Result<Json<Value>, (StatusCode, String)> {
    let mut s = body.into_data_stream();
    let mut n: u64 = 0;
    while let Some(chunk) = s.next().await {
        let c = chunk.map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
        n += c.len() as u64;
        if n > MAX_TRANSFER {
            return Err((StatusCode::PAYLOAD_TOO_LARGE, "Upload too large".into()));
        }
    }
    Ok(Json(serde_json::json!({ "bytes": n })))
}

/// `tailscale status --json`, cached for 3 s so page loads stay cheap.
async fn ts_status(st: &St) -> Value {
    let mut g = st.status.lock().await;
    if g.0.is_some_and(|t| t.elapsed() < Duration::from_secs(3)) {
        return g.1.clone();
    }
    let out = timeout(
        Duration::from_secs(5),
        Command::new("tailscale").args(["status", "--json"]).stdin(Stdio::null()).stderr(Stdio::null()).output(),
    )
    .await;
    let v = match out {
        Ok(Ok(o)) if o.status.success() => serde_json::from_slice(&o.stdout).unwrap_or(Value::Null),
        _ => Value::Null,
    };
    *g = (Some(Instant::now()), v.clone());
    v
}

fn find_peer<'a>(status: &'a Value, ip: &str) -> Option<&'a Value> {
    let has = |p: &Value| p["TailscaleIPs"].as_array().is_some_and(|a| a.iter().any(|x| x.as_str() == Some(ip)));
    if has(&status["Self"]) {
        return Some(&status["Self"]);
    }
    status["Peer"].as_object()?.values().find(|p| has(p))
}

/// GET /api/whoami: who is testing, and how their device reaches this node.
async fn whoami(State(st): State<St>, h: HeaderMap) -> impl IntoResponse {
    let ip = client_ip(&h);
    let status = ts_status(&st).await;
    let server = serde_json::json!({
        "host": status["Self"]["HostName"],
        "os": status["Self"]["OS"],
        "relay": status["Self"]["Relay"],
    });
    let peer = find_peer(&status, &ip).map(|p| {
        let cur = p["CurAddr"].as_str().unwrap_or("");
        let is_self = p["ID"] == status["Self"]["ID"];
        serde_json::json!({
            "host": p["HostName"],
            "os": p["OS"],
            "online": p["Online"],
            "relay": p["Relay"],
            "direct": is_self || !cur.is_empty(),
            "self": is_self,
        })
    });
    (
        no_store(),
        Json(serde_json::json!({
            "ip": ip,
            "user": hdr(&h, "tailscale-user-name"),
            "login": hdr(&h, "tailscale-user-login"),
            "peer": peer,
            "server": server,
        })),
    )
}

/// POST /api/path: runs `tailscale ping` to the client. Besides reporting the
/// route, this nudges tailscaled to try a direct connection.
async fn path(State(st): State<St>, h: HeaderMap) -> Result<Json<Value>, (StatusCode, String)> {
    let ip = client_ip(&h);
    if ip.parse::<std::net::IpAddr>().is_err() {
        return Err((StatusCode::BAD_REQUEST, "Unknown client address (not behind tailscale serve?)".into()));
    }
    let _p = st.pings.try_acquire().map_err(|_| (StatusCode::TOO_MANY_REQUESTS, "A path check is already running".into()))?;
    let out = timeout(
        Duration::from_secs(12),
        Command::new("tailscale")
            .args(["ping", "-c", "4", "--timeout", "2s", &ip])
            .stdin(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .map_err(|_| (StatusCode::GATEWAY_TIMEOUT, "Path check timed out".into()))?
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    // "pong from host (ip) via DERP(par) in 209ms" / "... via 1.2.3.4:41641 in 5ms"
    let pongs: Vec<Value> = text
        .lines()
        .filter_map(|l| {
            let via = l.split(" via ").nth(1)?.split(" in ").next()?.to_string();
            let ms = l.rsplit(" in ").next()?.trim().trim_end_matches("ms").parse::<f64>().ok()?;
            Some(serde_json::json!({ "via": via, "ms": ms, "relay": via.starts_with("DERP") }))
        })
        .collect();
    let direct = pongs.last().is_some_and(|p| p["relay"] == false);
    st.status.lock().await.0 = None;
    Ok(Json(serde_json::json!({ "pongs": pongs, "direct": direct, "timeout": pongs.is_empty() })))
}

async fn list(State(st): State<St>) -> impl IntoResponse {
    let v: Vec<Result_> = st.results.lock().unwrap().iter().rev().cloned().collect();
    (no_store(), Json(v))
}

/// POST /api/results: the browser reports its numbers; the server fills in who and how.
async fn save(State(st): State<St>, h: HeaderMap, Json(mut r): Json<Result_>) -> StatusCode {
    let ok = |x: f64| x.is_finite() && (0.0..1e6).contains(&x);
    if ![r.down, r.up, r.ping, r.jitter].iter().all(|x| ok(*x)) {
        return StatusCode::BAD_REQUEST;
    }
    r.ip = client_ip(&h);
    let status = ts_status(&st).await;
    let peer = find_peer(&status, &r.ip);
    r.host = peer.and_then(|p| p["HostName"].as_str()).unwrap_or("unknown").chars().take(60).collect();
    r.path = match peer {
        Some(p) if p["ID"] == status["Self"]["ID"] => "local".into(),
        Some(p) if !p["CurAddr"].as_str().unwrap_or("").is_empty() => "direct".into(),
        Some(p) => format!("relay {}", p["Relay"].as_str().unwrap_or("?")),
        None => "unknown".into(),
    };
    r.at = now();
    let mut g = st.results.lock().unwrap();
    g.push_back(r);
    while g.len() > HISTORY {
        g.pop_front();
    }
    StatusCode::NO_CONTENT
}

#[tokio::main]
async fn main() {
    let mut buf = vec![0u8; CHUNK];
    rand::rng().fill_bytes(&mut buf);
    PAYLOAD.set(Bytes::from(buf)).ok();
    let st: St = Arc::new(AppState {
        results: Mutex::new(VecDeque::new()),
        status: tokio::sync::Mutex::new((None, Value::Null)),
        pings: Semaphore::new(1),
    });
    let app = Router::new()
        .route("/", get(|| async { Html(include_str!("index.html")) }))
        .route("/api/ping", get(|| async { (no_store(), StatusCode::NO_CONTENT) }))
        .route("/api/down", get(down))
        .route("/api/up", post(up).layer(DefaultBodyLimit::disable()))
        .route("/api/whoami", get(whoami))
        .route("/api/path", post(path))
        .route("/api/results", get(list).post(save))
        .with_state(st);
    let addr = std::env::var("BIND").unwrap_or_else(|_| "127.0.0.1:8453".into());
    let l = tokio::net::TcpListener::bind(&addr).await.expect("bind");
    println!("listening on {addr}");
    axum::serve(l, app).await.unwrap();
}
