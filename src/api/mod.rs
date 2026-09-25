use std::{net::SocketAddr, str::FromStr, sync::Arc};

use axum::{
    Json, Router,
    extract::{Path, Request, State},
    http::{HeaderMap, StatusCode, header::AUTHORIZATION},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use crate::{
    config::SharedWatchdogConfig,
    supervisor::{Status, Watchdog},
};

#[derive(Clone)]
pub struct AppState {
    pub watchdog: Arc<Watchdog>,
    pub admin_token: Option<String>,
}

pub fn router(state: AppState) -> Router {
    let admin = Router::new()
        .route("/instances", get(list_instances))
        .route("/instances/{key}/status", get(instance_status))
        .route("/instances/{key}/start", post(start_instance))
        .route("/instances/{key}/stop", post(stop_instance))
        .route("/instances/{key}/restart", post(restart_instance))
        .route(
            "/instances/{key}/force-restart",
            post(force_restart_instance),
        )
        .route("/instances/{key}/silence", post(silence_instance))
        .route("/instances/{key}/unsilence", post(unsilence_instance))
        .route("/instances/{key}/history", get(instance_history))
        .route("/instances/{key}/logs", get(instance_logs))
        .route("/reload", post(reload_config))
        .layer(middleware::from_fn_with_state(state.clone(), require_admin));

    // update accepts either admin bearer (from the CLI) or basic key:apiToken
    // (from Robust.Cdn's NotifyWatchdogUpdateJob). Same handler either way.
    let update = Router::new()
        .route("/instances/{key}/update", post(update_instance))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_admin_or_instance_basic,
        ));

    let game = Router::new()
        .route("/server_api/{key}/ping", post(ping_instance))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_instance_basic,
        ));

    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .merge(admin)
        .merge(update)
        .merge(game)
        .with_state(state)
}

pub async fn serve(
    cfg: SharedWatchdogConfig,
    watchdog: Arc<Watchdog>,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    let addr = SocketAddr::from_str(&cfg.admin.bind)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(bind = %addr, "admin API listening");

    let state = AppState {
        watchdog,
        admin_token: cfg.admin.token.clone(),
    };
    if state.admin_token.is_none() {
        tracing::warn!("no [admin].token set; admin API will refuse every request");
    }

    axum::serve(listener, router(state))
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
}

async fn health() -> &'static str {
    "ok"
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    use crate::db::incidents::{Kind, count};
    use crate::supervisor::Status;
    use std::fmt::Write as _;

    let mut buf = String::with_capacity(2048);
    let _ = writeln!(
        buf,
        "# HELP cerberus_build_info Build metadata; value is always 1."
    );
    let _ = writeln!(buf, "# TYPE cerberus_build_info gauge");
    let _ = writeln!(
        buf,
        "cerberus_build_info{{version=\"{}\"}} 1",
        env!("CARGO_PKG_VERSION")
    );

    let _ = writeln!(
        buf,
        "# HELP cerberus_instance_up 1 if the instance's supervisor is in state=running."
    );
    let _ = writeln!(buf, "# TYPE cerberus_instance_up gauge");
    let _ = writeln!(
        buf,
        "# HELP cerberus_instance_uptime_seconds Seconds since the current process started; 0 if not running."
    );
    let _ = writeln!(buf, "# TYPE cerberus_instance_uptime_seconds gauge");
    let _ = writeln!(
        buf,
        "# HELP cerberus_instance_last_ping_seconds Seconds since the last heartbeat from this instance; -1 if never seen."
    );
    let _ = writeln!(buf, "# TYPE cerberus_instance_last_ping_seconds gauge");
    let _ = writeln!(
        buf,
        "# HELP cerberus_instance_backoff_seconds Seconds remaining in the current backoff wait; 0 outside backoff."
    );
    let _ = writeln!(buf, "# TYPE cerberus_instance_backoff_seconds gauge");
    let _ = writeln!(
        buf,
        "# HELP cerberus_instance_failures Consecutive spawn/heartbeat failures counting toward backoff."
    );
    let _ = writeln!(buf, "# TYPE cerberus_instance_failures gauge");
    let _ = writeln!(
        buf,
        "# HELP cerberus_instance_silenced 1 if the instance's stdout/stderr is currently dropped."
    );
    let _ = writeln!(buf, "# TYPE cerberus_instance_silenced gauge");
    let _ = writeln!(
        buf,
        "# HELP cerberus_instance_starts_total Successful process spawns; each restart increments."
    );
    let _ = writeln!(buf, "# TYPE cerberus_instance_starts_total counter");
    let _ = writeln!(
        buf,
        "# HELP cerberus_instance_crashes_total Non-clean exits (non-zero status or unexpected signal)."
    );
    let _ = writeln!(buf, "# TYPE cerberus_instance_crashes_total counter");
    let _ = writeln!(
        buf,
        "# HELP cerberus_instance_clean_exits_total Clean exits (status 0), e.g. in-game admin shutdown."
    );
    let _ = writeln!(buf, "# TYPE cerberus_instance_clean_exits_total counter");
    let _ = writeln!(
        buf,
        "# HELP cerberus_instance_heartbeat_lost_total Times heartbeat timeout forced a restart."
    );
    let _ = writeln!(buf, "# TYPE cerberus_instance_heartbeat_lost_total counter");

    for (key, sup) in state.watchdog.iter() {
        let label = format!("key=\"{}\"", escape(key));
        let status = sup.status().await;
        let (up, uptime, last_ping, backoff_s, failures) = match &status {
            Status::Running {
                uptime_secs,
                last_ping_secs_ago,
                ..
            } => (
                1,
                *uptime_secs as i64,
                last_ping_secs_ago.map(|s| s as i64).unwrap_or(-1),
                0,
                0,
            ),
            Status::Backoff {
                wait_secs,
                failures,
            } => (0, 0, -1, *wait_secs as i64, *failures as i64),
            Status::Starting | Status::Updating | Status::Stopping => (0, 0, -1, 0, 0),
            Status::Stopped | Status::Failed { .. } => (0, 0, -1, 0, 0),
        };

        let _ = writeln!(buf, "cerberus_instance_up{{{label}}} {up}");
        let _ = writeln!(buf, "cerberus_instance_uptime_seconds{{{label}}} {uptime}");
        let _ = writeln!(
            buf,
            "cerberus_instance_last_ping_seconds{{{label}}} {last_ping}"
        );
        let _ = writeln!(
            buf,
            "cerberus_instance_backoff_seconds{{{label}}} {backoff_s}"
        );
        let _ = writeln!(buf, "cerberus_instance_failures{{{label}}} {failures}");
        let _ = writeln!(
            buf,
            "cerberus_instance_silenced{{{label}}} {}",
            if sup.is_silent() { 1 } else { 0 }
        );

        let starts = count(sup.db(), key, Kind::Start).await.unwrap_or(0);
        let crashes = count(sup.db(), key, Kind::Crashed).await.unwrap_or(0);
        let clean_exits = count(sup.db(), key, Kind::CleanExit).await.unwrap_or(0);
        let hb_lost = count(sup.db(), key, Kind::HeartbeatLost).await.unwrap_or(0);
        let _ = writeln!(buf, "cerberus_instance_starts_total{{{label}}} {starts}");
        let _ = writeln!(buf, "cerberus_instance_crashes_total{{{label}}} {crashes}");
        let _ = writeln!(
            buf,
            "cerberus_instance_clean_exits_total{{{label}}} {clean_exits}"
        );
        let _ = writeln!(
            buf,
            "cerberus_instance_heartbeat_lost_total{{{label}}} {hb_lost}"
        );
    }

    (
        [("Content-Type", "text/plain; version=0.0.4; charset=utf-8")],
        buf,
    )
}

fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            _ => out.push(c),
        }
    }
    out
}

async fn require_admin(
    State(state): State<AppState>,
    headers: HeaderMap,
    req: Request,
    next: Next,
) -> Response {
    let Some(expected) = state.admin_token.as_deref() else {
        return (StatusCode::UNAUTHORIZED, "admin token not configured").into_response();
    };
    let presented = headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    let ok = presented.is_some_and(|p| constant_time_eq(p.as_bytes(), expected.as_bytes()));
    if !ok {
        return (StatusCode::UNAUTHORIZED, "bad admin token").into_response();
    }
    next.run(req).await
}

async fn require_instance_basic(
    State(state): State<AppState>,
    headers: HeaderMap,
    req: Request,
    next: Next,
) -> Response {
    let path = req.uri().path().to_owned();
    let Some(url_key) = extract_server_api_key(&path) else {
        return (StatusCode::BAD_REQUEST, "no instance key").into_response();
    };
    let raw = headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let Some((user, pass)) = parse_basic(raw) else {
        return unauthorized_basic();
    };
    if user != url_key {
        return (StatusCode::FORBIDDEN, "key mismatch").into_response();
    }
    let Some(sup) = state.watchdog.get(&url_key) else {
        return (StatusCode::NOT_FOUND, "unknown instance").into_response();
    };
    if !constant_time_eq(pass.as_bytes(), sup.api_token().as_bytes()) {
        return unauthorized_basic();
    }
    next.run(req).await
}

fn extract_server_api_key(path: &str) -> Option<String> {
    let rest = path.strip_prefix("/server_api/")?;
    let end = rest.find('/').unwrap_or(rest.len());
    Some(rest[..end].to_string())
}

fn extract_instance_key(path: &str) -> Option<String> {
    let rest = path.strip_prefix("/instances/")?;
    let end = rest.find('/').unwrap_or(rest.len());
    Some(rest[..end].to_string())
}

// try admin bearer first; on fail fall back to basic key:apiToken.
// used by /instances/{key}/update so both the CLI and Robust.Cdn's
// NotifyWatchdogUpdateJob can hit the same handler.
async fn require_admin_or_instance_basic(
    State(state): State<AppState>,
    headers: HeaderMap,
    req: Request,
    next: Next,
) -> Response {
    let raw = headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if let Some(presented) = raw.strip_prefix("Bearer ") {
        if let Some(expected) = state.admin_token.as_deref()
            && constant_time_eq(presented.as_bytes(), expected.as_bytes())
        {
            return next.run(req).await;
        }
        return (StatusCode::UNAUTHORIZED, "bad admin token").into_response();
    }

    if raw.starts_with("Basic ") {
        let path = req.uri().path().to_owned();
        let Some(url_key) = extract_instance_key(&path) else {
            return (StatusCode::BAD_REQUEST, "no instance key").into_response();
        };
        let Some((user, pass)) = parse_basic(raw) else {
            return unauthorized_basic();
        };
        if user != url_key {
            return (StatusCode::FORBIDDEN, "key mismatch").into_response();
        }
        let Some(sup) = state.watchdog.get(&url_key) else {
            return (StatusCode::NOT_FOUND, "unknown instance").into_response();
        };
        if !constant_time_eq(pass.as_bytes(), sup.api_token().as_bytes()) {
            return unauthorized_basic();
        }
        return next.run(req).await;
    }

    (StatusCode::UNAUTHORIZED, "missing auth").into_response()
}

fn parse_basic(header: &str) -> Option<(String, String)> {
    use base64::Engine as _;
    let encoded = header.strip_prefix("Basic ")?;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .ok()?;
    let s = std::str::from_utf8(&decoded).ok()?;
    let (u, p) = s.split_once(':')?;
    Some((u.to_string(), p.to_string()))
}

fn unauthorized_basic() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [("WWW-Authenticate", "Basic realm=\"ss14watchdog\"")],
        "bad basic credentials",
    )
        .into_response()
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut acc = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        acc |= x ^ y;
    }
    acc == 0
}

#[derive(Serialize)]
struct InstanceEntry {
    key: String,
    name: String,
    #[serde(flatten)]
    status: Status,
}

async fn list_instances(State(state): State<AppState>) -> Json<Vec<InstanceEntry>> {
    let mut out = Vec::new();
    for (key, sup) in state.watchdog.iter() {
        out.push(InstanceEntry {
            key: key.clone(),
            name: sup.display_name().to_string(),
            status: sup.status().await,
        });
    }
    Json(out)
}

async fn instance_status(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Json<InstanceEntry>, Response> {
    let sup = state.watchdog.get(&key).ok_or_else(not_found)?;
    Ok(Json(InstanceEntry {
        key,
        name: sup.display_name().to_string(),
        status: sup.status().await,
    }))
}

async fn start_instance(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<StatusCode, Response> {
    state
        .watchdog
        .get(&key)
        .ok_or_else(not_found)?
        .restart()
        .await
        .map_err(sup_err)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn stop_instance(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<StatusCode, Response> {
    state
        .watchdog
        .get(&key)
        .ok_or_else(not_found)?
        .stop()
        .await
        .map_err(sup_err)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn restart_instance(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<StatusCode, Response> {
    state
        .watchdog
        .get(&key)
        .ok_or_else(not_found)?
        .restart()
        .await
        .map_err(sup_err)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn force_restart_instance(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<StatusCode, Response> {
    state
        .watchdog
        .get(&key)
        .ok_or_else(not_found)?
        .force_restart()
        .await
        .map_err(sup_err)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn update_instance(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Json<crate::supervisor::UpdateOutcome>, Response> {
    let outcome = state
        .watchdog
        .get(&key)
        .ok_or_else(not_found)?
        .update()
        .await
        .map_err(sup_err)?;
    Ok(Json(outcome))
}

async fn ping_instance(State(state): State<AppState>, Path(key): Path<String>) -> StatusCode {
    if let Some(sup) = state.watchdog.get(&key) {
        sup.record_ping().await;
    }
    StatusCode::NO_CONTENT
}

#[derive(serde::Deserialize)]
struct HistoryQuery {
    #[serde(default = "default_history_limit")]
    limit: i64,
}

fn default_history_limit() -> i64 {
    50
}

async fn instance_history(
    State(state): State<AppState>,
    Path(key): Path<String>,
    axum::extract::Query(q): axum::extract::Query<HistoryQuery>,
) -> Result<Json<Vec<crate::db::incidents::Incident>>, Response> {
    let sup = state.watchdog.get(&key).ok_or_else(not_found)?;
    let limit = q.limit.clamp(1, 1000);
    let rows = crate::db::incidents::list_recent(sup.db(), &key, limit)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response())?;
    Ok(Json(rows))
}

async fn instance_logs(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<
    axum::response::sse::Sse<
        impl futures_util::Stream<Item = Result<axum::response::sse::Event, std::convert::Infallible>>,
    >,
    Response,
> {
    use axum::response::sse::{Event, KeepAlive, Sse};
    use tokio::sync::broadcast::error::RecvError;

    let sup = state.watchdog.get(&key).ok_or_else(not_found)?;
    let rx = sup.subscribe_logs().ok_or_else(not_found)?;
    let stream = futures_util::stream::unfold(rx, |mut rx| async move {
        match rx.recv().await {
            Ok(line) => Some((Ok(Event::default().data(line)), rx)),
            Err(RecvError::Lagged(n)) => {
                Some((Ok(Event::default().data(format!("[lagged {n}]"))), rx))
            }
            Err(RecvError::Closed) => None,
        }
    });
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

async fn silence_instance(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<StatusCode, Response> {
    state
        .watchdog
        .get(&key)
        .ok_or_else(not_found)?
        .set_silent(true);
    Ok(StatusCode::NO_CONTENT)
}

async fn unsilence_instance(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<StatusCode, Response> {
    state
        .watchdog
        .get(&key)
        .ok_or_else(not_found)?
        .set_silent(false);
    Ok(StatusCode::NO_CONTENT)
}

async fn reload_config(
    State(state): State<AppState>,
) -> Result<Json<crate::supervisor::ReloadReport>, Response> {
    let watchdog = state.watchdog.clone();
    let result = tokio::task::spawn_blocking(move || watchdog.reload())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response())?;
    match result {
        Ok(report) => Ok(Json(report)),
        Err(e) => Err((StatusCode::BAD_REQUEST, e).into_response()),
    }
}

fn not_found() -> Response {
    (StatusCode::NOT_FOUND, "unknown instance").into_response()
}

fn sup_err(e: crate::supervisor::SupervisorError) -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response()
}
