//! one supervisor per server: spawns it, restarts on crash, honors admin commands.

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    process::ExitStatus,
    sync::{
        Arc, RwLock as StdRwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use serde::Serialize;
use sqlx::SqlitePool;
use tokio::{
    sync::{Mutex, broadcast, mpsc, oneshot},
    task::JoinHandle,
    time::sleep,
};

use crate::{
    config::{ServerConfig, SharedWatchdogConfig, UpdateType, WatchdogConfig},
    db,
    instances::{
        InstanceHandle, InstanceProvider, InstanceSpec, LogBus, ProcessInstanceProvider,
        SilenceRegistry, new_log_broadcaster,
    },
    updates::{UpdateProvider, manifest::ManifestUpdateProvider},
};

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum UpdateOutcome {
    NoUpdate,
    // running server was told to wind down; download runs on its next exit
    Notified { notification_sent: bool },
    // instance was stopped, download ran now
    Applied,
    Failed { reason: String },
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum Status {
    Stopped,
    Starting,
    Running {
        pid: u32,
        version: String,
        uptime_secs: u64,
        last_ping_secs_ago: Option<u64>,
    },
    Backoff {
        wait_secs: u64,
        failures: u32,
    },
    Stopping,
    Updating,
    Failed {
        reason: String,
    },
}

enum Command {
    Stop(oneshot::Sender<()>),
    Kill(oneshot::Sender<()>),
    Restart(oneshot::Sender<()>),
    ForceRestart(oneshot::Sender<()>),
    // never disturbs a running server, see handle_update_running
    Update(oneshot::Sender<UpdateOutcome>),
    Shutdown(oneshot::Sender<()>),
}

#[derive(Clone)]
pub struct Supervisor {
    inner: Arc<Inner>,
    tx: mpsc::Sender<Command>,
}

struct Inner {
    key: String,
    display_name: String,
    // swappable so reload picks up new values on the next spawn without
    // disturbing the running process
    server: StdRwLock<Arc<ServerConfig>>,
    state: Mutex<InternalState>,
    last_ping: Mutex<Option<Instant>>,
    db: SqlitePool,
    provider: Arc<dyn InstanceProvider>,
    updater: StdRwLock<Arc<dyn UpdateProvider>>,
    admin_base_url: String,
    silenced: SilenceRegistry,
    log_bus: LogBus,
    // set by `update` when a new version is found and drained on next exit
    pending_update: AtomicBool,
    // for POST /shutdown and /update on the game server
    game_client: reqwest::Client,
}

enum InternalState {
    Stopped,
    Starting,
    Running {
        pid: u32,
        started_at: Instant,
        version: String,
    },
    Backoff {
        until: Instant,
        failures: u32,
    },
    Stopping,
    Updating,
    Failed {
        reason: String,
    },
}

impl Inner {
    fn server(&self) -> Arc<ServerConfig> {
        self.server
            .read()
            .expect("server config lock poisoned")
            .clone()
    }

    fn updater(&self) -> Arc<dyn UpdateProvider> {
        self.updater.read().expect("updater lock poisoned").clone()
    }
}

fn build_updater(server: &ServerConfig) -> Arc<dyn UpdateProvider> {
    match &server.update.update_type {
        UpdateType::Manifest(m) => Arc::new(ManifestUpdateProvider::new(
            m.manifest_url.clone(),
            m.authentication.clone(),
        )),
    }
}

impl Supervisor {
    pub fn spawn(
        key: String,
        server: Arc<ServerConfig>,
        db: SqlitePool,
        admin_base_url: String,
        silenced: SilenceRegistry,
        log_bus: LogBus,
    ) -> Self {
        let updater = build_updater(&server);

        let game_client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(5))
            .build()
            .expect("build reqwest client");
        let inner = Arc::new(Inner {
            key: key.clone(),
            display_name: server.name.clone(),
            server: StdRwLock::new(server),
            state: Mutex::new(InternalState::Stopped),
            last_ping: Mutex::new(None),
            db,
            provider: Arc::new(ProcessInstanceProvider::new(
                silenced.clone(),
                log_bus.clone(),
            )),
            updater: StdRwLock::new(updater),
            admin_base_url,
            silenced,
            log_bus,
            pending_update: AtomicBool::new(false),
            game_client,
        });

        let (tx, rx) = mpsc::channel(16);
        let task_inner = inner.clone();
        tokio::spawn(run(task_inner, rx));

        Self { inner, tx }
    }

    pub fn key(&self) -> &str {
        &self.inner.key
    }
    pub fn display_name(&self) -> &str {
        &self.inner.display_name
    }
    pub fn api_token(&self) -> String {
        self.inner.server().api.token.clone()
    }
    pub fn db(&self) -> &SqlitePool {
        &self.inner.db
    }

    pub fn apply_config(&self, new: ServerConfig) {
        let new_arc = Arc::new(new);
        let updater = build_updater(&new_arc);
        *self
            .inner
            .server
            .write()
            .expect("server config lock poisoned") = new_arc;
        *self.inner.updater.write().expect("updater lock poisoned") = updater;
    }

    pub fn set_silent(&self, silent: bool) {
        let mut set = self
            .inner
            .silenced
            .write()
            .expect("silence registry poisoned");
        if silent {
            set.insert(self.inner.key.clone());
        } else {
            set.remove(&self.inner.key);
        }
    }

    pub fn subscribe_logs(&self) -> Option<broadcast::Receiver<String>> {
        let guard = self.inner.log_bus.read().ok()?;
        Some(guard.get(&self.inner.key)?.subscribe())
    }

    pub fn is_silent(&self) -> bool {
        self.inner
            .silenced
            .read()
            .map(|s| s.contains(&self.inner.key))
            .unwrap_or(false)
    }

    pub async fn record_ping(&self) {
        *self.inner.last_ping.lock().await = Some(Instant::now());
    }

    pub async fn status(&self) -> Status {
        let state = self.inner.state.lock().await;
        let last_ping = *self.inner.last_ping.lock().await;
        snapshot(&state, last_ping)
    }

    pub async fn stop(&self) -> Result<(), SupervisorError> {
        self.send(Command::Stop).await
    }
    pub async fn kill(&self) -> Result<(), SupervisorError> {
        self.send(Command::Kill).await
    }
    pub async fn restart(&self) -> Result<(), SupervisorError> {
        self.send(Command::Restart).await
    }
    pub async fn force_restart(&self) -> Result<(), SupervisorError> {
        self.send(Command::ForceRestart).await
    }
    pub async fn shutdown(&self) -> Result<(), SupervisorError> {
        self.send(Command::Shutdown).await
    }

    pub async fn update(&self) -> Result<UpdateOutcome, SupervisorError> {
        let (rtx, rrx) = oneshot::channel();
        self.tx
            .send(Command::Update(rtx))
            .await
            .map_err(|_| SupervisorError::TaskGone)?;
        rrx.await.map_err(|_| SupervisorError::TaskGone)
    }

    async fn send<F>(&self, make: F) -> Result<(), SupervisorError>
    where
        F: FnOnce(oneshot::Sender<()>) -> Command,
    {
        let (rtx, rrx) = oneshot::channel();
        self.tx
            .send(make(rtx))
            .await
            .map_err(|_| SupervisorError::TaskGone)?;
        rrx.await.map_err(|_| SupervisorError::TaskGone)?;
        Ok(())
    }
}

fn snapshot(s: &InternalState, last_ping: Option<Instant>) -> Status {
    match s {
        InternalState::Stopped => Status::Stopped,
        InternalState::Starting => Status::Starting,
        InternalState::Running {
            pid,
            started_at,
            version,
        } => Status::Running {
            pid: *pid,
            version: version.clone(),
            uptime_secs: started_at.elapsed().as_secs(),
            last_ping_secs_ago: last_ping.map(|t| t.elapsed().as_secs()),
        },
        InternalState::Backoff { until, failures } => Status::Backoff {
            wait_secs: until.saturating_duration_since(Instant::now()).as_secs(),
            failures: *failures,
        },
        InternalState::Stopping => Status::Stopping,
        InternalState::Updating => Status::Updating,
        InternalState::Failed { reason } => Status::Failed {
            reason: reason.clone(),
        },
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SupervisorError {
    #[error("supervisor task is not running")]
    TaskGone,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Desired {
    Running,
    Stopped,
    Shutdown,
}

enum RunOutcome {
    Crashed {
        uptime: Duration,
        status: Option<ExitStatus>,
    },
    // clean exit (status 0) - restart without backoff or failure bump
    CleanExit {
        uptime: Duration,
    },
    HeartbeatLost {
        uptime: Duration,
    },
    StoppedByCommand,
    Restart,
    ForceRestart,
    Shutdown,
    SpawnFailed(String),
}

async fn run(inner: Arc<Inner>, mut rx: mpsc::Receiver<Command>) {
    let key = inner.key.clone();
    tracing::info!(instance = %key, "supervisor started");
    let mut desired = Desired::Running;
    let mut failures: u32 = 0;

    loop {
        match desired {
            Desired::Shutdown => break,
            Desired::Stopped => match rx.recv().await {
                Some(cmd) => {
                    if let Some(d) = apply_command_while_stopped(&inner, cmd).await {
                        desired = d;
                    }
                }
                None => break,
            },
            Desired::Running => {
                if inner.pending_update.swap(false, Ordering::SeqCst) {
                    set_state(&inner, InternalState::Updating).await;
                    if let Err(e) = run_update(&inner).await {
                        tracing::error!(instance = %key, error = %e, "queued update failed");
                    }
                }
                if let Err(e) = ensure_version(&inner).await {
                    tracing::error!(instance = %key, error = %e, "cannot install initial version");
                    set_state(&inner, InternalState::Failed { reason: e.clone() }).await;
                    match rx.recv().await {
                        Some(cmd) => {
                            if let Some(d) = apply_command_while_stopped(&inner, cmd).await {
                                desired = d;
                            }
                        }
                        None => break,
                    }
                    continue;
                }

                match run_once(&inner, &mut rx).await {
                    RunOutcome::Shutdown => desired = Desired::Shutdown,
                    RunOutcome::StoppedByCommand => desired = Desired::Stopped,
                    RunOutcome::Restart | RunOutcome::ForceRestart => failures = 0,
                    RunOutcome::CleanExit { uptime } => {
                        tracing::info!(instance = %key, uptime_secs = uptime.as_secs(), "clean exit, restarting");
                        failures = 0;
                    }
                    RunOutcome::Crashed { uptime, status } => {
                        let cfg = inner.server();
                        if uptime >= Duration::from_secs(cfg.healthy_after_secs) {
                            failures = 0;
                        }
                        failures = failures.saturating_add(1);
                        let wait = backoff(cfg.restart_min_secs, cfg.restart_max_secs, failures);
                        tracing::warn!(
                            instance = %key, uptime_secs = uptime.as_secs(),
                            failures, wait_secs = wait.as_secs(),
                            ?status, "instance down; backing off"
                        );
                        set_state(
                            &inner,
                            InternalState::Backoff {
                                until: Instant::now() + wait,
                                failures,
                            },
                        )
                        .await;
                        if let Some(cmd) = wait_or_command(wait, &mut rx).await
                            && let Some(d) = apply_command_while_stopped(&inner, cmd).await
                        {
                            desired = d;
                        }
                    }
                    RunOutcome::HeartbeatLost { uptime } => {
                        let cfg = inner.server();
                        if uptime >= Duration::from_secs(cfg.healthy_after_secs) {
                            failures = 0;
                        }
                        failures = failures.saturating_add(1);
                        let wait = backoff(cfg.restart_min_secs, cfg.restart_max_secs, failures);
                        tracing::error!(
                            instance = %key, uptime_secs = uptime.as_secs(),
                            failures, wait_secs = wait.as_secs(),
                            "heartbeat lost; force-restarting after backoff"
                        );
                        set_state(
                            &inner,
                            InternalState::Backoff {
                                until: Instant::now() + wait,
                                failures,
                            },
                        )
                        .await;
                        if let Some(cmd) = wait_or_command(wait, &mut rx).await
                            && let Some(d) = apply_command_while_stopped(&inner, cmd).await
                        {
                            desired = d;
                        }
                    }
                    RunOutcome::SpawnFailed(reason) => {
                        tracing::error!(instance = %key, %reason, "failed to spawn");
                        set_state(&inner, InternalState::Failed { reason }).await;
                        match rx.recv().await {
                            Some(cmd) => {
                                if let Some(d) = apply_command_while_stopped(&inner, cmd).await {
                                    desired = d;
                                }
                            }
                            None => break,
                        }
                    }
                }
            }
        }
    }

    set_state(&inner, InternalState::Stopped).await;
    tracing::info!(instance = %key, "supervisor stopped");
}

async fn apply_command_while_stopped(inner: &Arc<Inner>, cmd: Command) -> Option<Desired> {
    match cmd {
        Command::Stop(rtx) => {
            record_admin(inner, "stop").await;
            let _ = rtx.send(());
            None
        }
        Command::Kill(rtx) => {
            record_admin(inner, "kill").await;
            let _ = rtx.send(());
            None
        }
        Command::Restart(rtx) => {
            record_admin(inner, "restart").await;
            let _ = rtx.send(());
            Some(Desired::Running)
        }
        Command::ForceRestart(rtx) => {
            record_admin(inner, "force_restart").await;
            let _ = rtx.send(());
            Some(Desired::Running)
        }
        Command::Update(rtx) => {
            record_admin(inner, "update").await;
            let outcome = handle_update_stopped(inner).await;
            let go_running = matches!(outcome, UpdateOutcome::Applied);
            let _ = rtx.send(outcome);
            if go_running {
                Some(Desired::Running)
            } else {
                None
            }
        }
        Command::Shutdown(rtx) => {
            record_admin(inner, "shutdown").await;
            let _ = rtx.send(());
            Some(Desired::Shutdown)
        }
    }
}

async fn record_admin(inner: &Arc<Inner>, action: &str) {
    db::incidents::record(
        &inner.db,
        &inner.key,
        db::incidents::Kind::Admin,
        Some(action),
    )
    .await;
}

// how long we let the server exit cleanly after POST /shutdown before SIGKILL
const SHUTDOWN_GRACE: Duration = Duration::from_secs(15);

// try POST /shutdown, if it fails or the server doesn't exit in time, SIGKILL.
async fn graceful_or_kill(
    inner: &Arc<Inner>,
    wait: &mut std::pin::Pin<&mut tokio::task::JoinHandle<std::io::Result<ExitStatus>>>,
    kill: &tokio_util::sync::CancellationToken,
    reason: &str,
) {
    if !send_shutdown_notification(inner, reason).await {
        tracing::warn!(instance = %inner.key, "graceful /shutdown failed; killing");
        kill.cancel();
        let _ = wait.as_mut().await;
        return;
    }
    tokio::select! {
        _ = wait.as_mut() => {}
        _ = tokio::time::sleep(SHUTDOWN_GRACE) => {
            tracing::warn!(instance = %inner.key, "graceful shutdown timed out; killing");
            kill.cancel();
            let _ = wait.as_mut().await;
        }
    }
}

async fn handle_update_running(inner: &Arc<Inner>) -> UpdateOutcome {
    let current = match current_version(inner).await {
        Ok(v) => v,
        Err(e) => return UpdateOutcome::Failed { reason: e },
    };
    let updater = inner.updater();
    if !updater.check_for_updates(current.clone()).await {
        return UpdateOutcome::NoUpdate;
    }
    inner.pending_update.store(true, Ordering::SeqCst);
    let sent = send_update_notification(inner).await;
    UpdateOutcome::Notified {
        notification_sent: sent,
    }
}

async fn handle_update_stopped(inner: &Arc<Inner>) -> UpdateOutcome {
    let current = match current_version(inner).await {
        Ok(v) => v,
        Err(e) => return UpdateOutcome::Failed { reason: e },
    };
    let updater = inner.updater();
    if !updater.check_for_updates(current.clone()).await {
        return UpdateOutcome::NoUpdate;
    }
    set_state(inner, InternalState::Updating).await;
    match run_update(inner).await {
        Ok(()) => UpdateOutcome::Applied,
        Err(e) => UpdateOutcome::Failed { reason: e },
    }
}

// header name and PascalCase body field are what ss14 expects
async fn send_shutdown_notification(inner: &Arc<Inner>, reason: &str) -> bool {
    let cfg = inner.server();
    let Some(port) = cfg.api.port else {
        tracing::debug!(instance = %inner.key, "no api.port; skipping graceful /shutdown");
        return false;
    };
    let url = format!("http://127.0.0.1:{port}/shutdown");
    let body = serde_json::json!({ "Reason": reason });
    match inner
        .game_client
        .post(&url)
        .header("WatchdogToken", cfg.api.token.clone())
        .json(&body)
        .send()
        .await
    {
        Ok(res) if res.status().is_success() => true,
        Ok(res) => {
            tracing::warn!(instance = %inner.key, status = %res.status(), "server rejected /shutdown");
            false
        }
        Err(e) => {
            tracing::debug!(instance = %inner.key, error = %e, "/shutdown POST failed");
            false
        }
    }
}

async fn send_update_notification(inner: &Arc<Inner>) -> bool {
    let cfg = inner.server();
    let Some(port) = cfg.api.port else {
        tracing::debug!(instance = %inner.key, "no api.port; skipping /update notification");
        return false;
    };
    let url = format!("http://127.0.0.1:{port}/update");
    match inner
        .game_client
        .post(&url)
        .header("WatchdogToken", cfg.api.token.clone())
        .send()
        .await
    {
        Ok(res) if res.status().is_success() => true,
        Ok(res) => {
            tracing::warn!(instance = %inner.key, status = %res.status(), "server rejected /update");
            false
        }
        Err(e) => {
            tracing::debug!(instance = %inner.key, error = %e, "/update POST failed");
            false
        }
    }
}

async fn wait_or_command(d: Duration, rx: &mut mpsc::Receiver<Command>) -> Option<Command> {
    tokio::select! {
        _ = sleep(d) => None,
        cmd = rx.recv() => cmd,
    }
}

fn backoff(min_secs: u64, max_secs: u64, failures: u32) -> Duration {
    let exp = 1u64
        .checked_shl(failures.saturating_sub(1).min(20))
        .unwrap_or(u64::MAX);
    let secs = min_secs.saturating_mul(exp).min(max_secs.max(min_secs));
    Duration::from_secs(secs.max(1))
}

async fn run_once(inner: &Arc<Inner>, rx: &mut mpsc::Receiver<Command>) -> RunOutcome {
    set_state(inner, InternalState::Starting).await;
    *inner.last_ping.lock().await = None;

    let version = match current_version(inner).await {
        Ok(Some(v)) => v,
        Ok(None) => return RunOutcome::SpawnFailed("no version installed".to_string()),
        Err(e) => return RunOutcome::SpawnFailed(format!("db error: {e}")),
    };

    let spec = build_spec(inner, &version);
    let handle = match inner.provider.spawn(spec).await {
        Ok(h) => h,
        Err(e) => {
            let reason = e.to_string();
            db::incidents::record(
                &inner.db,
                &inner.key,
                db::incidents::Kind::SpawnFailed,
                Some(&reason),
            )
            .await;
            return RunOutcome::SpawnFailed(reason);
        }
    };
    let started_at = Instant::now();
    let pid = handle.pid;
    set_state(
        inner,
        InternalState::Running {
            pid,
            started_at,
            version: version.clone(),
        },
    )
    .await;
    db::incidents::record(
        &inner.db,
        &inner.key,
        db::incidents::Kind::Start,
        Some(&format!("pid={pid} version={version}")),
    )
    .await;

    // snapshot heartbeat_timeout: a mid-run reload never shifts the goalposts on a running process
    let timeout_secs = inner.server().heartbeat_timeout;
    let mut ticker = tokio::time::interval(Duration::from_secs(5));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // stop token (SIGTERM) is intentionally unused - graceful path is HTTP + SIGKILL
    let InstanceHandle {
        wait,
        stop: _,
        kill,
        ..
    } = handle;
    tokio::pin!(wait);

    loop {
        tokio::select! {
            biased;
            cmd = rx.recv() => {
                let Some(cmd) = cmd else {
                    set_state(inner, InternalState::Stopping).await;
                    graceful_or_kill(inner, &mut wait, &kill, "Watchdog shutting down").await;
                    return RunOutcome::Shutdown;
                };
                match cmd {
                    Command::Stop(rtx) => {
                        record_admin(inner, "stop").await;
                        set_state(inner, InternalState::Stopping).await;
                        graceful_or_kill(inner, &mut wait, &kill, "Stop requested by operator").await;
                        let _ = rtx.send(());
                        return RunOutcome::StoppedByCommand;
                    }
                    Command::Kill(rtx) => {
                        record_admin(inner, "kill").await;
                        set_state(inner, InternalState::Stopping).await;
                        kill.cancel();
                        let _ = (&mut wait).await;
                        let _ = rtx.send(());
                        return RunOutcome::StoppedByCommand;
                    }
                    Command::Restart(rtx) => {
                        record_admin(inner, "restart").await;
                        set_state(inner, InternalState::Stopping).await;
                        graceful_or_kill(inner, &mut wait, &kill, "Restart requested by operator").await;
                        let _ = rtx.send(());
                        return RunOutcome::Restart;
                    }
                    Command::ForceRestart(rtx) => {
                        record_admin(inner, "force_restart").await;
                        set_state(inner, InternalState::Stopping).await;
                        kill.cancel();
                        let _ = (&mut wait).await;
                        let _ = rtx.send(());
                        return RunOutcome::ForceRestart;
                    }
                    Command::Update(rtx) => {
                        record_admin(inner, "update").await;
                        let outcome = handle_update_running(inner).await;
                        let _ = rtx.send(outcome);
                        continue;
                    }
                    Command::Shutdown(rtx) => {
                        record_admin(inner, "shutdown").await;
                        set_state(inner, InternalState::Stopping).await;
                        graceful_or_kill(inner, &mut wait, &kill, "Watchdog shutting down").await;
                        let _ = rtx.send(());
                        return RunOutcome::Shutdown;
                    }
                }
            }
            join = &mut wait => {
                let status = match join {
                    Ok(Ok(s)) => Some(s),
                    Ok(Err(e)) => {
                        tracing::error!(instance = %inner.key, error = %e, "instance io error");
                        None
                    }
                    Err(e) => {
                        tracing::error!(instance = %inner.key, error = %e, "supervisor join error");
                        None
                    }
                };
                let uptime = started_at.elapsed();
                let clean = matches!(&status, Some(s) if s.success());
                let detail = match &status {
                    Some(s) => format!("status={s} uptime_s={}", uptime.as_secs()),
                    None => format!("io_error uptime_s={}", uptime.as_secs()),
                };
                let kind = if clean { db::incidents::Kind::CleanExit } else { db::incidents::Kind::Crashed };
                db::incidents::record(&inner.db, &inner.key, kind, Some(&detail)).await;
                if clean {
                    return RunOutcome::CleanExit { uptime };
                }
                return RunOutcome::Crashed { uptime, status };
            }
            _ = ticker.tick() => {
                if timeout_secs == 0 { continue; }
                let last = *inner.last_ping.lock().await;
                // give the process a chance to boot before demanding a heartbeat
                let boot_grace = Duration::from_secs(timeout_secs.max(30));
                if started_at.elapsed() < boot_grace && last.is_none() {
                    continue;
                }
                let missed = match last {
                    Some(t) => t.elapsed() >= Duration::from_secs(timeout_secs),
                    None => started_at.elapsed() >= boot_grace,
                };
                if missed {
                    let uptime = started_at.elapsed();
                    db::incidents::record(
                        &inner.db,
                        &inner.key,
                        db::incidents::Kind::HeartbeatLost,
                        Some(&format!("uptime_s={}", uptime.as_secs())),
                    ).await;
                    set_state(inner, InternalState::Stopping).await;
                    kill.cancel();
                    let _ = (&mut wait).await;
                    return RunOutcome::HeartbeatLost { uptime };
                }
            }
        }
    }
}

async fn set_state(inner: &Arc<Inner>, s: InternalState) {
    *inner.state.lock().await = s;
}

async fn current_version(inner: &Arc<Inner>) -> Result<Option<String>, String> {
    match db::server::find_by_key(&inner.db, &inner.key).await {
        Ok(row) => Ok(row.and_then(|s| s.version)),
        Err(e) => Err(e.to_string()),
    }
}

async fn ensure_version(inner: &Arc<Inner>) -> Result<(), String> {
    match current_version(inner).await? {
        Some(_) => Ok(()),
        None => run_update(inner).await,
    }
}

async fn run_update(inner: &Arc<Inner>) -> Result<(), String> {
    let current = current_version(inner).await?;
    let path = instance_bin_dir(&inner.key);
    if let Some(parent) = path.parent()
        && let Err(e) = tokio::fs::create_dir_all(parent).await
    {
        return Err(format!("create instance dir: {e}"));
    }
    match inner.updater().run_update(current.clone(), path).await {
        Some(new_version) => {
            let db = &inner.db;
            let res = if current.is_some() {
                db::server::update_version(db, &inner.key, &new_version).await
            } else {
                db::server::create(db, &inner.key, &new_version)
                    .await
                    .map(|_| ())
            };
            res.map_err(|e| e.to_string())
        }
        None => {
            if current.is_none() {
                Err("no version installed and update produced none".to_string())
            } else {
                Ok(())
            }
        }
    }
}

fn build_spec(inner: &Arc<Inner>, version: &str) -> InstanceSpec {
    let cfg = inner.server();
    // watchdog.token is sensitive so goes via env (ROBUST_CVAR_ maps __ -> .);
    // key and baseUrl are safe on the command line
    let mut env: HashMap<String, String> = HashMap::new();
    env.insert(
        "ROBUST_CVAR_watchdog__token".to_string(),
        cfg.api.token.clone(),
    );
    for (k, v) in &cfg.environment {
        env.insert(k.clone(), v.clone());
    }

    let mut args: Vec<String> = vec![
        "--cvar".into(),
        format!("watchdog.key={}", inner.key),
        "--cvar".into(),
        format!("watchdog.baseUrl={}", inner.admin_base_url),
        "--config-file".into(),
        "config.toml".into(),
        "--data-dir".into(),
        "data".into(),
    ];
    args.extend(cfg.arguments.iter().cloned());

    let program = PathBuf::from(cfg.run_command.as_deref().unwrap_or("bin/Robust.Server"));

    InstanceSpec {
        id: inner.key.clone(),
        version: version.to_string(),
        cwd: instance_dir(&inner.key),
        program,
        args,
        env,
    }
}

pub const INSTANCE_ROOT: &str = "./instances";

pub fn instance_bin_dir(key: &str) -> PathBuf {
    instance_dir(key).join("bin")
}

pub fn instance_dir(key: &str) -> PathBuf {
    PathBuf::from(INSTANCE_ROOT).join(key)
}

pub struct Watchdog {
    supervisors: HashMap<String, Supervisor>,
    silenced: SilenceRegistry,
    // kept alive here so it outlives supervisors during shutdown
    #[allow(dead_code)]
    log_bus: LogBus,
    config_path: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReloadReport {
    pub applied: Vec<String>,
    pub added_ignored: Vec<String>,
    pub removed_ignored: Vec<String>,
    pub silenced: Vec<String>,
}

impl Watchdog {
    pub fn new(
        cfg: SharedWatchdogConfig,
        db: SqlitePool,
        admin_base_url: String,
        config_path: PathBuf,
    ) -> Self {
        let silenced: SilenceRegistry = Arc::new(StdRwLock::new(HashSet::new()));
        let log_bus: LogBus = Arc::new(StdRwLock::new(HashMap::new()));
        {
            let mut set = silenced.write().expect("silence registry poisoned");
            let mut buses = log_bus.write().expect("log bus poisoned");
            for (key, server) in &cfg.servers {
                if server.silent {
                    set.insert(key.clone());
                }
                buses.insert(key.clone(), new_log_broadcaster());
            }
        }

        let mut supervisors = HashMap::new();
        for (key, server) in &cfg.servers {
            let sup = Supervisor::spawn(
                key.clone(),
                Arc::new(clone_server_cfg(server)),
                db.clone(),
                admin_base_url.clone(),
                silenced.clone(),
                log_bus.clone(),
            );
            supervisors.insert(key.clone(), sup);
        }
        Self {
            supervisors,
            silenced,
            log_bus,
            config_path,
        }
    }

    pub fn get(&self, key: &str) -> Option<&Supervisor> {
        self.supervisors.get(key)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Supervisor)> {
        self.supervisors.iter()
    }

    pub fn silenced_keys(&self) -> Vec<String> {
        self.silenced
            .read()
            .map(|s| {
                let mut v: Vec<String> = s.iter().cloned().collect();
                v.sort();
                v
            })
            .unwrap_or_default()
    }

    pub fn reload(&self) -> Result<ReloadReport, String> {
        let raw = std::fs::read_to_string(&self.config_path)
            .map_err(|e| format!("read {}: {e}", self.config_path.display()))?;
        let new_cfg: WatchdogConfig = toml::from_str(&raw).map_err(|e| e.to_string())?;
        Ok(self.reload_from(&new_cfg))
    }

    pub fn reload_from(&self, new_cfg: &WatchdogConfig) -> ReloadReport {
        let mut applied = Vec::new();
        let mut added_ignored = Vec::new();
        let mut removed_ignored = Vec::new();

        // config is source of truth for silence; runtime toggles are wiped by reload
        {
            let mut set = self.silenced.write().expect("silence registry poisoned");
            set.clear();
            for (key, server) in &new_cfg.servers {
                if server.silent {
                    set.insert(key.clone());
                }
            }
        }

        for (key, server) in &new_cfg.servers {
            match self.supervisors.get(key) {
                Some(sup) => {
                    sup.apply_config(clone_server_cfg(server));
                    applied.push(key.clone());
                }
                None => added_ignored.push(key.clone()),
            }
        }
        for key in self.supervisors.keys() {
            if !new_cfg.servers.contains_key(key) {
                removed_ignored.push(key.clone());
            }
        }

        applied.sort();
        added_ignored.sort();
        removed_ignored.sort();

        if !added_ignored.is_empty() {
            tracing::warn!(?added_ignored, "reload: new servers require daemon restart");
        }
        if !removed_ignored.is_empty() {
            tracing::warn!(
                ?removed_ignored,
                "reload: removed servers still running until daemon restart"
            );
        }
        tracing::info!(?applied, "reload applied");

        ReloadReport {
            applied,
            added_ignored,
            removed_ignored,
            silenced: self.silenced_keys(),
        }
    }

    pub async fn shutdown_all(&self) {
        let handles: Vec<JoinHandle<()>> = self
            .supervisors
            .values()
            .map(|s| {
                let s = s.clone();
                tokio::spawn(async move {
                    if let Err(e) = s.shutdown().await {
                        tracing::warn!(key = %s.key(), error = %e, "supervisor shutdown");
                    }
                })
            })
            .collect();
        for h in handles {
            let _ = h.await;
        }
    }
}

impl AsRef<Path> for Watchdog {
    fn as_ref(&self) -> &Path {
        &self.config_path
    }
}

// ServerConfig lacks Clone because of nested non-Clone fields, so we do it by hand
fn clone_server_cfg(s: &ServerConfig) -> ServerConfig {
    use crate::config::{ApiConfig, ManifestUpdateConfig, UpdateConfig, UpdateType};
    ServerConfig {
        name: s.name.clone(),
        api: ApiConfig {
            token: s.api.token.clone(),
            port: s.api.port,
        },
        update: UpdateConfig {
            update_type: match &s.update.update_type {
                UpdateType::Manifest(m) => UpdateType::Manifest(ManifestUpdateConfig {
                    manifest_url: m.manifest_url.clone(),
                    authentication: m.authentication.clone(),
                }),
            },
        },
        run_command: s.run_command.clone(),
        arguments: s.arguments.clone(),
        environment: s.environment.clone(),
        heartbeat_timeout: s.heartbeat_timeout,
        restart_min_secs: s.restart_min_secs,
        restart_max_secs: s.restart_max_secs,
        healthy_after_secs: s.healthy_after_secs,
        silent: s.silent,
    }
}
