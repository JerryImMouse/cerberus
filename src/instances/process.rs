use std::{
    collections::{HashMap, HashSet},
    io,
    path::Path,
    process::{ExitStatus, Stdio},
    sync::{Arc, RwLock},
    time::Duration,
};

use tokio::{
    io::{AsyncBufReadExt, AsyncRead, BufReader},
    process::{Child, Command},
    sync::broadcast,
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use super::{IResult, InstanceHandle, InstanceProvider, InstanceSpec};

const STOP_GRACE: Duration = Duration::from_secs(30);

const LOG_BROADCAST_CAP: usize = 256;

pub type SilenceRegistry = Arc<RwLock<HashSet<String>>>;
pub type LogBus = Arc<RwLock<HashMap<String, broadcast::Sender<String>>>>;

pub fn new_log_broadcaster() -> broadcast::Sender<String> {
    let (tx, _rx) = broadcast::channel(LOG_BROADCAST_CAP);
    tx
}

pub struct ProcessInstanceProvider {
    silenced: SilenceRegistry,
    log_bus: LogBus,
}

impl ProcessInstanceProvider {
    pub fn new(silenced: SilenceRegistry, log_bus: LogBus) -> Self {
        Self { silenced, log_bus }
    }
}

#[async_trait::async_trait]
impl InstanceProvider for ProcessInstanceProvider {
    async fn spawn(&self, spec: InstanceSpec) -> IResult<InstanceHandle> {
        tokio::fs::create_dir_all(&spec.cwd).await?;
        let cwd = tokio::fs::canonicalize(&spec.cwd).await?;
        let program = if spec.program.is_absolute() {
            spec.program.clone()
        } else {
            cwd.join(&spec.program)
        };
        make_executable(&program).await?;

        let mut child = Command::new(&program)
            .args(&spec.args)
            .envs(&spec.env)
            .current_dir(&cwd)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            // don't orphan servers if the supervisor task drops without stopping.
            .kill_on_drop(true)
            .spawn()?;

        let pid = child.id().unwrap_or_default();
        let id = spec.id.clone();
        tracing::info!(
            instance = %id, version = %spec.version, pid,
            program = %program.display(), "instance started"
        );

        if let Some(out) = child.stdout.take() {
            tokio::spawn(pump(
                id.clone(),
                out,
                false,
                self.silenced.clone(),
                self.log_bus.clone(),
            ));
        }
        if let Some(err) = child.stderr.take() {
            tokio::spawn(pump(
                id.clone(),
                err,
                true,
                self.silenced.clone(),
                self.log_bus.clone(),
            ));
        }

        let stop = CancellationToken::new();
        let kill = CancellationToken::new();
        let stop_c = stop.clone();
        let kill_c = kill.clone();
        let id_c = id.clone();
        let wait: JoinHandle<io::Result<ExitStatus>> = tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = kill_c.cancelled() => {
                    tracing::warn!(instance = %id_c, "force kill requested");
                    child.kill().await?;
                    child.wait().await
                }
                _ = stop_c.cancelled() => terminate(&id_c, &mut child, pid, STOP_GRACE).await,
                status = child.wait() => {
                    match &status {
                        Ok(s) => tracing::info!(instance = %id_c, status = %s, "instance exited on its own"),
                        Err(e) => tracing::error!(instance = %id_c, error = %e, "waiting on instance failed"),
                    }
                    status
                }
            }
        });

        Ok(InstanceHandle {
            id: spec.id,
            version: spec.version,
            pid,
            wait,
            stop,
            kill,
        })
    }
}

async fn terminate(
    id: &str,
    child: &mut Child,
    pid: u32,
    grace: Duration,
) -> io::Result<ExitStatus> {
    #[cfg(unix)]
    if let Ok(pid) = i32::try_from(pid) {
        use nix::{
            sys::signal::{Signal, kill},
            unistd::Pid,
        };
        let _ = kill(Pid::from_raw(pid), Signal::SIGTERM);
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        let _ = child.start_kill();
    }

    match tokio::time::timeout(grace, child.wait()).await {
        Ok(status) => status,
        Err(_) => {
            tracing::warn!(instance = %id, ?grace, "grace period expired, killing");
            child.kill().await?;
            child.wait().await
        }
    }
}

async fn pump<R: AsyncRead + Unpin>(
    id: String,
    reader: R,
    is_stderr: bool,
    silenced: SilenceRegistry,
    log_bus: LogBus,
) {
    let mut lines = BufReader::new(reader).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        if let Ok(guard) = log_bus.read()
            && let Some(tx) = guard.get(&id)
        {
            let _ = tx.send(line.clone());
        }
        if silenced.read().map(|s| s.contains(&id)).unwrap_or(false) {
            continue;
        }
        if is_stderr {
            tracing::warn!(target: "instance", instance = %id, "{line}");
        } else {
            tracing::info!(target: "instance", instance = %id, "{line}");
        }
    }
}

#[cfg(unix)]
async fn make_executable(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut perms = tokio::fs::metadata(path).await?.permissions();
    perms.set_mode(perms.mode() | 0o111);
    tokio::fs::set_permissions(path, perms).await
}

#[cfg(not(unix))]
async fn make_executable(_path: &Path) -> io::Result<()> {
    Ok(())
}
