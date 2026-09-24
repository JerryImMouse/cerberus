use std::{collections::HashMap, io, path::PathBuf, process::ExitStatus};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

mod process;
pub use process::{LogBus, ProcessInstanceProvider, SilenceRegistry, new_log_broadcaster};

mod error;
pub use error::{IResult, InstanceError};

#[derive(Debug, Clone)]
pub struct InstanceSpec {
    pub id: String,
    pub version: String,
    pub cwd: PathBuf,
    pub program: PathBuf,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
}

pub struct InstanceHandle {
    pub id: String,
    pub version: String,
    pub pid: u32,
    pub wait: JoinHandle<io::Result<ExitStatus>>,
    pub stop: CancellationToken,
    pub kill: CancellationToken,
}

#[async_trait::async_trait]
pub trait InstanceProvider: Send + Sync {
    async fn spawn(&self, spec: InstanceSpec) -> IResult<InstanceHandle>;
}
