use std::path::Path;

use super::{CResult, SharedWatchdogConfig, WatchdogConfig};

pub const DEFAULT_PATH: &str = "./cerberus.toml"; // look in a cwd.

pub fn from_file<P: AsRef<Path>>(path: P) -> CResult<SharedWatchdogConfig> {
    let data = std::fs::read_to_string(path)?;
    Ok(std::sync::Arc::new(toml::from_str::<WatchdogConfig>(
        &data,
    )?))
}
