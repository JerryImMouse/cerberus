use std::collections::HashMap;

use serde::Deserialize;
use url::Url;

mod error;
mod utils;
pub use error::{CResult, ConfigError};
pub use utils::{DEFAULT_PATH, from_file};

pub type SharedWatchdogConfig = std::sync::Arc<WatchdogConfig>;

#[derive(Debug, Deserialize)]
pub struct WatchdogConfig {
    pub database: DatabaseConfig,
    pub logging: LoggingConfig,
    #[serde(default)]
    pub admin: AdminConfig,
    pub servers: HashMap<String, ServerConfig>,
}

#[derive(Debug, Deserialize)]
pub struct AdminConfig {
    #[serde(default = "default_admin_bind")]
    pub bind: String,
    #[serde(default)]
    pub token: Option<String>,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            bind: default_admin_bind(),
            token: None,
        }
    }
}

fn default_admin_bind() -> String {
    "0.0.0.0:5000".to_string()
}

fn default_heartbeat_timeout() -> u64 {
    60
}
fn default_restart_min_secs() -> u64 {
    1
}
fn default_restart_max_secs() -> u64 {
    60
}
fn default_healthy_after_secs() -> u64 {
    30
}

#[derive(Debug, Deserialize)]
pub struct DatabaseConfig {
    pub file: String,
}

#[derive(Debug, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
    pub overrides: HashMap<String, String>,

    pub console: Option<ConsoleLoggingConfig>,
    pub loki: Option<LokiLoggingConfig>,
}

#[derive(Debug, Deserialize)]
pub struct ConsoleLoggingConfig {
    pub enabled: bool,
    pub ansi: bool,
    pub compact: bool,
}

#[derive(Debug, Deserialize)]
pub struct LokiLoggingConfig {
    pub enabled: bool,
    pub endpoint: Url,

    pub labels: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    pub name: String,
    pub api: ApiConfig,
    pub update: UpdateConfig,

    #[serde(default)]
    pub run_command: Option<String>,

    #[serde(default)]
    pub arguments: Vec<String>,

    #[serde(default)]
    pub environment: HashMap<String, String>,

    #[serde(default = "default_heartbeat_timeout")]
    pub heartbeat_timeout: u64,

    #[serde(default = "default_restart_min_secs")]
    pub restart_min_secs: u64,
    #[serde(default = "default_restart_max_secs")]
    pub restart_max_secs: u64,

    #[serde(default = "default_healthy_after_secs")]
    pub healthy_after_secs: u64,

    #[serde(default)]
    pub silent: bool,
}

#[derive(Debug, Deserialize)]
pub struct ApiConfig {
    pub token: String,
    #[serde(default)]
    pub port: Option<u16>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateConfig {
    #[serde(flatten)]
    pub update_type: UpdateType,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum UpdateType {
    Manifest(ManifestUpdateConfig),
}

#[derive(Debug, Deserialize)]
pub struct ManifestUpdateConfig {
    pub manifest_url: Url,
}

#[cfg(test)]
mod test {
    use super::*;
    const CONFIG_SAMPLE: &str = include_str!("../../cerberus.toml");

    #[test]
    fn config_deserializable() {
        toml::from_str::<WatchdogConfig>(CONFIG_SAMPLE).unwrap();
    }
}
