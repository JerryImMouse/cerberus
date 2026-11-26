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
fn default_autostart() -> bool {
    true
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

    #[serde(default, deserialize_with = "deserialize_cpus")]
    pub cpus: Option<Vec<usize>>,

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

    #[serde(default = "default_autostart")]
    pub autostart: bool,
}

// CPU_SETSIZE in glibc and musl
const MAX_CPUS: usize = 1024;

fn deserialize_cpus<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Option<Vec<usize>>, D::Error> {
    let raw = String::deserialize(d)?;
    parse_cpu_list(&raw)
        .map(Some)
        .map_err(serde::de::Error::custom)
}

fn parse_cpu_list(raw: &str) -> Result<Vec<usize>, String> {
    let mut cpus = Vec::new();
    for part in raw.split(',') {
        let part = part.trim();
        let (lo, hi) = part.split_once('-').unwrap_or((part, part));
        let parse = |s: &str| {
            s.trim()
                .parse::<usize>()
                .map_err(|_| format!("bad cpu list {raw:?}: {part:?} is not a cpu or a range"))
        };
        let (lo, hi) = (parse(lo)?, parse(hi)?);
        if lo > hi {
            return Err(format!("bad cpu list {raw:?}: range {part:?} is reversed"));
        }
        if hi >= MAX_CPUS {
            return Err(format!("bad cpu list {raw:?}: cpu {hi} is out of range"));
        }
        cpus.extend(lo..=hi);
    }
    Ok(cpus)
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
    #[serde(default)]
    pub authentication: Option<ManifestAuth>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ManifestAuth {
    pub username: String,
    pub password: String,
}

#[cfg(test)]
mod test {
    use super::*;
    const CONFIG_SAMPLE: &str = include_str!("../../cerberus.toml");

    #[test]
    fn config_deserializable() {
        toml::from_str::<WatchdogConfig>(CONFIG_SAMPLE).unwrap();
    }

    #[test]
    fn autostart_defaults_to_true() {
        assert!(server_with("").unwrap().autostart);
    }

    #[test]
    fn autostart_can_be_disabled() {
        assert!(!server_with("autostart = false").unwrap().autostart);
    }

    fn cpu_list_parses_ranges_and_singles() {
        assert_eq!(parse_cpu_list("0-2,16-18").unwrap(), [0, 1, 2, 16, 17, 18]);
        assert_eq!(parse_cpu_list("3").unwrap(), [3]);
        assert_eq!(parse_cpu_list(" 1 , 4-5 ").unwrap(), [1, 4, 5]);
    }

    #[test]
    fn cpu_list_rejects_garbage() {
        for raw in ["", "1,", "a", "1-", "-1", "3-1", "0-2,x", "1024", "1-2-3"] {
            assert!(parse_cpu_list(raw).is_err(), "{raw:?} should be rejected");
        }
    }

    #[test]
    fn cpus_is_optional() {
        assert_eq!(server_with("").unwrap().cpus, None);
        assert_eq!(
            server_with("cpus = \"0-1,8\"").unwrap().cpus,
            Some(vec![0, 1, 8])
        );
    }

    #[test]
    fn bad_cpus_fails_config_load() {
        let err = server_with("cpus = \"4-2\"").unwrap_err();
        assert!(err.to_string().contains("reversed"), "{err}");
    }

    fn server_with(extra: &str) -> Result<ServerConfig, toml::de::Error> {
        toml::from_str(&format!(
            "name = \"x\"\napi.token = \"t\"\nupdate.type = \"manifest\"\n\
             update.manifest_url = \"https://example.com/manifest\"\n{extra}"
        ))
    }
}
