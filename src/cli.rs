use std::path::{Path, PathBuf};

use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::Shell;
use reqwest::header::AUTHORIZATION;
use serde::Deserialize;
use serde_json::Value;

use crate::config;

#[derive(Parser)]
#[command(
    name = "cerberus",
    version,
    about = "Cerberus \u{2014} SS14 server watchdog + control CLI"
)]
pub struct Cli {
    #[arg(short, long, default_value = config::DEFAULT_PATH, global = true)]
    pub config: String,

    #[arg(long, global = true)]
    pub api: Option<String>,

    #[arg(long, global = true)]
    pub token: Option<String>,

    #[arg(long, global = true)]
    pub cli_config: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Subcommand)]
pub enum Command {
    #[cfg(feature = "daemon")]
    Daemon,
    List,
    Status { key: String },
    Start { key: String },
    Stop { key: String },
    Restart { key: String },
    ForceRestart { key: String },
    Update { key: String },
    Reload,
    Silence { key: String },
    Unsilence { key: String },
    History {
        key: String,
        #[arg(short = 'n', long, default_value_t = 50)]
        limit: i64,
    },
    Logs {
        key: String,
        #[arg(short = 'f', long, default_value_t = true)]
        follow: bool,
    },
    Completions {
        shell: Shell,
    },
}

impl Cli {
    pub async fn dispatch(mut self) -> Result<(), Box<dyn std::error::Error>> {
        let cmd = self.command.take().unwrap_or_else(default_command);

        #[cfg(feature = "daemon")]
        if matches!(cmd, Command::Daemon) {
            let cfg = config::from_file(&self.config)?;
            let path = PathBuf::from(&self.config);
            return crate::daemon(cfg, path).await;
        }

        if let Command::Completions { shell } = cmd {
            let mut cmd = Cli::command();
            let name = cmd.get_name().to_string();
            clap_complete::generate(shell, &mut cmd, name, &mut std::io::stdout());
            return Ok(());
        }

        // Everything else is an HTTP client.
        let client = self.resolve_client()?;
        match cmd {
            #[cfg(feature = "daemon")]
            Command::Daemon => unreachable!("handled above"),
            Command::List => {
                let body: Value = client
                    .request(reqwest::Method::GET, "/instances", None)
                    .await?;
                println!("{}", serde_json::to_string_pretty(&body)?);
            }
            Command::Status { key } => {
                let body: Value = client
                    .request(
                        reqwest::Method::GET,
                        &format!("/instances/{key}/status"),
                        None,
                    )
                    .await?;
                println!("{}", serde_json::to_string_pretty(&body)?);
            }
            Command::Start { key } => client.post_ok(&format!("/instances/{key}/start")).await?,
            Command::Stop { key } => client.post_ok(&format!("/instances/{key}/stop")).await?,
            Command::Restart { key } => {
                client.post_ok(&format!("/instances/{key}/restart")).await?
            }
            Command::ForceRestart { key } => {
                client
                    .post_ok(&format!("/instances/{key}/force-restart"))
                    .await?
            }
            Command::Update { key } => {
                let body: Value = client
                    .request(
                        reqwest::Method::POST,
                        &format!("/instances/{key}/update"),
                        None,
                    )
                    .await?;
                print_update_outcome(&body);
            }
            Command::Reload => {
                let body: Value = client
                    .request(reqwest::Method::POST, "/reload", None)
                    .await?;
                println!("{}", serde_json::to_string_pretty(&body)?);
            }
            Command::Silence { key } => {
                client.post_ok(&format!("/instances/{key}/silence")).await?
            }
            Command::Unsilence { key } => {
                client
                    .post_ok(&format!("/instances/{key}/unsilence"))
                    .await?
            }
            Command::History { key, limit } => {
                let path = format!("/instances/{key}/history?limit={limit}");
                let body: Value = client.request(reqwest::Method::GET, &path, None).await?;
                print_history(&body);
            }
            Command::Logs { key, follow: _ } => {
                client.stream_sse(&format!("/instances/{key}/logs")).await?;
            }
            Command::Completions { .. } => {
                unreachable!("handled above the client-resolution step");
            }
        }
        Ok(())
    }

    fn resolve_client(&self) -> Result<HttpClient, Box<dyn std::error::Error>> {
        let user_cli = load_user_cli_config(self.cli_config.as_deref())?;
        let daemon = load_daemon_config_lenient(&self.config);

        let base = self
            .api
            .clone()
            .or_else(|| user_cli.api.clone())
            .or_else(|| {
                daemon
                    .as_ref()
                    .map(|c| default_base_from_bind(&c.admin.bind))
            })
            .ok_or_else(|| {
                "no admin URL; pass --api, or set `api = ...` in the CLI \
                 config, or provide a daemon config with [admin].bind"
                    .to_string()
            })?;
        let token = self
            .token
            .clone()
            .or_else(|| user_cli.token.clone())
            .or_else(|| daemon.as_ref().and_then(|c| c.admin.token.clone()))
            .ok_or_else(|| {
                "no admin token; pass --token, or set `token = ...` in the CLI \
                 config, or set [admin].token in the daemon config"
                    .to_string()
            })?;
        Ok(HttpClient {
            base,
            token,
            client: reqwest::Client::new(),
        })
    }
}

fn print_update_outcome(v: &Value) {
    let outcome = v.get("outcome").and_then(|v| v.as_str()).unwrap_or("");
    match outcome {
        "no_update" => println!("no update available"),
        "notified" => {
            let sent = v
                .get("notification_sent")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if sent {
                println!("update queued; server notified, will apply on next natural exit");
            } else {
                println!(
                    "update queued; server notification could NOT be delivered - the update \
                     will apply the next time the server exits for any reason"
                );
            }
        }
        "applied" => println!("update installed; will start on next spawn"),
        "failed" => {
            let reason = v
                .get("reason")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            println!("update failed: {reason}");
        }
        _ => println!("{}", serde_json::to_string_pretty(v).unwrap_or_default()),
    }
}

fn print_history(v: &Value) {
    let Some(rows) = v.as_array() else {
        println!("{}", serde_json::to_string_pretty(v).unwrap_or_default());
        return;
    };
    if rows.is_empty() {
        println!("(no history)");
        return;
    }
    println!("{:<20} {:<16} DETAIL", "TIMESTAMP", "KIND");
    for row in rows {
        let ts = row.get("ts").and_then(|v| v.as_i64()).unwrap_or(0);
        let kind = row.get("kind").and_then(|v| v.as_str()).unwrap_or("?");
        let detail = row.get("detail").and_then(|v| v.as_str()).unwrap_or("");
        println!("{:<20} {:<16} {}", format_ts(ts), kind, detail);
    }
}

fn format_ts(ts: i64) -> String {
    let secs = ts.max(0) as u64;
    let days = (secs / 86_400) as i64;
    let (h, m, s) = ((secs / 3600) % 24, (secs / 60) % 60, secs % 60);
    let (y, mo, d) = days_to_ymd(days);
    format!("{y:04}-{mo:02}-{d:02} {h:02}:{m:02}:{s:02}")
}

fn days_to_ymd(mut days: i64) -> (i32, u32, u32) {
    days += 719_468;
    let era = days.div_euclid(146_097);
    let doe = days.rem_euclid(146_097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i32 + (era * 400) as i32;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    (year, m, d)
}

#[cfg(feature = "daemon")]
fn default_command() -> Command {
    Command::Daemon
}

#[cfg(not(feature = "daemon"))]
fn default_command() -> Command {
    Command::List
}

fn default_base_from_bind(bind: &str) -> String {
    let (host, port) = bind.rsplit_once(':').unwrap_or(("127.0.0.1", "5000"));
    let host = if host == "0.0.0.0" || host == "[::]" || host.is_empty() {
        "127.0.0.1"
    } else {
        host
    };
    format!("http://{host}:{port}")
}

#[derive(Deserialize, Default, Debug)]
struct UserCliConfig {
    api: Option<String>,
    token: Option<String>,
}

fn load_user_cli_config(
    override_path: Option<&Path>,
) -> Result<UserCliConfig, Box<dyn std::error::Error>> {
    let path = match override_path {
        Some(p) => p.to_path_buf(),
        None => match default_cli_config_path() {
            Some(p) => p,
            None => return Ok(UserCliConfig::default()),
        },
    };
    match std::fs::read_to_string(&path) {
        Ok(raw) => Ok(toml::from_str(&raw).map_err(|e| format!("{}: {e}", path.display()))?),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(UserCliConfig::default()),
        Err(e) => Err(format!("{}: {e}", path.display()).into()),
    }
}

fn default_cli_config_path() -> Option<PathBuf> {
    if let Ok(xdg) = std::env::var("XDG_CONFIG_HOME")
        && !xdg.is_empty()
    {
        return Some(PathBuf::from(xdg).join("cerberus/cli.toml"));
    }
    let home = std::env::var("HOME").ok()?;
    Some(PathBuf::from(home).join(".config/cerberus/cli.toml"))
}

fn load_daemon_config_lenient(path: &str) -> Option<config::SharedWatchdogConfig> {
    config::from_file(path).ok()
}

struct HttpClient {
    base: String,
    token: String,
    client: reqwest::Client,
}

impl HttpClient {
    async fn request(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, Box<dyn std::error::Error>> {
        let url = format!("{}{}", self.base.trim_end_matches('/'), path);
        let mut req = self
            .client
            .request(method, &url)
            .header(AUTHORIZATION, format!("Bearer {}", self.token));
        if let Some(b) = body {
            req = req.json(&b);
        }
        let res = req.send().await?;
        let status = res.status();
        let text = res.text().await?;
        if !status.is_success() {
            return Err(format!("{status}: {text}").into());
        }
        if text.is_empty() {
            return Ok(Value::Null);
        }
        Ok(serde_json::from_str(&text).unwrap_or(Value::String(text)))
    }

    async fn post_ok(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let _ = self.request(reqwest::Method::POST, path, None).await?;
        println!("ok");
        Ok(())
    }

    async fn stream_sse(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let url = format!("{}{}", self.base.trim_end_matches('/'), path);
        let res = self
            .client
            .get(&url)
            .header(AUTHORIZATION, format!("Bearer {}", self.token))
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .send()
            .await?;
        let status = res.status();
        if !status.is_success() {
            let text = res.text().await.unwrap_or_default();
            return Err(format!("{status}: {text}").into());
        }
        let mut buf: Vec<u8> = Vec::new();
        let mut res = res;
        while let Some(chunk) = res.chunk().await? {
            buf.extend_from_slice(&chunk);
            while let Some(pos) = find_event_break(&buf) {
                let event: Vec<u8> = buf.drain(..pos + 2).collect();
                for line in event.split(|&b| b == b'\n') {
                    let line = strip_cr(line);
                    if let Some(rest) = line
                        .strip_prefix(b"data: ")
                        .or_else(|| line.strip_prefix(b"data:"))
                        && let Ok(s) = std::str::from_utf8(rest)
                    {
                        println!("{s}");
                    }
                }
            }
        }
        Ok(())
    }
}

fn find_event_break(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\n\n")
}

fn strip_cr(line: &[u8]) -> &[u8] {
    match line.split_last() {
        Some((&b'\r', head)) => head,
        _ => line,
    }
}
