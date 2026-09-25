use std::collections::BTreeMap;
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

    // one-shot override of the endpoint (api+token). ignores CLI config.
    #[arg(long, global = true)]
    pub api: Option<String>,

    #[arg(long, global = true)]
    pub token: Option<String>,

    // pick a named daemon from the CLI config
    #[arg(short = 'd', long, global = true)]
    pub daemon: Option<String>,

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
    Status {
        target: String,
    },
    Start {
        target: String,
    },
    Stop {
        target: String,
    },
    Restart {
        target: String,
    },
    ForceRestart {
        target: String,
    },
    Update {
        target: String,
    },
    // reload is a daemon-level op, not per-instance
    Reload,
    Silence {
        target: String,
    },
    Unsilence {
        target: String,
    },
    History {
        target: String,
        #[arg(short = 'n', long, default_value_t = 50)]
        limit: i64,
    },
    Logs {
        target: String,
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

        let ctx = self.resolve_daemons()?;
        match cmd {
            #[cfg(feature = "daemon")]
            Command::Daemon => unreachable!("handled above"),
            Command::Completions { .. } => unreachable!("handled above"),

            Command::List => list_all(&ctx).await?,
            Command::Reload => {
                let (name, client) = ctx.pick_single()?;
                let body: Value = client
                    .request(reqwest::Method::POST, "/reload", None)
                    .await?;
                if ctx.has_multiple() {
                    println!("[{name}]");
                }
                println!("{}", serde_json::to_string_pretty(&body)?);
            }

            Command::Status { target } => {
                let (client, key) = ctx.resolve(&target)?;
                let body: Value = client
                    .request(
                        reqwest::Method::GET,
                        &format!("/instances/{key}/status"),
                        None,
                    )
                    .await?;
                println!("{}", serde_json::to_string_pretty(&body)?);
            }
            Command::Start { target } => {
                let (client, key) = ctx.resolve(&target)?;
                client.post_ok(&format!("/instances/{key}/start")).await?
            }
            Command::Stop { target } => {
                let (client, key) = ctx.resolve(&target)?;
                client.post_ok(&format!("/instances/{key}/stop")).await?
            }
            Command::Restart { target } => {
                let (client, key) = ctx.resolve(&target)?;
                client.post_ok(&format!("/instances/{key}/restart")).await?
            }
            Command::ForceRestart { target } => {
                let (client, key) = ctx.resolve(&target)?;
                client
                    .post_ok(&format!("/instances/{key}/force-restart"))
                    .await?
            }
            Command::Update { target } => {
                let (client, key) = ctx.resolve(&target)?;
                let body: Value = client
                    .request(
                        reqwest::Method::POST,
                        &format!("/instances/{key}/update"),
                        None,
                    )
                    .await?;
                print_update_outcome(&body);
            }
            Command::Silence { target } => {
                let (client, key) = ctx.resolve(&target)?;
                client.post_ok(&format!("/instances/{key}/silence")).await?
            }
            Command::Unsilence { target } => {
                let (client, key) = ctx.resolve(&target)?;
                client
                    .post_ok(&format!("/instances/{key}/unsilence"))
                    .await?
            }
            Command::History { target, limit } => {
                let (client, key) = ctx.resolve(&target)?;
                let path = format!("/instances/{key}/history?limit={limit}");
                let body: Value = client.request(reqwest::Method::GET, &path, None).await?;
                print_history(&body);
            }
            Command::Logs { target, follow: _ } => {
                let (client, key) = ctx.resolve(&target)?;
                client.stream_sse(&format!("/instances/{key}/logs")).await?;
            }
        }
        Ok(())
    }

    fn resolve_daemons(&self) -> Result<DaemonCtx, Box<dyn std::error::Error>> {
        // explicit --api/--token wins; everything else is one adhoc daemon
        if let (Some(api), Some(token)) = (self.api.as_deref(), self.token.as_deref()) {
            let mut daemons = BTreeMap::new();
            daemons.insert(
                "default".to_string(),
                HttpClient::new(api.to_string(), token.to_string()),
            );
            return Ok(DaemonCtx {
                daemons,
                selected: Some("default".to_string()),
                restricted: true,
            });
        }

        let user_cli = load_user_cli_config(self.cli_config.as_deref())?;
        let daemon_cfg = load_daemon_config_lenient(&self.config);

        let mut daemons: BTreeMap<String, HttpClient> = BTreeMap::new();

        // legacy top-level api/token becomes an implicit "default" entry
        if let (Some(api), Some(token)) = (&user_cli.api, &user_cli.token) {
            daemons.insert(
                "default".to_string(),
                HttpClient::new(api.clone(), token.clone()),
            );
        }
        for (name, d) in &user_cli.daemons {
            daemons.insert(
                name.clone(),
                HttpClient::new(d.api.clone(), d.token.clone()),
            );
        }

        // last-resort fallback: local daemon config on disk. only if we have
        // nothing else — a bare `cerberus list` on a host running a daemon
        // Just Works without any CLI config.
        if daemons.is_empty()
            && let Some(c) = &daemon_cfg
            && let Some(token) = c.admin.token.clone()
        {
            daemons.insert(
                "default".to_string(),
                HttpClient::new(default_base_from_bind(&c.admin.bind), token),
            );
        }

        if daemons.is_empty() {
            return Err(
                "no daemons configured; pass --api/--token, or set [daemons.<name>] \
                 (or api/token) in ~/.config/cerberus/cli.toml"
                    .into(),
            );
        }

        // pick the "default" daemon for single-target commands with no prefix
        let (selected, restricted) = if let Some(d) = &self.daemon {
            if !daemons.contains_key(d) {
                return Err(format!(
                    "no daemon named {d:?}; known: {}",
                    daemons.keys().cloned().collect::<Vec<_>>().join(", ")
                )
                .into());
            }
            (Some(d.clone()), true)
        } else if let Some(d) = &user_cli.default {
            if !daemons.contains_key(d) {
                return Err(
                    format!("default = {d:?} in CLI config, but no such daemon defined").into(),
                );
            }
            (Some(d.clone()), false)
        } else if daemons.len() == 1 {
            (Some(daemons.keys().next().unwrap().clone()), true)
        } else if daemons.contains_key("default") {
            (Some("default".to_string()), false)
        } else {
            (None, false)
        };

        Ok(DaemonCtx {
            daemons,
            selected,
            restricted,
        })
    }
}

struct DaemonCtx {
    daemons: BTreeMap<String, HttpClient>,
    // name of the daemon a bare `key` target resolves to; None when
    // multiple daemons are configured and no default was picked, forcing
    // the operator to write `daemon/key`
    selected: Option<String>,
    // set when --daemon or --api narrowed the scope to a single daemon;
    // `list` then filters instead of aggregating across all configured ones
    restricted: bool,
}

impl DaemonCtx {
    fn has_multiple(&self) -> bool {
        self.daemons.len() > 1
    }

    // for reload etc. — needs exactly one daemon
    fn pick_single(&self) -> Result<(&str, &HttpClient), Box<dyn std::error::Error>> {
        let name = self.selected.as_deref().ok_or_else(|| {
            format!(
                "multiple daemons configured ({}); pick one with --daemon <name> or set default = ...",
                self.names_joined()
            )
        })?;
        let client = self.daemons.get(name).expect("selected must exist");
        Ok((name, client))
    }

    // target is either `daemon/key` or `key` (uses selected daemon)
    fn resolve(&self, target: &str) -> Result<(&HttpClient, String), Box<dyn std::error::Error>> {
        if let Some((d, k)) = target.split_once('/') {
            let client = self
                .daemons
                .get(d)
                .ok_or_else(|| format!("no daemon named {d:?}; known: {}", self.names_joined()))?;
            return Ok((client, k.to_string()));
        }
        let name = self.selected.as_deref().ok_or_else(|| {
            format!(
                "multiple daemons configured ({}); write target as `<daemon>/{target}` \
                 or pass --daemon <name>",
                self.names_joined()
            )
        })?;
        let client = self.daemons.get(name).expect("selected must exist");
        Ok((client, target.to_string()))
    }

    fn names_joined(&self) -> String {
        self.daemons.keys().cloned().collect::<Vec<_>>().join(", ")
    }
}

// hits every configured daemon in parallel, merges results, prefixes each
// key with its daemon name so operators see `local/syndicate`, `prod/beta`
async fn list_all(ctx: &DaemonCtx) -> Result<(), Box<dyn std::error::Error>> {
    // when --daemon or --api narrowed the scope, only hit that one; otherwise
    // aggregate across every configured daemon
    let targets: Vec<(&String, &HttpClient)> = if ctx.restricted {
        let name = ctx.selected.as_ref().expect("restricted implies selected");
        vec![(name, ctx.daemons.get(name).expect("selected must exist"))]
    } else {
        ctx.daemons.iter().collect()
    };

    let mut tasks = Vec::new();
    for (name, client) in &targets {
        let name = (*name).clone();
        let client = (*client).clone();
        tasks.push(tokio::spawn(async move {
            let res = client
                .request(reqwest::Method::GET, "/instances", None)
                .await
                .map_err(|e| e.to_string());
            (name, res)
        }));
    }

    let mut merged: Vec<Value> = Vec::new();
    let show_daemon = targets.len() > 1;
    for t in tasks {
        let (name, res) = t.await?;
        match res {
            Ok(Value::Array(rows)) => {
                for mut row in rows {
                    if show_daemon && let Some(obj) = row.as_object_mut() {
                        obj.insert("daemon".to_string(), Value::String(name.clone()));
                        if let Some(Value::String(k)) = obj.get("key").cloned() {
                            obj.insert("key".to_string(), Value::String(format!("{name}/{k}")));
                        }
                    }
                    merged.push(row);
                }
            }
            Ok(other) => {
                eprintln!("[{name}] unexpected response shape: {other}");
            }
            Err(e) => {
                eprintln!("[{name}] {e}");
            }
        }
    }

    println!("{}", serde_json::to_string_pretty(&Value::Array(merged))?);
    Ok(())
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
    // legacy: single-daemon fields; treated as an implicit "default"
    api: Option<String>,
    token: Option<String>,
    // which named daemon to use when a target has no prefix
    default: Option<String>,
    #[serde(default)]
    daemons: std::collections::HashMap<String, DaemonEntry>,
}

#[derive(Deserialize, Debug, Clone)]
struct DaemonEntry {
    api: String,
    token: String,
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

#[derive(Clone)]
struct HttpClient {
    base: String,
    token: String,
    client: reqwest::Client,
}

impl HttpClient {
    fn new(base: String, token: String) -> Self {
        Self {
            base,
            token,
            client: reqwest::Client::new(),
        }
    }

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
