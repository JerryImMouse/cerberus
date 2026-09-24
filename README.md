# cerberus

A watchdog and CLI for Space Station 14 servers.

Does the same job as [SS14.Watchdog](https://github.com/space-wizards/SS14.Watchdog), but written in Rust, ships as a Docker image, and doesn't leave your server dead when it crashes.

## What you get

- Restarts servers when they die. If one keeps crashing, it backs off instead of hammering the CDN.
- HTTP API and CLI. The CLI is the same binary as the daemon, or you can build a CLI-only binary and put it on your laptop.
- One CLI can talk to multiple daemons at once. `cerberus list` aggregates all your servers across all cerberus hosts you configured.
- Prometheus metrics at `/metrics`, ready for Grafana.
- Live log tail: `cerberus logs -f <server>`.
- History of every start, exit, crash and admin command in SQLite. `cerberus history <server>` shows what happened at 3am.
- Silence noisy servers without touching the process: `cerberus silence <server>`.
- Reload the config without restarting the daemon. Running servers keep going; new config kicks in when they next stop.
- Talks to SS14 game servers exactly like the C# watchdog does (same `watchdog.token` cvar, same `/server_api/{key}/ping`, same `/shutdown` and `/update` on the game server), so existing SS14 configs drop in unchanged.

## Quick start with docker compose

```bash
git clone <this repo>
cd cerberus
vim cerberus.toml # configure
docker compose up -d --build
```

That brings up cerberus on port 5000, loki on 3100, prometheus on 9090 and grafana on 3000.

Then:

```bash
docker compose exec cerberus cerberus list
```

## Configuration

Full example is `cerberus.toml` in the repo. Minimal:

```toml
[database]
file = "./db.sqlite"

[admin]
bind = "0.0.0.0:5000"
token = "some-random-string"

[logging]
level = "info"
[logging.overrides]
cerberus = "debug"
[logging.console]
enabled = true
compact = true
ansi = true

[servers.mymain]
name = "My Main Server"
api.token = "another-random-string"
api.port = 1212
update.type = "manifest"
update.manifest_url = "https://wizards.cdn.spacestation14.com/fork/wizards/manifest"
```

`api.token` is what the game server proves its identity with. `api.port` is the game server's own status port (must match its `config.toml`) - cerberus uses it to send `/shutdown` and `/update`.

For a private CDN with Basic auth:

```toml
[servers.mymain.update.authentication]
username = "myuser"
password = "mypass"
```

Per-server knobs you'll probably want at some point:

```toml
heartbeat_timeout = 60      # seconds of no ping before we kill and restart
restart_min_secs = 1        # backoff floor
restart_max_secs = 60       # backoff ceiling
healthy_after_secs = 30     # up this long, failure counter resets
silent = false              # true = drop the server's stdout/stderr from logs
```

## CLI

Every command takes an instance target, either bare or `daemon/key` when you have multiple daemons:

```bash
cerberus list                     # all instances across all configured daemons
cerberus status mymain
cerberus restart mymain           # graceful: POST /shutdown, wait, SIGKILL if it hangs
cerberus force-restart mymain     # SIGKILL now
cerberus stop mymain              # graceful stop, don't restart
cerberus update mymain            # checks the manifest; if there is an update, tells the server to shut down when it wants and applies on exit. If no update, does nothing.
cerberus silence mymain
cerberus unsilence mymain
cerberus history mymain -n 100
cerberus logs -f mymain
cerberus reload                   # re-read the config file
```

`update` does not force-restart. It waits for the round to end on its own. Same as upstream.

## CLI on your laptop, daemon on the server

Build a smaller CLI-only binary:

```bash
cargo build --release --no-default-features
```

That drops axum, sqlx and everything the daemon needs, leaves you with about 6 MiB.

Put the binary somewhere in your PATH, then create `~/.config/cerberus/cli.toml`:

```toml
default = "prod"

[daemons.prod]
api = "https://cerberus.example.com:5000"
token = "..."

[daemons.staging]
api = "https://staging.example.com:5000"
token = "..."
```

Now:

```bash
cerberus list                       # both daemons, keys prefixed like prod/mymain
cerberus --daemon staging list      # just staging
cerberus restart prod/mymain        # explicit
cerberus restart mymain             # uses default = "prod"
```

## HTTP API

Admin routes take `Authorization: Bearer <admin.token>`.

```
GET  /health                                    (no auth)
GET  /metrics                                   (no auth, Prometheus format)

GET  /instances
GET  /instances/{key}/status
POST /instances/{key}/start
POST /instances/{key}/stop
POST /instances/{key}/restart
POST /instances/{key}/force-restart
POST /instances/{key}/update
POST /instances/{key}/silence
POST /instances/{key}/unsilence
GET  /instances/{key}/history?limit=50
GET  /instances/{key}/logs                      (SSE)
POST /reload
```

Game server routes take HTTP Basic auth with `user=<key>`, `pass=<api.token>`, matching upstream:

```
POST /server_api/{key}/ping
```

## Grafana

`grafana/datasources.yml` wires up both Prometheus and Loki. Prometheus is the default. Open `http://localhost:3000`, anonymous admin is on. Try `cerberus_instance_up{key="mymain"}` in Explore.

Available metrics:

```
cerberus_instance_up
cerberus_instance_uptime_seconds
cerberus_instance_last_ping_seconds
cerberus_instance_backoff_seconds
cerberus_instance_failures
cerberus_instance_silenced
cerberus_instance_starts_total
cerberus_instance_crashes_total
cerberus_instance_clean_exits_total
cerberus_instance_heartbeat_lost_total
```

Crash counter only bumps on non-zero exits and signal deaths. When someone shuts a server down cleanly in-game, that goes into `clean_exits_total` and does not spend backoff budget.

## .NET version

The Dockerfile defaults to .NET 10 on noble. When you want .NET 11/12, etc.:

```bash
docker compose build --build-arg DOTNET_TAG=<TAG>
```

## Building without Docker

```bash
cargo build --release
./target/release/cerberus --config ./cerberus.toml daemon
```

Needs Rust 1.94+.

## Not done yet

- One container per instance. Right now all game servers are subprocesses of the cerberus container.
- Notification hooks (Discord/Slack webhook when things go wrong).
- Version pin and rollback.
- Other supported update methods, only manifest for now. Because this method is the most powerful.

## License

MIT.
