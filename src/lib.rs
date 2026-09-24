pub mod cli;
pub mod config;

#[cfg(feature = "daemon")]
pub mod api;
#[cfg(feature = "daemon")]
pub mod db;
#[cfg(feature = "daemon")]
pub mod instances;
#[cfg(feature = "daemon")]
pub mod logging;
#[cfg(feature = "daemon")]
pub mod supervisor;
#[cfg(feature = "daemon")]
pub mod updates;
#[cfg(feature = "daemon")]
pub mod utils;

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    use clap::Parser;
    cli::Cli::parse().dispatch().await
}

#[cfg(feature = "daemon")]
mod daemon_impl {
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

    use crate::{config::SharedWatchdogConfig, supervisor::Watchdog};

    pub async fn daemon(
        cfg: SharedWatchdogConfig,
        config_path: std::path::PathBuf,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::{api, db, logging};
        let loki_info = logging::setup(cfg.clone())?;
        let db = db::setup(&cfg.database.file).await?;
        let admin_base_url = local_base_url(&cfg.admin.bind);

        let watchdog = Arc::new(Watchdog::new(cfg.clone(), db, admin_base_url, config_path));

        let shutdown = CancellationToken::new();
        install_signal_handlers(shutdown.clone());

        let api_cfg = cfg.clone();
        let api_wd = watchdog.clone();
        let api_shutdown = shutdown.clone();
        let api_task = tokio::spawn(async move {
            if let Err(e) = api::serve(api_cfg, api_wd, api_shutdown).await {
                tracing::error!(error = %e, "admin api server error");
            }
        });

        tracing::info!("watchdog ready");
        shutdown.cancelled().await;
        tracing::info!("shutdown requested; draining");

        watchdog.shutdown_all().await;
        // The API server takes its own shutdown from the same token.
        let _ = api_task.await;

        if let Some((controller, handle)) = loki_info {
            controller.shutdown().await;
            let _ = handle.await;
        }
        tracing::info!("bye");
        Ok(())
    }

    fn local_base_url(bind: &str) -> String {
        let (host, port) = bind.rsplit_once(':').unwrap_or(("127.0.0.1", "5000"));
        let host = if host == "0.0.0.0" || host == "[::]" || host.is_empty() {
            "127.0.0.1"
        } else {
            host
        };
        format!("http://{host}:{port}")
    }

    #[cfg(unix)]
    fn install_signal_handlers(token: CancellationToken) {
        use tokio::signal::unix::{SignalKind, signal};
        tokio::spawn(async move {
            let mut sigterm = match signal(SignalKind::terminate()) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(error = %e, "failed to install SIGTERM handler");
                    return;
                }
            };
            let mut sigint = match signal(SignalKind::interrupt()) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(error = %e, "failed to install SIGINT handler");
                    return;
                }
            };
            tokio::select! {
                _ = sigterm.recv() => tracing::info!("SIGTERM received"),
                _ = sigint.recv() => tracing::info!("SIGINT received"),
            }
            token.cancel();
        });
    }

    #[cfg(not(unix))]
    fn install_signal_handlers(token: CancellationToken) {
        tokio::spawn(async move {
            if let Err(e) = tokio::signal::ctrl_c().await {
                tracing::error!(error = %e, "failed to install ctrl-c handler");
                return;
            }
            tracing::info!("ctrl-c received");
            token.cancel();
        });
    }
} // mod daemon_impl

#[cfg(feature = "daemon")]
pub use daemon_impl::daemon;
