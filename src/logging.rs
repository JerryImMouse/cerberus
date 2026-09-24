use std::{collections::HashMap, str::FromStr};

use tokio::task::JoinHandle;
use tracing::level_filters::LevelFilter;
use tracing_loki::BackgroundTaskController;
use tracing_subscriber::{
    Layer, filter::Targets, layer::SubscriberExt, registry::Registry, util::SubscriberInitExt,
};

use crate::config::{ConsoleLoggingConfig, LokiLoggingConfig, SharedWatchdogConfig};

type BoxLayer = Box<dyn Layer<Registry> + Send + Sync + 'static>;

fn build_filter(default: LevelFilter, overrides: &HashMap<String, String>) -> Targets {
    let mut t = Targets::new().with_default(default);
    for (target, level) in overrides {
        match LevelFilter::from_str(level) {
            Ok(l) => t = t.with_target(target.as_str(), l),
            // tracing isn't initialized yet, so eprintln
            Err(_) => eprintln!("logging: invalid level {level:?} for target {target:?}"),
        }
    }
    t
}

fn loki_filter(default: LevelFilter, overrides: &HashMap<String, String>) -> Targets {
    ["hyper", "hyper_util", "reqwest", "h2", "tracing_loki"]
        .into_iter()
        .fold(build_filter(default, overrides), |t, name| {
            t.with_target(name, LevelFilter::OFF)
        })
}

pub fn setup(
    c: SharedWatchdogConfig,
) -> Result<Option<(BackgroundTaskController, JoinHandle<()>)>, tracing_loki::Error> {
    let mut layers: Vec<BoxLayer> = Vec::new();
    let level = LevelFilter::from_str(&c.logging.level).unwrap_or(LevelFilter::INFO);

    if let Some(console) = &c.logging.console
        && console.enabled
    {
        layers.push(console_layer(console, level, &c.logging.overrides));
    }

    let loki_data = if let Some(loki) = &c.logging.loki
        && loki.enabled
    {
        let (layer, handle, controller) = loki_layer(loki, level, &c.logging.overrides)?;
        layers.push(layer);
        Some((controller, handle))
    } else {
        None
    };

    let layers_len = layers.len();
    tracing_subscriber::registry().with(layers).init();

    tracing::info!(providers = %layers_len, "tracing initialized");
    Ok(loki_data)
}

fn console_layer(
    c: &ConsoleLoggingConfig,
    default: LevelFilter,
    overrides: &HashMap<String, String>,
) -> BoxLayer {
    tracing_subscriber::fmt::layer()
        .with_level(true)
        .with_file(true)
        .with_ansi(c.ansi)
        .with_ansi_sanitization(c.ansi)
        .with_filter(build_filter(default, overrides))
        .boxed()
}

fn loki_layer(
    c: &LokiLoggingConfig,
    default: LevelFilter,
    overrides: &HashMap<String, String>,
) -> Result<(BoxLayer, JoinHandle<()>, BackgroundTaskController), tracing_loki::Error> {
    let mut builder = tracing_loki::builder();
    for (name, value) in c.labels.iter() {
        builder = builder.label(name, value)?;
    }

    let (layer, controller, task) = builder.build_controller_url(c.endpoint.clone())?;
    let handle = tokio::spawn(task);

    Ok((
        layer.with_filter(loki_filter(default, overrides)).boxed(),
        handle,
        controller,
    ))
}
