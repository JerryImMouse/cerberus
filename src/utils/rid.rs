use std::collections::{HashMap, HashSet, VecDeque};
use std::env::consts::{ARCH, OS};
use std::sync::LazyLock;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct RidCatalog {
    runtimes: HashMap<String, Runtime>,
}

#[derive(Debug, Deserialize)]
pub struct Runtime {
    #[serde(rename = "#import", default)]
    imports: Vec<String>,
}

static CATALOG: LazyLock<RidCatalog> = LazyLock::new(|| {
    serde_json::from_str(include_str!("../../assets/utility.json"))
        .expect("embedded RID catalog is invalid")
});

pub fn find_rid(runtimes: &[String], current: Option<&str>) -> Option<String> {
    let catalog = &*CATALOG;

    let current = match current {
        Some(c) => c.to_owned(),
        None => match runtime_identifier() {
            Some(rid) if catalog.runtimes.contains_key(&rid) => {
                tracing::trace!(%rid, "current RID");
                rid
            }
            reported => {
                let guessed = guess_rid();
                tracing::trace!(?reported, guessed, "unknown RID reported, guessing");
                guessed.to_owned()
            }
        },
    };

    if !catalog.runtimes.contains_key(&current) {
        return None;
    }

    // BFS. `discovered` replaces the mutable `Discovered` flag from C#.
    let mut discovered = HashSet::from([current.clone()]);
    let mut queue = VecDeque::from([current]);

    while let Some(v) = queue.pop_front() {
        if runtimes.contains(&v) {
            return Some(v);
        }

        let Some(rt) = catalog.runtimes.get(&v) else {
            continue;
        };
        for w in &rt.imports {
            if discovered.insert(w.clone()) {
                queue.push_back(w.clone());
            }
        }
    }

    None
}

fn guess_rid() -> &'static str {
    match (OS, ARCH) {
        ("linux", "x86") => "linux-x86",
        ("linux", "x86_64") => "linux-x64",
        ("linux", "arm") => "linux-arm",
        ("linux", "aarch64") => "linux-arm64",
        ("freebsd", "x86_64") => "freebsd-x64",
        ("windows", "x86") => "win-x86",
        ("windows", "x86_64") => "win-x64",
        ("windows", "arm") => "win-arm",
        ("windows", "aarch64") => "win-arm64",
        ("macos", "x86_64") => "osx-x64",
        ("macos", "aarch64") => "osx-arm64",
        _ => "unknown",
    }
}

pub fn runtime_identifier() -> Option<String> {
    let os = match OS {
        "linux" if cfg!(target_env = "musl") => "linux-musl",
        "linux" => "linux",
        "windows" => "win",
        "macos" => "osx",
        _ => return None,
    };

    let arch = match ARCH {
        "x86_64" => "x64",
        "x86" => "x86",
        "aarch64" => "arm64",
        "arm" => "arm",
        _ => return None,
    };

    Some(format!("{os}-{arch}"))
}
