use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use chrono::Utc;
use futures_util::StreamExt;
use reqwest::Client;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::{
    fs::File,
    io::{AsyncSeekExt, AsyncWriteExt},
};
use url::Url;

use super::UpdateProvider;
use crate::{
    config::ManifestAuth,
    updates::{UResult, UpdateError, utils::extract_build},
    utils,
};

#[derive(Debug)]
pub struct ManifestUpdateProvider {
    manifest_url: Url,
    auth: Option<ManifestAuth>,
    client: Client,
}

impl ManifestUpdateProvider {
    pub fn new(manifest_url: Url, auth: Option<ManifestAuth>) -> Self {
        let client = Client::builder()
            .user_agent("cerberus")
            .connect_timeout(Duration::from_secs(15))
            .read_timeout(Duration::from_secs(30))
            .build()
            .expect("shouldn't happen");
        Self {
            manifest_url,
            auth,
            client,
        }
    }

    fn get(&self, url: &str) -> reqwest::RequestBuilder {
        let mut req = self.client.get(url);
        if let Some(auth) = &self.auth {
            req = req.basic_auth(&auth.username, Some(&auth.password));
        }
        req
    }

    pub async fn fetch_manifest(&self) -> UResult<ManifestInfo> {
        tracing::debug!(manifest_url = %self.manifest_url, "fetching build manifest");
        let res = self
            .get(self.manifest_url.as_str())
            .timeout(Duration::from_secs(30))
            .send()
            .await?;
        res.error_for_status_ref()?;
        let manifest = res.json().await?;
        Ok(manifest)
    }

    #[tracing::instrument(skip_all, fields(version = %version, rid = tracing::field::Empty))]
    async fn download_and_install<'a, P: AsRef<Path> + Send>(
        &self,
        version: &'a str,
        info: &VersionInfo,
        path: P,
    ) -> UResult<&'a str> {
        let keys: Vec<String> = info.server.keys().cloned().collect();
        let rid = utils::rid::find_rid(&keys, None).ok_or(UpdateError::NoRid)?;
        tracing::Span::current().record("rid", rid.as_str());
        let build = &info.server[&rid];

        let res = self.get(build.url.as_str()).send().await?;
        res.error_for_status_ref()?;
        let total = res.content_length();
        tracing::info!(url = %build.url, total, "downloading server binary");

        let mut file = File::from_std(tempfile::tempfile()?);
        let mut hasher = Sha256::new();
        let mut stream = res.bytes_stream();

        let start = Instant::now();
        let mut last_log = start;
        let mut downloaded = 0u64;

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            downloaded += chunk.len() as u64;
            hasher.update(&chunk);
            file.write_all(&chunk).await?;

            if last_log.elapsed() >= Duration::from_secs(5) {
                tracing::debug!(downloaded, total, "download progress");
                last_log = Instant::now();
            }
        }
        file.flush().await?;

        let secs = start.elapsed().as_secs_f64();
        let mib_per_s = (downloaded as f64 / 1_048_576.0 / secs.max(0.001) * 10.0).round() / 10.0;
        tracing::info!(
            bytes = downloaded,
            elapsed_s = secs,
            mib_per_s,
            "download finished"
        );

        let actual = hex::encode(hasher.finalize());
        if !actual.eq_ignore_ascii_case(&build.sha256) {
            tracing::error!(expected = %build.sha256, %actual, "hash mismatch");
            return Err(UpdateError::HashMismatch {
                expected: build.sha256.clone(),
                got: actual,
            });
        }
        tracing::debug!(sha256 = %actual, "hash verified");

        file.seek(std::io::SeekFrom::Start(0)).await?;
        let file = file.into_std().await;
        let dest = path.as_ref().to_owned();

        tracing::info!(dest = %dest.display(), "extracting zip");
        let t = Instant::now();
        let dest_log = dest.clone();
        tokio::task::spawn_blocking(move || -> UResult<()> {
            // extract next to the target and swap at the end, so a failed
            // extraction never destroys the currently installed version.
            let staging = sibling(&dest, ".new");
            if staging.exists() {
                std::fs::remove_dir_all(&staging)?;
            }
            std::fs::create_dir_all(&staging)?;

            if let Err(e) = extract_build(staging.clone(), file) {
                let _ = std::fs::remove_dir_all(&staging);
                return Err(e);
            }

            if dest.exists() {
                std::fs::remove_dir_all(&dest)?;
            }
            std::fs::rename(&staging, &dest)?;
            Ok(())
        })
        .await??;

        tracing::info!(
            dest = %dest_log.display(),
            elapsed_ms = t.elapsed().as_millis() as u64,
            "extracted"
        );

        Ok(version)
    }
}

fn sibling(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_owned();
    name.push(suffix);
    path.with_file_name(name)
}

fn latest_version(manifest: &ManifestInfo) -> Option<&str> {
    manifest
        .builds
        .iter()
        .max_by_key(|(_, build)| build.time)
        .map(|(key, _)| key.as_str())
}

#[async_trait::async_trait]
impl UpdateProvider for ManifestUpdateProvider {
    async fn check_for_updates(&self, current_version: Option<String>) -> bool {
        let manifest = match self.fetch_manifest().await {
            Ok(manifest) => manifest,
            Err(e) => {
                tracing::error!(error = ?e, "error fetching manifest");
                return false;
            }
        };

        match latest_version(&manifest) {
            Some(latest) => current_version.as_deref() != Some(latest),
            None => false,
        }
    }

    async fn run_update(&self, current_version: Option<String>, path: PathBuf) -> Option<String> {
        let manifest = match self.fetch_manifest().await {
            Ok(manifest) => manifest,
            Err(e) => {
                tracing::error!(error = ?e, "error fetching manifest");
                // its kinda silly, but we can't let it stop the whole watchdog
                return None;
            }
        };

        let Some(latest) = latest_version(&manifest) else {
            tracing::info!("no versions found, no updates");
            return None;
        };

        if current_version.as_deref() == Some(latest) {
            tracing::info!(%latest, "already up to date");
            return None;
        }

        tracing::info!(%latest, current_version = ?current_version, "updating");

        let info = &manifest.builds[latest];
        match self.download_and_install(latest, info, path).await {
            Ok(v) => {
                tracing::info!(version = %v, previous = ?current_version, "update installed");
                Some(v.to_string())
            }
            Err(e) => {
                tracing::error!(error = ?e, %latest, "update failed");
                None
            }
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ManifestInfo {
    builds: HashMap<String, VersionInfo>,
}

#[derive(Debug, Deserialize)]
pub struct VersionInfo {
    time: chrono::DateTime<Utc>,
    server: HashMap<String, DownloadInfo>,
}

#[derive(Debug, Deserialize)]
pub struct DownloadInfo {
    url: Url,
    sha256: String,
}
