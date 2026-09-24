mod error;
mod utils;
use std::path::PathBuf;

pub use error::{UResult, UpdateError};

pub mod manifest;

#[async_trait::async_trait]
pub trait UpdateProvider: Send + Sync {
    async fn check_for_updates(&self, current_version: Option<String>) -> bool;
    async fn run_update(&self, current_version: Option<String>, path: PathBuf) -> Option<String>;
}
