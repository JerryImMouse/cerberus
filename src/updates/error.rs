pub type UResult<T> = std::result::Result<T, UpdateError>;

#[derive(Debug, thiserror::Error)]
pub enum UpdateError {
    #[error("zip error: {0}")]
    Zip(#[from] zip::result::ZipError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("join error: {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error("unable to find compatible build for a platform")]
    NoRid,

    #[error("invalid hash, expected `{expected}`, got `{got}`")]
    HashMismatch { expected: String, got: String },
}
