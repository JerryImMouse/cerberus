pub type IResult<T> = std::result::Result<T, InstanceError>;

#[derive(Debug, thiserror::Error)]
pub enum InstanceError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("join error: {0}")]
    Join(#[from] tokio::task::JoinError),
}
