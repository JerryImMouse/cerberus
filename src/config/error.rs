pub type CResult<T> = std::result::Result<T, ConfigError>;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("toml error: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
