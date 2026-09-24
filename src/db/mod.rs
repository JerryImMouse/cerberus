use sqlx::{
    SqlitePool,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};
use std::path::Path;

pub mod incidents;
pub mod server;

mod error;
pub use error::{DResult, DatabaseError};

pub async fn setup<P: AsRef<Path>>(file: P) -> DResult<SqlitePool> {
    let pool = SqlitePoolOptions::new()
        .connect_with(
            SqliteConnectOptions::new()
                .filename(file)
                .create_if_missing(true)
                .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal),
        )
        .await?;

    sqlx::migrate!().run(&pool).await?;
    Ok(pool)
}
