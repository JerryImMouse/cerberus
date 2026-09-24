use sqlx::SqlitePool;

use crate::db::DResult;

#[derive(Debug, Clone)]
pub struct Server {
    pub key: String,
    pub version: Option<String>,
}

pub async fn create(d: &SqlitePool, key: &str, version: &str) -> DResult<Server> {
    Ok(sqlx::query_as!(
        Server,
        "INSERT INTO servers (key, version) VALUES (?, ?) RETURNING *",
        key,
        version
    )
    .fetch_one(d)
    .await?)
}

pub async fn update_version(d: &SqlitePool, key: &str, version: &str) -> DResult<()> {
    sqlx::query!("UPDATE servers SET version = ? WHERE key = ?", version, key)
        .execute(d)
        .await?;
    Ok(())
}

pub async fn find_by_key(d: &SqlitePool, key: &str) -> DResult<Option<Server>> {
    Ok(
        sqlx::query_as!(Server, "SELECT * FROM servers WHERE key = ?", key)
            .fetch_optional(d)
            .await?,
    )
}
