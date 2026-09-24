use serde::Serialize;
use sqlx::{Row, SqlitePool};

use crate::db::DResult;

#[derive(Debug, Clone, Copy)]
pub enum Kind {
    Start,
    Crashed,
    CleanExit,
    HeartbeatLost,
    Admin,
    SpawnFailed,
}

impl Kind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Kind::Start => "start",
            Kind::Crashed => "crashed",
            Kind::CleanExit => "clean_exit",
            Kind::HeartbeatLost => "heartbeat_lost",
            Kind::Admin => "admin",
            Kind::SpawnFailed => "spawn_failed",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Incident {
    pub id: i64,
    pub instance_key: String,
    pub ts: i64,
    pub kind: String,
    pub detail: Option<String>,
}

pub async fn record(pool: &SqlitePool, key: &str, kind: Kind, detail: Option<&str>) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let res =
        sqlx::query("INSERT INTO incidents (instance_key, ts, kind, detail) VALUES (?, ?, ?, ?)")
            .bind(key)
            .bind(now)
            .bind(kind.as_str())
            .bind(detail)
            .execute(pool)
            .await;
    if let Err(e) = res {
        tracing::warn!(error = %e, instance = %key, kind = kind.as_str(), "failed to record incident");
    }
}

pub async fn list_recent(pool: &SqlitePool, key: &str, limit: i64) -> DResult<Vec<Incident>> {
    let rows = sqlx::query(
        "SELECT id, instance_key, ts, kind, detail
         FROM incidents WHERE instance_key = ? ORDER BY ts DESC, id DESC LIMIT ?",
    )
    .bind(key)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        out.push(Incident {
            id: r.try_get("id")?,
            instance_key: r.try_get("instance_key")?,
            ts: r.try_get("ts")?,
            kind: r.try_get("kind")?,
            detail: r.try_get::<Option<String>, _>("detail")?,
        });
    }
    Ok(out)
}

pub async fn count(pool: &SqlitePool, key: &str, kind: Kind) -> DResult<u64> {
    let row =
        sqlx::query("SELECT COUNT(*) AS n FROM incidents WHERE instance_key = ? AND kind = ?")
            .bind(key)
            .bind(kind.as_str())
            .fetch_one(pool)
            .await?;
    let n: i64 = row.try_get("n")?;
    Ok(n.max(0) as u64)
}
