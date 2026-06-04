//! `url_events` テーブル — URL 単位の処理状態管理。
//!
//! status の遷移: pending → processing → done | failed
//! id は URL の FNV-1a hash (= LanceStore の content_id と同一)。

use anyhow::{Context, Result};
use chrono::Utc;
use sqlx::SqlitePool;

pub struct UrlEvent {
    pub id: i64,
    pub url: String,
    pub namespace: String,
}

/// URL リストを pending として一括登録する。既存 id は INSERT OR IGNORE でスキップ。
pub async fn enqueue_many(pool: &SqlitePool, events: &[UrlEvent]) -> Result<()> {
    if events.is_empty() {
        return Ok(());
    }
    let now = Utc::now().timestamp();
    for ev in events {
        sqlx::query(
            "INSERT OR IGNORE INTO url_events (id, url, namespace, status, created_at, updated_at)
             VALUES (?, ?, ?, 'pending', ?, ?)",
        )
        .bind(ev.id)
        .bind(&ev.url)
        .bind(&ev.namespace)
        .bind(now)
        .bind(now)
        .execute(pool)
        .await
        .context("enqueue url_event")?;
    }
    Ok(())
}

/// pending イベントを processing に遷移させて返す。
/// `limit` 件を一括で claiming し、処理中クラッシュ時は processing のまま残る。
pub async fn claim_pending(
    pool: &SqlitePool,
    namespace: &str,
    limit: i64,
) -> Result<Vec<UrlEvent>> {
    let now = Utc::now().timestamp();
    let rows = sqlx::query_as::<_, (i64, String, String)>(
        "SELECT id, url, namespace FROM url_events
         WHERE namespace = ? AND status = 'pending'
         ORDER BY created_at
         LIMIT ?",
    )
    .bind(namespace)
    .bind(limit)
    .fetch_all(pool)
    .await
    .context("claim_pending select")?;

    if rows.is_empty() {
        return Ok(vec![]);
    }

    let ids: Vec<i64> = rows.iter().map(|(id, _, _)| *id).collect();
    // SQLite では IN (?, ?, ...) を動的に組む必要がある
    let placeholders = std::iter::repeat("?")
        .take(ids.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "UPDATE url_events SET status = 'processing', updated_at = ? WHERE id IN ({})",
        placeholders
    );
    let q = ids
        .iter()
        .fold(sqlx::query(&sql).bind(now), |q, id| q.bind(id));
    q.execute(pool).await.context("claim_pending update")?;

    Ok(rows
        .into_iter()
        .map(|(id, url, namespace)| UrlEvent { id, url, namespace })
        .collect())
}

/// 処理済み id を done にマークする。
pub async fn mark_done_many(pool: &SqlitePool, ids: &[i64]) -> Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    let now = Utc::now().timestamp();
    let placeholders = std::iter::repeat("?")
        .take(ids.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "UPDATE url_events SET status = 'done', updated_at = ? WHERE id IN ({})",
        placeholders
    );
    let q = ids
        .iter()
        .fold(sqlx::query(&sql).bind(now), |q, id| q.bind(id));
    q.execute(pool).await.context("mark_done_many")?;
    Ok(())
}

/// 失敗した id を failed にマークし、エラーメッセージを記録する。
pub async fn mark_failed(pool: &SqlitePool, id: i64, error: &str) -> Result<()> {
    let now = Utc::now().timestamp();
    sqlx::query(
        "UPDATE url_events SET status = 'failed', error = ?, updated_at = ? WHERE id = ?",
    )
    .bind(error)
    .bind(now)
    .bind(id)
    .execute(pool)
    .await
    .context("mark_failed")?;
    Ok(())
}

/// namespace 内の status 別件数を返す。
pub async fn count_by_status(
    pool: &SqlitePool,
    namespace: &str,
) -> Result<Vec<(String, i64)>> {
    let rows = sqlx::query_as::<_, (String, i64)>(
        "SELECT status, COUNT(*) FROM url_events WHERE namespace = ? GROUP BY status",
    )
    .bind(namespace)
    .fetch_all(pool)
    .await
    .context("count_by_status")?;
    Ok(rows)
}