//! Agent チャット履歴の SQLite 永続化。
//!
//! テーブル: `chat_messages`
//! ```sql
//! CREATE TABLE chat_messages (
//!     id         INTEGER PRIMARY KEY AUTOINCREMENT,
//!     session_id TEXT    NOT NULL,
//!     role       TEXT    NOT NULL,  -- "user" | "assistant"
//!     content    TEXT    NOT NULL,
//!     created_at INTEGER NOT NULL   -- Unix timestamp (秒)
//! );
//! ```

use anyhow::{Context, Result};
use rig::completion::message::Message;
use sqlx::SqlitePool;

/// テーブルが存在しなければ作成する。アプリ起動時に 1 回呼ぶ。
pub async fn ensure_table(pool: &SqlitePool) -> Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS chat_messages (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT    NOT NULL,
            role       TEXT    NOT NULL,
            content    TEXT    NOT NULL,
            created_at INTEGER NOT NULL
        )",
    )
    .execute(pool)
    .await
    .context("create chat_messages table")?;

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_chat_messages_session
         ON chat_messages (session_id, id)",
    )
    .execute(pool)
    .await
    .context("create chat_messages index")?;

    Ok(())
}

/// セッションの会話履歴を古い順に取得し、rig の `Message` に変換して返す。
pub async fn load_history(pool: &SqlitePool, session_id: &str) -> Result<Vec<Message>> {
    let rows = sqlx::query_as::<_, (String, String)>(
        "SELECT role, content FROM chat_messages WHERE session_id = ? ORDER BY id",
    )
    .bind(session_id)
    .fetch_all(pool)
    .await
    .context("load chat history")?;

    Ok(rows
        .into_iter()
        .map(|(role, content)| match role.as_str() {
            "assistant" => Message::assistant(content),
            _ => Message::user(content),
        })
        .collect())
}

/// メッセージ 1 件を追記する。role は `"user"` または `"assistant"`。
pub async fn append(
    pool: &SqlitePool,
    session_id: &str,
    role: &str,
    content: &str,
) -> Result<()> {
    let now = chrono::Utc::now().timestamp();
    sqlx::query(
        "INSERT INTO chat_messages (session_id, role, content, created_at)
         VALUES (?, ?, ?, ?)",
    )
    .bind(session_id)
    .bind(role)
    .bind(content)
    .bind(now)
    .execute(pool)
    .await
    .context("append chat message")?;
    Ok(())
}

/// セッションの履歴を全件削除する。
pub async fn clear(pool: &SqlitePool, session_id: &str) -> Result<()> {
    sqlx::query("DELETE FROM chat_messages WHERE session_id = ?")
        .bind(session_id)
        .execute(pool)
        .await
        .context("clear chat history")?;
    Ok(())
}
