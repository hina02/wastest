//! 日次パイプライン: hn_items の URL を起点に
//! Stage 1 (fetch + parse) → Splitter (fan-out)
//! → Stage 2 (LLM extract)
//! の3つの生産タスクが、唯一の DB writer である Writer Actor に
//! `DbWrite` enum を投げ込む構成。
//!
//! 構造:
//! ```text
//! Stage 1 (fetch+parse, n=20)
//!   │ ContentRecord
//!   ▼
//! Splitter ───→ DbWrite::Content / CodeBlock ──┐
//!   │                                          │
//!   └→ ExtractRequest                          │
//!         │                                    │
//!         ▼                                    │
//!       Stage 2 (LLM, n=10)                    │
//!         │                                    │
//!         └─→ DbWrite::Statement ──────────────┤
//!                                              ▼
//!                                       Writer Actor
//!                                       (Connection 1個所有,
//!                                        テーブル別 buffer で batch flush)
//! ```
//!
//! Writer Actor 設計の利点:
//! - DB Connection は1個だけ。`try_clone_conn` も1回呼ぶだけ
//! - 書き込み箇所が1ヶ所に集約され、将来トランザクション化しやすい
//! - テーブル追加は `DbWrite` の variant 追加で完結
//! - 全生産タスクが `writer_tx` を drop すると Actor が自然終了

use crate::agent::{LlmProvider, Statement};
use crate::api::parse::fetch_html;
use crate::db::duck::{DuckDBWriter, DuckReadOps};
use crate::db::hn::list_urls;
use crate::parse::html::parse_html;
use anyhow::{Context, Result};
use duckdb::Connection;
use duckdb::params;
use futures::stream::StreamExt;
use sqlx::SqlitePool;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info, warn};
use uuid::Uuid;

const FETCH_CONCURRENCY: usize = 20;
const LLM_CONCURRENCY: usize = 10;
const CONTENT_BATCH: usize = 20;
const CODE_BLOCK_BATCH: usize = 80;
const STATEMENT_BATCH: usize = 50;
const CHANNEL_BUFFER: usize = 100;

/// Stage 1 → Splitter の搬送物。
struct ContentRecord {
    id: i64,
    url: String,
    content: String,
    code_blocks: Vec<String>,
}

/// Splitter → Stage 2 の搬送物。本文をコピーして渡す。
struct ExtractRequest {
    content_id: i64,
    content: String,
}

/// Writer Actor が受け取る書き込みコマンド。
/// テーブル追加時はここに variant を足す。
enum DbWrite {
    Content {
        id: i64,
        url: String,
        content: String,
    },
    CodeBlock {
        id: Uuid,
        content_id: i64,
        code: String,
    },
    Statement {
        id: Uuid,
        content_id: i64,
        statement: String,
        keywords: Vec<String>,
    },
}

/// 本番エントリ: SQLite hn_items を起点に、未処理の URL だけ流す。
pub async fn run_pipeline<P>(
    pool: &SqlitePool,
    client: Arc<P>,
    writer: &DuckDBWriter,
) -> Result<()>
where
    P: LlmProvider + 'static,
{
    let all_urls = list_urls(pool).await?;
    let existing = writer.existing_content_ids()?;
    let urls: Vec<(i64, String)> = all_urls
        .into_iter()
        .filter(|(id, _)| !existing.contains(id))
        .collect();
    run_pipeline_with_urls(urls, client, writer).await
}

/// 動作確認・部分実行用: 任意の URL リストを直接流す。
/// 既存 id 重複の責務は呼び出し側に任せる。
pub async fn run_pipeline_with_urls<P>(
    urls: Vec<(i64, String)>,
    client: Arc<P>,
    writer: &DuckDBWriter,
) -> Result<()>
where
    P: LlmProvider + 'static,
{
    if urls.is_empty() {
        info!("pipeline: 未処理 URL なし");
        return Ok(());
    }
    info!(count = urls.len(), "pipeline: 開始");

    let (content_tx, content_rx) = mpsc::channel::<ContentRecord>(CHANNEL_BUFFER);
    let (extract_tx, extract_rx) = mpsc::channel::<ExtractRequest>(CHANNEL_BUFFER);
    let (writer_tx, writer_rx) = mpsc::channel::<DbWrite>(CHANNEL_BUFFER);

    let writer_conn = writer.try_clone_conn()?;

    let s1 = tokio::spawn(stage1_fetch(urls, content_tx));
    let splitter_h = tokio::spawn(splitter(content_rx, extract_tx, writer_tx.clone()));
    let s2 = tokio::spawn(stage2_extract(client, extract_rx, writer_tx));
    let writer_h = tokio::spawn(writer_actor(writer_conn, writer_rx));

    s1.await.context("stage1 join")??;
    splitter_h.await.context("splitter join")??;
    s2.await.context("stage2 join")??;
    writer_h.await.context("writer join")??;

    info!("pipeline: 完了");
    Ok(())
}

// ----------------------------------------------------------------
// Stage 1: fetch + parse (I/O bound, 並列度 FETCH_CONCURRENCY)
// ----------------------------------------------------------------

async fn stage1_fetch(
    urls: Vec<(i64, String)>,
    content_tx: mpsc::Sender<ContentRecord>,
) -> Result<()> {
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .user_agent("wastest/0.1")
        .build()?;

    futures::stream::iter(urls)
        .map(|(id, url)| {
            let http = http.clone();
            let tx = content_tx.clone();
            async move {
                let body = match fetch_html(&http, &url).await {
                    Ok(b) => b,
                    Err(e) => {
                        warn!(id, %url, error = %e, "fetch failed");
                        return;
                    }
                };
                let parsed = parse_html(&body, &url);
                if parsed.is_spa || parsed.content.is_empty() {
                    warn!(id, %url, "empty content (SPA or noise-only)");
                    return;
                }
                let rec = ContentRecord {
                    id,
                    url,
                    content: parsed.content,
                    code_blocks: parsed.code_blocks,
                };
                if tx.send(rec).await.is_err() {
                    warn!(id, "downstream closed before send");
                }
            }
        })
        .buffer_unordered(FETCH_CONCURRENCY)
        .for_each(|_| async {})
        .await;

    Ok(())
}

// ----------------------------------------------------------------
// Splitter: ContentRecord を Writer と Stage 2 に分配する
// (DB には触らない、ただの fan-out タスク)
// ----------------------------------------------------------------

async fn splitter(
    mut content_rx: mpsc::Receiver<ContentRecord>,
    extract_tx: mpsc::Sender<ExtractRequest>,
    writer_tx: mpsc::Sender<DbWrite>,
) -> Result<()> {
    while let Some(rec) = content_rx.recv().await {
        if extract_tx
            .send(ExtractRequest {
                content_id: rec.id,
                content: rec.content.clone(),
            })
            .await
            .is_err()
        {
            warn!(id = rec.id, "stage2 closed; stop forwarding");
        }

        if writer_tx
            .send(DbWrite::Content {
                id: rec.id,
                url: rec.url,
                content: rec.content,
            })
            .await
            .is_err()
        {
            warn!(id = rec.id, "writer closed; abort splitter");
            return Ok(());
        }

        for cb in rec.code_blocks {
            if writer_tx
                .send(DbWrite::CodeBlock {
                    id: Uuid::now_v7(),
                    content_id: rec.id,
                    code: cb,
                })
                .await
                .is_err()
            {
                return Ok(());
            }
        }
    }
    Ok(())
}

// ----------------------------------------------------------------
// Stage 2: LLM extract (rate-limit bound, 並列度 LLM_CONCURRENCY)
// 抽出結果は直接 writer_tx へ送る (Sink B は不要になった)
// ----------------------------------------------------------------

async fn stage2_extract<P>(
    client: Arc<P>,
    extract_rx: mpsc::Receiver<ExtractRequest>,
    writer_tx: mpsc::Sender<DbWrite>,
) -> Result<()>
where
    P: LlmProvider + 'static,
{
    ReceiverStream::new(extract_rx)
        .map(|req| {
            let client = client.clone();
            let tx = writer_tx.clone();
            async move {
                match client.extract_statement(&req.content).await {
                    Ok(stmts) => forward_statements(req.content_id, stmts, &tx).await,
                    Err(e) => warn!(content_id = req.content_id, error = %e, "extract failed"),
                }
            }
        })
        .buffer_unordered(LLM_CONCURRENCY)
        .for_each(|_| async {})
        .await;
    Ok(())
}

async fn forward_statements(
    content_id: i64,
    stmts: Vec<Statement>,
    tx: &mpsc::Sender<DbWrite>,
) {
    for s in stmts {
        if tx
            .send(DbWrite::Statement {
                id: Uuid::now_v7(),
                content_id,
                statement: s.statement,
                keywords: s.keywords,
            })
            .await
            .is_err()
        {
            warn!(content_id, "writer closed; drop remaining");
            return;
        }
    }
}

// ----------------------------------------------------------------
// Writer Actor: 唯一の DB writer
// テーブル別に内部バッファを持ち、各バッファが閾値を超えたら flush
// すべての writer_tx が drop されると recv が None を返して終了 → 残バッファを最終 flush
// ----------------------------------------------------------------

async fn writer_actor(conn: Connection, mut rx: mpsc::Receiver<DbWrite>) -> Result<()> {
    let mut content_buf: Vec<(i64, String, String)> = Vec::with_capacity(CONTENT_BATCH);
    let mut code_buf: Vec<(Uuid, i64, String)> = Vec::with_capacity(CODE_BLOCK_BATCH);
    let mut stmt_buf: Vec<(Uuid, i64, String, Vec<String>)> = Vec::with_capacity(STATEMENT_BATCH);

    while let Some(msg) = rx.recv().await {
        match msg {
            DbWrite::Content { id, url, content } => {
                content_buf.push((id, url, content));
                if content_buf.len() >= CONTENT_BATCH {
                    flush_contents(&conn, &mut content_buf)?;
                }
            }
            DbWrite::CodeBlock {
                id,
                content_id,
                code,
            } => {
                code_buf.push((id, content_id, code));
                if code_buf.len() >= CODE_BLOCK_BATCH {
                    flush_code_blocks(&conn, &mut code_buf)?;
                }
            }
            DbWrite::Statement {
                id,
                content_id,
                statement,
                keywords,
            } => {
                stmt_buf.push((id, content_id, statement, keywords));
                if stmt_buf.len() >= STATEMENT_BATCH {
                    flush_statements(&conn, &mut stmt_buf)?;
                }
            }
        }
    }

    // 最終 flush
    if !content_buf.is_empty() {
        flush_contents(&conn, &mut content_buf)?;
    }
    if !code_buf.is_empty() {
        flush_code_blocks(&conn, &mut code_buf)?;
    }
    if !stmt_buf.is_empty() {
        flush_statements(&conn, &mut stmt_buf)?;
    }
    Ok(())
}

// ----------------------------------------------------------------
// テーブル別 flush ヘルパー (writer_actor からのみ呼ばれる)
// ----------------------------------------------------------------

fn flush_contents(conn: &Connection, buf: &mut Vec<(i64, String, String)>) -> Result<()> {
    let n = buf.len();
    let mut app = conn.appender("hn_contents")?;
    for (id, url, content) in buf.drain(..) {
        app.append_row(params![id, url, content])?;
    }
    app.flush()?;
    info!(rows = n, "hn_contents flushed");
    Ok(())
}

fn flush_code_blocks(conn: &Connection, buf: &mut Vec<(Uuid, i64, String)>) -> Result<()> {
    let n = buf.len();
    let mut app = conn.appender("code_blocks")?;
    for (id, content_id, code) in buf.drain(..) {
        app.append_row(params![id, content_id, code])?;
    }
    app.flush()?;
    info!(rows = n, "code_blocks flushed");
    Ok(())
}

/// statements の flush。
/// `keywords` は `TEXT[]` 列。DuckDB 自体は配列をネイティブに保存・クエリできる
/// (`list_contains`, `unnest`, `array_length` 等が動く) が、duckdb-rs / C API の
/// パラメータ bind では List 型を渡せない (`duckdb_bind_value` 側の未実装)。
/// そのため keywords は SQL リテラル `['a','b']` 形式で埋め込み、
/// 他のカラムだけ `?` で bind する。embedding は後段で埋めるので NULL リテラル。
fn flush_statements(
    conn: &Connection,
    buf: &mut Vec<(Uuid, i64, String, Vec<String>)>,
) -> Result<()> {
    if buf.is_empty() {
        return Ok(());
    }
    let n = buf.len();
    for (id, content_id, statement, keywords) in buf.drain(..) {
        let kw_lit = format_text_array(&keywords);
        let sql = format!(
            "INSERT INTO statements (id, content_id, statement, keywords, embedding)
             VALUES (?, ?, ?, {kw_lit}, NULL)"
        );
        conn.execute(&sql, params![id, content_id, statement])?;
    }
    info!(rows = n, "statements flushed");
    Ok(())
}

/// `Vec<String>` を DuckDB の TEXT[] リテラル `['a','b','c']` に変換する。
/// single quote だけエスケープ (SQL 文字列リテラルでは `'` → `''`)。
fn format_text_array(items: &[String]) -> String {
    if items.is_empty() {
        return "[]::TEXT[]".to_string();
    }
    let parts: Vec<String> = items
        .iter()
        .map(|s| format!("'{}'", s.replace('\'', "''")))
        .collect();
    format!("[{}]", parts.join(","))
}
