//! ドメイン非依存の抽出 pipeline。
//! 入力は `(id, url)` の列、出力は `LanceStore` (1 namespace = 1 Lance dataset 群) への append。
//!
//! namespace 分離はファイルレベルで行うため、pipeline は namespace の概念を持たない。
//! 呼び出し側 (main, smoke_pipeline, ingest_urls 等) が namespace に対応する
//! Lance dataset ディレクトリを開いた `LanceStore` を渡す。
//! HN 専用エントリ (`run_hn_pipeline`) は `api::hn` 側にある。
//!
//! 構造:
//! ```text
//! URL list
//!   ▼
//! Stage 1 (fetch+parse, n=FETCH_CONCURRENCY)
//!   │ ContentRecord
//!   ▼
//! Splitter ───→ DbWrite::Content / CodeBlock ──┐
//!   │                                          │
//!   └→ ExtractRequest                          │
//!         │                                    │
//!         ▼                                    │
//!       Stage 2 (LLM extract + embed,          │
//!                n=LLM_CONCURRENCY)            │
//!         │                                    │
//!         └─→ DbWrite::Statement ──────────────┤
//!                                              ▼
//!                                       Writer Actor
//!                                       (LanceStore 1個所有、
//!                                        テーブル別 buffer で batch insert)
//! ```
//!
//! 設計上のポイント:
//! - Writer Actor は唯一の Lance writer。LanceStore は `Arc<LanceStore>` で共有する。
//! - テーブル追加は `DbWrite` の variant + `*Row` 構造体 + LanceStore::insert_* を追加するだけで完結。
//! - 全生産タスクが `writer_tx` を drop すると Actor が自然終了する (channel close で recv が None)。
//! - 失敗時の挙動: Stage 1 の fetch 失敗 / Stage 2 の LLM 失敗 は warn のみで継続、
//!   embed 失敗時は NULL embedding で statement を保存。

// TODO Review

use crate::agent::gemini::EMBEDDING_DIMS;
use crate::agent::{LlmProvider, Statement};
use crate::api::parse::fetch_html;
use crate::lance::LanceStore;
use crate::lance::tables::{code_blocks, contents, statements};
use crate::parse::html::parse_html;
use anyhow::{Context, Result};
use futures::stream::StreamExt;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info, warn};
use uuid::Uuid;

const FETCH_CONCURRENCY: usize = 20;
const LLM_CONCURRENCY: usize = 10;
const CONTENT_BATCH: usize = 500;
const CODE_BLOCK_BATCH: usize = 2000;
const STATEMENT_BATCH: usize = 2500;
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
    Content(contents::Row),
    CodeBlock(code_blocks::Row),
    Statement(statements::Row),
}

/// 汎用エントリ: 任意の URL リストで実行する。
/// 既存 id 重複の責務は呼び出し側に任せる (`store.existing_content_ids()` で除外しておく)。
/// 書き込み先 Lance dataset は `store` に紐付いたもの (= 呼び出し側が選んだ namespace)。
pub async fn run_pipeline_with_urls<P>(
    urls: Vec<(i64, String)>,
    client: Arc<P>,
    store: Arc<LanceStore>,
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

    let s1 = tokio::spawn(stage1_fetch(urls, content_tx));
    let splitter_h = tokio::spawn(splitter(content_rx, extract_tx, writer_tx.clone()));
    let s2 = tokio::spawn(stage2_extract(client, extract_rx, writer_tx));
    let writer_h = tokio::spawn(writer_actor(store, writer_rx));

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
            .send(DbWrite::Content(contents::Row {
                id: rec.id,
                url: rec.url,
                content: rec.content,
            }))
            .await
            .is_err()
        {
            warn!(id = rec.id, "writer closed; abort splitter");
            return Ok(());
        }

        for cb in rec.code_blocks {
            if writer_tx
                .send(DbWrite::CodeBlock(code_blocks::Row {
                    id: Uuid::now_v7(),
                    content_id: rec.id,
                    code: cb,
                }))
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
// Stage 2: LLM extract + embed (rate-limit bound, 並列度 LLM_CONCURRENCY)
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
                let stmts = match client.extract_statement(&req.content).await {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(content_id = req.content_id, error = %e, "extract failed");
                        return;
                    }
                };
                if stmts.is_empty() {
                    return;
                }
                let embeddings = embed_or_warn(&client, req.content_id, &stmts).await;
                forward_statements(req.content_id, stmts, embeddings, &tx).await;
            }
        })
        .buffer_unordered(LLM_CONCURRENCY)
        .for_each(|_| async {})
        .await;
    Ok(())
}

async fn embed_or_warn<P>(
    client: &Arc<P>,
    content_id: i64,
    stmts: &[Statement],
) -> Vec<Option<Vec<f32>>>
where
    P: LlmProvider,
{
    let texts: Vec<String> = stmts.iter().map(|s| s.statement.clone()).collect();
    match client.embed_texts(texts).await {
        Ok(vecs) => vecs
            .into_iter()
            .map(|v| Some(truncate_and_cast(v)))
            .collect(),
        Err(e) => {
            warn!(content_id, error = %e, "embed failed; insert with NULL embedding");
            vec![None; stmts.len()]
        }
    }
}

/// f64 ベクトルを EMBEDDING_DIMS まで truncate し、f32 (Lance Float32) に変換。
fn truncate_and_cast(v: Vec<f64>) -> Vec<f32> {
    v.into_iter()
        .take(EMBEDDING_DIMS)
        .map(|x| x as f32)
        .collect()
}

async fn forward_statements(
    content_id: i64,
    stmts: Vec<Statement>,
    embeddings: Vec<Option<Vec<f32>>>,
    tx: &mpsc::Sender<DbWrite>,
) {
    for (s, emb) in stmts.into_iter().zip(embeddings) {
        if tx
            .send(DbWrite::Statement(statements::Row {
                id: Uuid::now_v7(),
                content_id,
                statement: s.statement,
                keywords: s.keywords,
                embedding: emb,
            }))
            .await
            .is_err()
        {
            warn!(content_id, "writer closed; drop remaining");
            return;
        }
    }
}

// ----------------------------------------------------------------
// Writer Actor: 唯一の DB writer (LanceStore 1個所有)
// テーブル別に内部 buffer を持ち、各 buffer が閾値を超えたら batch insert
// ----------------------------------------------------------------

async fn writer_actor(store: Arc<LanceStore>, mut rx: mpsc::Receiver<DbWrite>) -> Result<()> {
    let mut content_buf: Vec<contents::Row> = Vec::with_capacity(CONTENT_BATCH);
    let mut code_buf: Vec<code_blocks::Row> = Vec::with_capacity(CODE_BLOCK_BATCH);
    let mut stmt_buf: Vec<statements::Row> = Vec::with_capacity(STATEMENT_BATCH);

    while let Some(msg) = rx.recv().await {
        match msg {
            DbWrite::Content(row) => {
                content_buf.push(row);
                if content_buf.len() >= CONTENT_BATCH {
                    flush_contents(&store, &mut content_buf).await?;
                }
            }
            DbWrite::CodeBlock(row) => {
                code_buf.push(row);
                if code_buf.len() >= CODE_BLOCK_BATCH {
                    flush_code_blocks(&store, &mut code_buf).await?;
                }
            }
            DbWrite::Statement(row) => {
                stmt_buf.push(row);
                if stmt_buf.len() >= STATEMENT_BATCH {
                    flush_statements(&store, &mut stmt_buf).await?;
                }
            }
        }
    }

    // 最終 flush
    if !content_buf.is_empty() {
        flush_contents(&store, &mut content_buf).await?;
    }
    if !code_buf.is_empty() {
        flush_code_blocks(&store, &mut code_buf).await?;
    }
    if !stmt_buf.is_empty() {
        flush_statements(&store, &mut stmt_buf).await?;
    }
    Ok(())
}

async fn flush_contents(store: &LanceStore, buf: &mut Vec<contents::Row>) -> Result<()> {
    let n = buf.len();
    let batch = std::mem::take(buf);
    store.insert_contents(batch).await?;
    info!(rows = n, "contents flushed");
    Ok(())
}

async fn flush_code_blocks(store: &LanceStore, buf: &mut Vec<code_blocks::Row>) -> Result<()> {
    let n = buf.len();
    let batch = std::mem::take(buf);
    store.insert_code_blocks(batch).await?;
    info!(rows = n, "code_blocks flushed");
    Ok(())
}

async fn flush_statements(store: &LanceStore, buf: &mut Vec<statements::Row>) -> Result<()> {
    let n = buf.len();
    let batch = std::mem::take(buf);
    store.insert_statements(batch).await?;
    info!(rows = n, "statements flushed");
    Ok(())
}
