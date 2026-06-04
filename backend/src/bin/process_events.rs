//! url_events から pending を claim し、pipeline を実行して done/failed にマークする。
//!
//! ```
//! cargo run --bin process_events -- <namespace> [--batch <n>]
//! ```
//!
//! 処理フロー:
//! 1. url_events から pending を `batch` 件 claim (→ processing に遷移)
//! 2. `run_pipeline_with_urls` で一括処理
//! 3. Lance に取り込まれた content_id → done、それ以外 → failed
//! 4. 1 に戻る (pending がなくなるまでループ)
//!
//! クラッシュしても processing 状態の行が残るだけなので、
//! 手動で `UPDATE url_events SET status='pending' WHERE status='processing'` で再試行できる。

use anyhow::{Context, Result};
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::EnvFilter;
use wastest::config::SETTINGS;
use wastest::connect;
use wastest::db::events::{claim_pending, count_by_status, mark_done_many, mark_failed};
use wastest::lance::LanceStore;
use wastest::pipeline::run_pipeline_with_urls;
use wastest::GeminiClient;

const DEFAULT_BATCH: i64 = 100;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let mut args = std::env::args().skip(1).peekable();
    let namespace = args
        .next()
        .context("usage: process_events <namespace> [--batch <n>]")?;
    let batch_size = parse_batch_arg(args);

    let pool = connect(&SETTINGS.database_url).await?;
    let sqlite_ddl = include_str!("../../../ddl/sqlite.sql");
    sqlx::raw_sql(sqlite_ddl).execute(&pool).await?;

    let uri = SETTINGS.lance_uri_for(&namespace);
    let store = Arc::new(LanceStore::open(&uri).await?);
    let client = Arc::new(GeminiClient::new().await?);

    let mut total_done = 0usize;
    let mut total_failed = 0usize;

    loop {
        let claimed = claim_pending(&pool, &namespace, batch_size).await?;
        if claimed.is_empty() {
            info!("pending なし。終了。");
            break;
        }
        info!(count = claimed.len(), "claimed");

        let input: Vec<(i64, String)> = claimed
            .iter()
            .map(|ev| (ev.id, ev.url.clone()))
            .collect();
        let input_ids: Vec<i64> = claimed.iter().map(|ev| ev.id).collect();

        // pipeline 実行前の取り込み済み ID セット
        let before = store.existing_content_ids().await?;

        match run_pipeline_with_urls(input, client.clone(), store.clone()).await {
            Err(e) => {
                // pipeline 全体がエラー → 全件 failed
                for id in &input_ids {
                    mark_failed(&pool, *id, &e.to_string()).await?;
                }
                total_failed += input_ids.len();
                info!(count = input_ids.len(), error = %e, "batch failed");
            }
            Ok(()) => {
                // Lance に新たに取り込まれた ID = 成功
                let after = store.existing_content_ids().await?;
                let newly_added: Vec<i64> = input_ids
                    .iter()
                    .copied()
                    .filter(|id| after.contains(id) && !before.contains(id))
                    .collect();
                // 既に以前から存在していた ID も done 扱い
                let already_existed: Vec<i64> = input_ids
                    .iter()
                    .copied()
                    .filter(|id| before.contains(id))
                    .collect();
                let done_ids: Vec<i64> = newly_added
                    .iter()
                    .chain(already_existed.iter())
                    .copied()
                    .collect();
                let failed_ids: Vec<i64> = input_ids
                    .iter()
                    .copied()
                    .filter(|id| !after.contains(id))
                    .collect();

                mark_done_many(&pool, &done_ids).await?;
                for id in &failed_ids {
                    mark_failed(&pool, *id, "not found in lance after pipeline").await?;
                }
                total_done += done_ids.len();
                total_failed += failed_ids.len();
                info!(done = done_ids.len(), failed = failed_ids.len(), "batch complete");
            }
        }
    }

    // 最終集計
    let counts = count_by_status(&pool, &namespace).await?;
    println!("\n--- namespace={namespace} 最終集計 ---");
    for (status, n) in &counts {
        println!("  {status}: {n}");
    }
    println!("今回: done={total_done}, failed={total_failed}");
    println!(
        "\nFTS/Vector index を更新するには:\n  cargo run --bin refresh_indexes -- {namespace}"
    );
    Ok(())
}

fn parse_batch_arg(mut args: impl Iterator<Item = String>) -> i64 {
    while let Some(arg) = args.next() {
        if arg == "--batch" {
            if let Some(n) = args.next() {
                return n.parse().unwrap_or(DEFAULT_BATCH);
            }
        }
    }
    DEFAULT_BATCH
}