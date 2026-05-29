use anyhow::Result;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;
use wastest::config::SETTINGS;
use wastest::lance::LanceStore;
use wastest::{CrawlerState, GeminiClient, connect, run_hn_pipeline};

/// HN ingest 専用の namespace。物理パスは `<lance_dir>/hn/`。
const HN_NAMESPACE: &str = "hn";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let sqlite_pool = connect(&SETTINGS.database_url).await?;
    let sqlite_ddl = include_str!("../../ddl/sqlite.sql");
    sqlx::raw_sql(sqlite_ddl).execute(&sqlite_pool).await?;

    // HN 用 Lance store (contents/statements/code_blocks + top_stories)
    let store = Arc::new(LanceStore::open_hn(&SETTINGS.lance_uri_for(HN_NAMESPACE)).await?);

    // 1) Hacker News API から top stories のメタを SQLite hn_items に取り込み
    //    (top_stories スナップショットは Lance に保存)
    let mut state = CrawlerState::new(sqlite_pool, store.clone()).await?;
    state.run_ingest_pipeline().await?;

    // 2) hn_items の URL から本文を抽出し、HN namespace の Lance に statements / code_blocks を投入
    let llm = Arc::new(GeminiClient::new().await?);
    run_hn_pipeline(&state.pool, llm, store.clone()).await?;

    // 3) FTS / Vector index を構築 (初回のみ実体作業、以降は冪等)
    //    + 直前の append 分を incremental に index へ取り込む
    store.ensure_indexes().await?;
    store.optimize_indexes().await?;

    Ok(())
}
