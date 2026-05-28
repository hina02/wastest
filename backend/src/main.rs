use anyhow::Result;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;
use wastest::config::SETTINGS;
use wastest::{CrawlerState, DuckDBWriter, GeminiClient, connect, run_pipeline};

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

    let duck = DuckDBWriter::new(&SETTINGS.duckdb_path)?;

    // 1) Hacker News API から top stories のメタを SQLite hn_items に取り込み
    let mut state = CrawlerState::new(sqlite_pool, duck).await?;
    state.run_ingest_pipeline().await?;

    // 2) hn_items の URL から本文を抽出し、statements / code_blocks を DuckDB に投入
    let llm = Arc::new(GeminiClient::new().await?);
    run_pipeline(&state.pool, llm, &state.duck).await?;

    // 3) FTS index をリフレッシュ (DuckDB FTS は snapshot 型なので毎回 PRAGMA で再構築)
    //    VSS / HNSW は embedding 実装後に refresh_search_indexes に統合する
    state.duck.refresh_fts_indexes()?;

    Ok(())
}
