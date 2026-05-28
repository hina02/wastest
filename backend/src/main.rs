use anyhow::Result;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;
use wastest::config::SETTINGS;
use wastest::{CrawlerState, DuckDBWriter, GeminiClient, connect, run_hn_pipeline};

/// HN ingest 専用の namespace。物理ファイルは `<duckdb_dir>/hn.duckdb`。
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

    let duck = DuckDBWriter::new(&SETTINGS.duckdb_path_for(HN_NAMESPACE))?;

    // 1) Hacker News API から top stories のメタを SQLite hn_items に取り込み
    let mut state = CrawlerState::new(sqlite_pool, duck).await?;
    state.run_ingest_pipeline().await?;

    // 2) hn_items の URL から本文を抽出し、HN namespace の DuckDB に statements / code_blocks を投入
    let llm = Arc::new(GeminiClient::new().await?);
    run_hn_pipeline(&state.pool, llm, &state.duck).await?;

    // 3) 検索 index リフレッシュ:
    //    - FTS: snapshot 型なので PRAGMA で毎回再構築
    //    - HNSW: IF NOT EXISTS の冪等作成 (一度作れば INSERT 自動追従)
    state.duck.refresh_search_indexes()?;

    Ok(())
}
