//! 指定 namespace の DuckDB ファイルで FTS + HNSW を再構築。
//!
//! ```
//! cargo run --bin refresh_indexes -- hn
//! cargo run --bin refresh_indexes -- smoke
//! ```
//!
//! 主な用途:
//! - main pipeline と独立して再構築したい時 (例: 別 cron, 手動 ad-hoc)
//! - pipeline がエラーで refresh まで届かなかった時のリカバリ
//! - 検索動作確認の前段

use anyhow::{Context, Result};
use tracing_subscriber::EnvFilter;
use wastest::DuckDBWriter;
use wastest::config::SETTINGS;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let namespace = std::env::args()
        .nth(1)
        .context("usage: refresh_indexes <namespace>")?;
    let db_path = SETTINGS.duckdb_path_for(&namespace);

    let writer = DuckDBWriter::new(&db_path)?;
    writer.refresh_search_indexes()?;
    println!("search indexes (FTS + HNSW) refreshed for namespace={namespace}");
    Ok(())
}
