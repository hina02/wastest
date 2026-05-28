//! 検索 index (FTS + HNSW) のリフレッシュ用 bin。
//!
//! 主な用途:
//! - main pipeline とは独立に再構築したい時 (例: 別 cron, 手動 ad-hoc)
//! - pipeline がエラーで refresh まで届かなかった時のリカバリ
//! - 検索動作確認の前段
//!
//! main.rs では pipeline 末尾で `refresh_search_indexes` を呼ぶので、
//! 通常運用ではこの bin を別途叩く必要はない。
//!
//! ```
//! cargo run --bin refresh_indexes
//! ```

use anyhow::Result;
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

    let writer = DuckDBWriter::new(&SETTINGS.duckdb_path)?;
    writer.refresh_search_indexes()?;
    println!("search indexes (FTS + HNSW) refreshed");
    Ok(())
}
