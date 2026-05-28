//! FTS index 単独リフレッシュ用 bin。
//!
//! 主な用途:
//! - main pipeline とは独立に「FTS だけ再構築」したい時 (例: 別 cron, 手動 ad-hoc)
//! - pipeline がエラーで FTS 再構築まで届かなかった時のリカバリ
//! - 検索動作確認の前段に走らせる
//!
//! main.rs では引き続き pipeline 末尾で `refresh_fts_indexes` を呼ぶので、
//! 通常運用ではこの bin を別途叩く必要はない。
//!
//! HNSW (VSS) も同様に分離したくなったら、本ファイルに足すか
//! `refresh_vss_indexes.rs` として別 bin にする。
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
    writer.refresh_fts_indexes()?;
    println!("FTS indexes refreshed");
    Ok(())
}
