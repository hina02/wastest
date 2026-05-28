//! statements の FTS (BM25) 検索動作確認。
//!
//! ```
//! cargo run --bin search_fts -- "duckdb"
//! cargo run --bin search_fts -- "karma reputation" 10
//! ```
//!
//! 事前条件:
//! - `cargo run --bin smoke_pipeline` 等で statements にデータが入っていること
//! - `cargo run --bin refresh_indexes` で FTS index が再構築されていること
//!   (または main.rs 経由で pipeline 末尾の refresh_fts_indexes が走っていること)

use anyhow::{Context, Result};
use wastest::config::SETTINGS;
use wastest::{DuckDBReader, DuckReadOps};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let mut args = std::env::args().skip(1);
    let query = args
        .next()
        .context("usage: search_fts <query> [limit]")?;
    let limit: usize = args
        .next()
        .map(|s| s.parse())
        .transpose()
        .context("limit must be an integer")?
        .unwrap_or(5);

    let reader = DuckDBReader::new(&SETTINGS.duckdb_path)?;
    // FTS extension は別 connection の状態を引き継がないので、検索前にロードする。
    // Reader 側に setup_fts はないので、必要なら DuckDBWriter で setup する設計。
    // 本 bin は単発検証なので writer 経由で extension をロード。
    let writer = wastest::DuckDBWriter::new(&SETTINGS.duckdb_path)?;
    writer.setup_fts()?;

    let hits = reader.search_statements_fts(&query, limit)?;

    println!("--- query: {query:?}  (top {limit}) ---");
    if hits.is_empty() {
        println!("(no hits)");
        return Ok(());
    }
    for (i, h) in hits.iter().enumerate() {
        println!(
            "[{i}] score={:.3}  content_id={}",
            h.score, h.content_id
        );
        println!("    {}", h.statement);
        println!("    keywords: {:?}", h.keywords);
    }
    Ok(())
}
