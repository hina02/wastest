//! statements の FTS (BM25) 検索。
//!
//! ```
//! cargo run --bin search_fts -- <namespace> <query> [limit]
//! cargo run --bin search_fts -- hn "karma"
//! ```

use anyhow::{Context, Result};
use wastest::LanceReader;
use wastest::config::SETTINGS;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let mut args = std::env::args().skip(1);
    let namespace = args
        .next()
        .context("usage: search_fts <namespace> <query> [limit]")?;
    let query = args
        .next()
        .context("usage: search_fts <namespace> <query> [limit]")?;
    let limit: usize = args.next().map(|s| s.parse()).transpose()?.unwrap_or(5);

    let reader = LanceReader::open(&SETTINGS.lance_uri_for(&namespace)).await?;
    let hits = reader.search_fts(&query, limit).await?;

    println!("--- query: {query:?}  namespace={namespace}  (top {limit}, FTS) ---");
    if hits.is_empty() {
        println!("(no hits)");
        return Ok(());
    }
    for (i, h) in hits.iter().enumerate() {
        println!("[{i}] score={:.4}  content_id={}", h.score, h.content_id);
        println!("    {}", h.statement);
        println!("    keywords: {:?}", h.keywords);
    }
    Ok(())
}
