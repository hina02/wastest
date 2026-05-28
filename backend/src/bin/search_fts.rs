//! statements の FTS (BM25) 検索動作確認。
//!
//! ```
//! cargo run --bin search_fts -- <namespace> <query> [limit]
//! cargo run --bin search_fts -- hn "karma"
//! cargo run --bin search_fts -- smoke "lisp" 10
//! ```
//!
//! 1番目の引数の namespace で対応する DuckDB ファイル
//! (`<duckdb_dir>/<namespace>.duckdb`) を開いて検索する。

use anyhow::{Context, Result};
use wastest::config::SETTINGS;
use wastest::{DuckDBReader, DuckDBWriter, DuckReadOps};

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
    let limit: usize = args
        .next()
        .map(|s| s.parse())
        .transpose()
        .context("limit must be an integer")?
        .unwrap_or(5);

    let db_path = SETTINGS.duckdb_path_for(&namespace);

    // FTS extension をロード (Reader に setup API がないので Writer 経由)
    let writer = DuckDBWriter::new(&db_path)?;
    writer.setup_fts()?;

    let reader = DuckDBReader::new(&db_path)?;
    let hits = reader.search_statements_fts(&query, limit)?;

    println!("--- query: {query:?}  namespace={namespace}  (top {limit}) ---");
    if hits.is_empty() {
        println!("(no hits)");
        return Ok(());
    }
    for (i, h) in hits.iter().enumerate() {
        println!("[{i}] score={:.3}  content_id={}", h.score, h.content_id);
        println!("    {}", h.statement);
        println!("    keywords: {:?}", h.keywords);
    }
    Ok(())
}
