//! statements の VSS (cosine similarity) 検索動作確認。
//!
//! ```
//! cargo run --bin search_vss -- <namespace> <query> [limit]
//! cargo run --bin search_vss -- hn "lisp dialects used in production"
//! cargo run --bin search_vss -- smoke "rate limiting" 10
//! ```
//!
//! 1番目の引数の namespace で対応する DuckDB ファイル
//! (`<duckdb_dir>/<namespace>.duckdb`) を開いて検索する。

use anyhow::{Context, Result};
use wastest::config::SETTINGS;
use wastest::{DuckDBReader, DuckDBWriter, DuckReadOps, GeminiClient, LlmProvider};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let mut args = std::env::args().skip(1);
    let namespace = args
        .next()
        .context("usage: search_vss <namespace> <query> [limit]")?;
    let query = args
        .next()
        .context("usage: search_vss <namespace> <query> [limit]")?;
    let limit: usize = args
        .next()
        .map(|s| s.parse())
        .transpose()
        .context("limit must be an integer")?
        .unwrap_or(5);

    let db_path = SETTINGS.duckdb_path_for(&namespace);

    // クエリベクトル (Gemini 3072 dim, f32 化)
    let llm = GeminiClient::new().await?;
    let vecs = llm.embed_texts(vec![query.clone()]).await?;
    let q: Vec<f32> = vecs
        .into_iter()
        .next()
        .context("empty embedding")?
        .into_iter()
        .map(|x| x as f32)
        .collect();

    // VSS extension をロード
    let writer = DuckDBWriter::new(&db_path)?;
    writer.setup_vss()?;

    let reader = DuckDBReader::new(&db_path)?;
    let hits = reader.search_statements_vss(&q, limit)?;

    println!("--- query: {query:?}  namespace={namespace}  (top {limit}, cosine similarity) ---");
    if hits.is_empty() {
        println!("(no hits — similarity threshold filtered everything)");
        return Ok(());
    }
    for (i, h) in hits.iter().enumerate() {
        let label = match h.confidence {
            wastest::VssConfidence::Confident => "CONFIDENT",
            wastest::VssConfidence::Marginal => "MARGINAL",
        };
        println!(
            "[{i}] sim={:.4}  [{label}]  content_id={}",
            h.similarity, h.content_id
        );
        println!("    {}", h.statement);
        println!("    keywords: {:?}", h.keywords);
    }
    Ok(())
}
