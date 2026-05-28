//! FTS (BM25) と VSS (cosine) を Reciprocal Rank Fusion (RRF) でマージした検索。
//!
//! ```
//! cargo run --bin search_hybrid -- <namespace> <query> [limit]
//! cargo run --bin search_hybrid -- hn "lisp dialects"
//! cargo run --bin search_hybrid -- smoke "rate limiting" 10
//! ```

use anyhow::{Context, Result};
use wastest::config::SETTINGS;
use wastest::{DuckDBReader, DuckDBWriter, DuckReadOps, GeminiClient, LlmProvider};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let mut args = std::env::args().skip(1);
    let namespace = args
        .next()
        .context("usage: search_hybrid <namespace> <query> [limit]")?;
    let query = args
        .next()
        .context("usage: search_hybrid <namespace> <query> [limit]")?;
    let limit: usize = args
        .next()
        .map(|s| s.parse())
        .transpose()
        .context("limit must be an integer")?
        .unwrap_or(5);

    let db_path = SETTINGS.duckdb_path_for(&namespace);

    let llm = GeminiClient::new().await?;
    let vecs = llm.embed_texts(vec![query.clone()]).await?;
    let q_vec: Vec<f32> = vecs
        .into_iter()
        .next()
        .context("empty embedding")?
        .into_iter()
        .map(|x| x as f32)
        .collect();

    let writer = DuckDBWriter::new(&db_path)?;
    writer.setup_vss()?;
    writer.setup_fts()?;

    let reader = DuckDBReader::new(&db_path)?;
    let hits = reader.search_hybrid(&query, &q_vec, limit)?;

    println!("--- query: {query:?}  namespace={namespace}  (top {limit}, RRF) ---");
    if hits.is_empty() {
        println!("(no hits)");
        return Ok(());
    }
    for (i, h) in hits.iter().enumerate() {
        let fts = h
            .fts_rank
            .map(|r| format!("FTS#{r}"))
            .unwrap_or_else(|| "FTS:-".into());
        let vss = h
            .vss_rank
            .map(|r| {
                let s = h.vss_similarity.unwrap_or(f64::NAN);
                format!("VSS#{r}(sim={s:.3})")
            })
            .unwrap_or_else(|| "VSS:-".into());
        println!(
            "[{i}] rrf={:.4}  {fts}  {vss}  content_id={}",
            h.rrf_score, h.content_id
        );
        println!("    {}", h.statement);
        println!("    keywords: {:?}", h.keywords);
    }
    Ok(())
}
