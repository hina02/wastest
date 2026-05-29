//! FTS + VSS のハイブリッド検索 (lancedb の execute_hybrid)。
//!
//! ```
//! cargo run --bin search_hybrid -- <namespace> <query> [limit]
//! cargo run --bin search_hybrid -- hn "lisp dialects"
//! ```

use anyhow::{Context, Result};
use wastest::config::SETTINGS;
use wastest::{GeminiClient, LanceReader, LlmProvider};

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
    let limit: usize = args.next().map(|s| s.parse()).transpose()?.unwrap_or(5);

    let llm = GeminiClient::new().await?;
    let vecs = llm.embed_texts(vec![query.clone()]).await?;
    let q_vec: Vec<f32> = vecs
        .into_iter()
        .next()
        .context("empty embedding")?
        .into_iter()
        .map(|x| x as f32)
        .collect();

    let reader = LanceReader::open(&SETTINGS.lance_uri_for(&namespace)).await?;
    let hits = reader.search_hybrid(&query, q_vec, limit).await?;

    println!("--- query: {query:?}  namespace={namespace}  (top {limit}, hybrid) ---");
    if hits.is_empty() {
        println!("(no hits)");
        return Ok(());
    }
    for (i, h) in hits.iter().enumerate() {
        let fts = h
            .fts_score
            .map(|s| format!("FTS={s:.3}"))
            .unwrap_or_else(|| "FTS:-".into());
        let vss = h
            .vss_similarity
            .map(|s| format!("VSS(sim={s:.3})"))
            .unwrap_or_else(|| "VSS:-".into());
        println!(
            "[{i}] rel={:.4}  {fts}  {vss}  content_id={}",
            h.relevance_score, h.content_id
        );
        println!("    {}", h.statement);
        println!("    keywords: {:?}", h.keywords);
    }
    Ok(())
}
