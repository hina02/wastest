//! statements の VSS (cosine similarity) 検索。
//!
//! ```
//! cargo run --bin search_vss -- <namespace> <query> [limit]
//! cargo run --bin search_vss -- hn "lisp dialects used in production"
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
        .context("usage: search_vss <namespace> <query> [limit]")?;
    let query = args
        .next()
        .context("usage: search_vss <namespace> <query> [limit]")?;
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
    let hits = reader.search_vss(q_vec, limit).await?;

    println!("--- query: {query:?}  namespace={namespace}  (top {limit}, VSS) ---");
    if hits.is_empty() {
        println!("(no hits)");
        return Ok(());
    }
    for (i, h) in hits.iter().enumerate() {
        println!(
            "[{i}] sim={:.4}  content_id={}",
            h.similarity, h.content_id
        );
        println!("    {}", h.statement);
        println!("    keywords: {:?}", h.keywords);
    }
    Ok(())
}
