//! FTS (BM25) と VSS (cosine) を Reciprocal Rank Fusion (RRF) でマージした検索。
//!
//! ```
//! cargo run --bin search_hybrid -- "lisp dialects used in production"
//! cargo run --bin search_hybrid -- "rate limiting" 10
//! ```
//!
//! 流れ:
//! 1. クエリテキスト → Gemini で 3072 次元 embedding 取得
//! 2. FTS 上位 50 件 と VSS 上位 50 件 (hard 閾値内) を取得
//! 3. statement_id をキーに RRF (k=60) でスコア合算 → 降順 `limit` 件
//!
//! 事前条件:
//! - smoke_pipeline で statements に embedding が入っていること
//! - `refresh_indexes` (FTS + HNSW) を実行済みであること

use anyhow::{Context, Result};
use wastest::config::SETTINGS;
use wastest::{DuckDBReader, DuckDBWriter, DuckReadOps, GeminiClient, LlmProvider};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let mut args = std::env::args().skip(1);
    let query = args
        .next()
        .context("usage: search_hybrid <query> [limit]")?;
    let limit: usize = args
        .next()
        .map(|s| s.parse())
        .transpose()
        .context("limit must be an integer")?
        .unwrap_or(5);

    let llm = GeminiClient::new().await?;
    let vecs = llm.embed_texts(vec![query.clone()]).await?;
    let q_vec: Vec<f32> = vecs
        .into_iter()
        .next()
        .context("empty embedding")?
        .into_iter()
        .map(|x| x as f32)
        .collect();

    // VSS extension をロード (Reader は持っていないので Writer 経由)
    let writer = DuckDBWriter::new(&SETTINGS.duckdb_path)?;
    writer.setup_vss()?;
    writer.setup_fts()?;

    let reader = DuckDBReader::new(&SETTINGS.duckdb_path)?;
    let hits = reader.search_hybrid(&query, &q_vec, limit)?;

    println!("--- query: {query:?}  (top {limit}, RRF) ---");
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
