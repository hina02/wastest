//! statements の VSS (cosine 距離) 検索動作確認。
//!
//! ```
//! cargo run --bin search_vss -- "lisp dialects used in production"
//! cargo run --bin search_vss -- "rate limiting spam" 10
//! ```
//!
//! 流れ:
//! 1. クエリテキストを Gemini で 3072 次元埋め込み
//! 2. HNSW (cosine) を使った最近傍検索
//!
//! 事前条件:
//! - statements に embedding が入っていること (smoke_pipeline 実行済み)
//! - `cargo run --bin refresh_indexes` で HNSW index が作成されていること
//!   (index がなくても動くが full scan になる)

use anyhow::{Context, Result};
use wastest::config::SETTINGS;
use wastest::{DuckDBReader, DuckDBWriter, DuckReadOps, GeminiClient, LlmProvider};

const EMBEDDING_DIMS: usize = 3072;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let mut args = std::env::args().skip(1);
    let query = args.next().context("usage: search_vss <query> [limit]")?;
    let limit: usize = args
        .next()
        .map(|s| s.parse())
        .transpose()
        .context("limit must be an integer")?
        .unwrap_or(5);

    // クエリベクトルを取得 (Gemini 3072 dim をそのまま使う、f32 化)
    let llm = GeminiClient::new().await?;
    let vecs = llm.embed_texts(vec![query.clone()]).await?;
    let q: Vec<f32> = vecs
        .into_iter()
        .next()
        .context("empty embedding")?
        .into_iter()
        .take(EMBEDDING_DIMS)
        .map(|x| x as f32)
        .collect();

    // VSS extension をロード (Reader 側にはセットアップ API がないので Writer 経由)
    let writer = DuckDBWriter::new(&SETTINGS.duckdb_path)?;
    writer.setup_vss()?;

    let reader = DuckDBReader::new(&SETTINGS.duckdb_path)?;
    let hits = reader.search_statements_vss(&q, limit)?;

    println!("--- query: {query:?}  (top {limit}, cosine similarity) ---");
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
