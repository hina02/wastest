use anyhow::Result;
use wastest::{GeminiClient, LlmProvider};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let client = GeminiClient::new().await?;
    let vecs = client
        .embed_texts(vec![
            "DuckDB is an in-process OLAP DBMS.".to_string(),
            "Hacker News is implemented in Arc.".to_string(),
        ])
        .await?;
    for (i, v) in vecs.iter().enumerate() {
        println!(
            "[{i}] ndims={}  head[0..4]={:?}",
            v.len(),
            &v[..4.min(v.len())]
        );
    }
    Ok(())
}
