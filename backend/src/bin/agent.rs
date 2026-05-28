//! LLM クライアント単体の動作確認。
//! `cargo run --bin agent` で固定テキストに対する extract_statement を呼ぶ。

use anyhow::Result;
use wastest::{GeminiClient, LlmProvider};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let client = GeminiClient::new().await?;
    let stmts = client
        .extract_statement(
            "DuckDB is an in-process SQL OLAP DBMS. \
             It loads CSV at ~500 MB/s on M2 Pro from local SSD.",
        )
        .await?;
    for s in &stmts {
        println!("- {}\n  keywords: {:?}", s.statement, s.keywords);
    }
    Ok(())
}
