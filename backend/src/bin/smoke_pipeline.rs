//! Stage1 → Splitter → Stage2 → Writer Actor の動作確認 (Lance バックエンド版)。
//! namespace = "smoke" の Lance dataset 群 (`<lance_dir>/smoke/`) を使うので
//! 本番データ ("hn/" 等) と混ざらない。
//!
//! ```
//! cargo run --bin smoke_pipeline
//! cargo run --bin smoke_pipeline -- https://example.com/a https://example.com/b
//! ```

use anyhow::Result;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;
use wastest::config::SETTINGS;
use wastest::lance::LanceStore;
use wastest::pipeline::run_pipeline_with_urls;
use wastest::{GeminiClient, LanceReader};

const TEST_ID_BASE: i64 = 9_000_000_001;
const NAMESPACE: &str = "smoke";

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args: Vec<String> = std::env::args().skip(1).collect();
    let urls: Vec<(i64, String)> = if args.is_empty() {
        vec![
            (TEST_ID_BASE, "https://en.wikipedia.org/wiki/DuckDB".to_string()),
            (
                TEST_ID_BASE + 1,
                "https://en.wikipedia.org/wiki/Hacker_News".to_string(),
            ),
        ]
    } else {
        args.into_iter()
            .enumerate()
            .map(|(i, url)| (TEST_ID_BASE + i as i64, url))
            .collect()
    };

    let uri = SETTINGS.lance_uri_for(NAMESPACE);
    println!("--- 対象 URL (namespace={NAMESPACE}, uri={uri}) ---");
    for (id, url) in &urls {
        println!("  [{id}] {url}");
    }

    // smoke では再現性のために dataset を毎回作り直す
    if std::path::Path::new(&uri).exists() {
        std::fs::remove_dir_all(&uri)?;
        println!("--- 既存テストデータ削除完了 ---");
    }

    let store = Arc::new(LanceStore::open(&uri).await?);

    let client = Arc::new(GeminiClient::new().await?);
    run_pipeline_with_urls(urls, client, store.clone()).await?;

    // 集計表示は DuckDB-on-Lance で
    let reader = LanceReader::open(&uri).await?;
    let contents = reader.count("contents")?;
    let codes = reader.count("code_blocks")?;
    let stmts = reader.count("statements")?;
    println!("\n--- 投入結果 ---");
    println!("  contents:    {contents}");
    println!("  code_blocks: {codes}");
    println!("  statements:  {stmts}");

    if contents > 0 {
        println!("\n--- statements サンプル (先頭3件) ---");
        let path = reader.statements_path();
        let mut stmt = reader.duck().prepare(&format!(
            "SELECT content_id, statement, keywords FROM '{path}' LIMIT 3"
        ))?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let cid: i64 = row.get(0)?;
            let s: String = row.get(1)?;
            let kw: duckdb::types::Value = row.get(2)?;
            let kw_str = match kw {
                duckdb::types::Value::List(v) => v
                    .into_iter()
                    .filter_map(|x| {
                        if let duckdb::types::Value::Text(t) = x {
                            Some(t)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", "),
                _ => String::new(),
            };
            println!("  [{cid}] {s}");
            println!("    keywords: [{kw_str}]");
        }
    }

    Ok(())
}
