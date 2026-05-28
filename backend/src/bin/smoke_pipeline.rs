//! Stage1 → Splitter → Stage2 → Writer Actor の動作確認。
//!
//! 使い方:
//! ```
//! cargo run --bin smoke_pipeline
//! cargo run --bin smoke_pipeline -- https://example.com/a https://example.com/b
//! ```
//!
//! 流れ:
//! 1. テスト用 ID (9_000_000_001..) で URL を組み立てる
//! 2. hn_contents / code_blocks / statements から該当 ID を一度削除 (再実行可能に)
//! 3. `run_pipeline_with_urls` で実行
//! 4. 投入された行数を SELECT して表示

use anyhow::Result;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;
use wastest::config::SETTINGS;
use wastest::pipeline::run_pipeline_with_urls;
use wastest::{DuckDBWriter, GeminiClient};

const TEST_ID_BASE: i64 = 9_000_000_001;

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
            (TEST_ID_BASE + 1, "https://en.wikipedia.org/wiki/Hacker_News".to_string()),
        ]
    } else {
        args.into_iter()
            .enumerate()
            .map(|(i, url)| (TEST_ID_BASE + i as i64, url))
            .collect()
    };

    println!("--- 対象 URL ---");
    for (id, url) in &urls {
        println!("  [{id}] {url}");
    }

    let writer = DuckDBWriter::new(&SETTINGS.duckdb_path)?;
    let probe_conn = writer.try_clone_conn()?;

    cleanup(&probe_conn, &urls)?;

    let client = Arc::new(GeminiClient::new().await?);
    run_pipeline_with_urls(urls.clone(), client, &writer).await?;

    report(&probe_conn, &urls)?;
    Ok(())
}

fn ids_csv(urls: &[(i64, String)]) -> String {
    urls.iter()
        .map(|(id, _)| id.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn cleanup(conn: &duckdb::Connection, urls: &[(i64, String)]) -> Result<()> {
    let ids = ids_csv(urls);
    conn.execute_batch(&format!(
        "DELETE FROM statements WHERE content_id IN ({ids});
         DELETE FROM code_blocks WHERE content_id IN ({ids});
         DELETE FROM hn_contents WHERE id IN ({ids});"
    ))?;
    println!("--- 既存テストデータ削除完了 ---");
    Ok(())
}

fn report(conn: &duckdb::Connection, urls: &[(i64, String)]) -> Result<()> {
    let ids = ids_csv(urls);
    let content_count: i64 = conn.query_row(
        &format!("SELECT COUNT(*) FROM hn_contents WHERE id IN ({ids})"),
        [],
        |row| row.get(0),
    )?;
    let stmt_count: i64 = conn.query_row(
        &format!("SELECT COUNT(*) FROM statements WHERE content_id IN ({ids})"),
        [],
        |row| row.get(0),
    )?;
    let code_count: i64 = conn.query_row(
        &format!("SELECT COUNT(*) FROM code_blocks WHERE content_id IN ({ids})"),
        [],
        |row| row.get(0),
    )?;

    println!("\n--- 投入結果 (対象 ID のみ) ---");
    println!("  hn_contents: {content_count}");
    println!("  code_blocks: {code_count}");
    println!("  statements:  {stmt_count}");

    if content_count > 0 {
        println!("\n--- statements サンプル (先頭3件) ---");
        let mut stmt = conn.prepare(&format!(
            "SELECT content_id, statement, keywords
             FROM statements WHERE content_id IN ({ids})
             LIMIT 3"
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
