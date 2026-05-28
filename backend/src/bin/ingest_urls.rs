//! 汎用 ingest: 指定 namespace の DuckDB ファイルに任意 URL 列を取り込む。
//! HN 以外のドメイン知識を蓄積する用途。
//!
//! ```
//! cargo run --bin ingest_urls -- <namespace> <url> [url ...]
//! cargo run --bin ingest_urls -- my_docs \
//!   https://example.com/article1 \
//!   https://example.com/article2
//! ```
//!
//! 1番目の引数の namespace で対応する DuckDB ファイル
//! (`<duckdb_dir>/<namespace>.duckdb`) を開いて書き込む。
//!
//! ID は URL の FNV-1a ハッシュで決定的に決める。同じ URL を再実行すると同じ ID になり、
//! `existing_content_ids` で skip される。
//!
//! 取り込んだ後は `cargo run --bin refresh_indexes -- <namespace>` で FTS / HNSW を更新。

use anyhow::{Context, Result};
use std::sync::Arc;
use tracing_subscriber::EnvFilter;
use wastest::config::SETTINGS;
use wastest::pipeline::run_pipeline_with_urls;
use wastest::{DuckDBWriter, DuckReadOps, GeminiClient};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let mut args = std::env::args().skip(1);
    let namespace = args
        .next()
        .context("usage: ingest_urls <namespace> <url> [url ...]")?;
    let urls_raw: Vec<String> = args.collect();
    if urls_raw.is_empty() {
        anyhow::bail!("at least one URL is required");
    }

    let db_path = SETTINGS.duckdb_path_for(&namespace);
    let writer = DuckDBWriter::new(&db_path)?;
    let existing = writer.existing_content_ids()?;

    let urls: Vec<(i64, String)> = urls_raw
        .into_iter()
        .map(|u| (id_from_url(&u), u))
        .filter(|(id, url)| {
            if existing.contains(id) {
                println!("skip (already ingested): [{id}] {url}");
                false
            } else {
                true
            }
        })
        .collect();

    if urls.is_empty() {
        println!("nothing to ingest");
        return Ok(());
    }

    println!(
        "--- ingesting {} URL(s) into namespace={namespace} (file={db_path}) ---",
        urls.len()
    );
    for (id, url) in &urls {
        println!("  [{id}] {url}");
    }

    let client = Arc::new(GeminiClient::new().await?);
    run_pipeline_with_urls(urls, client, &writer).await?;
    println!(
        "done. run `cargo run --bin refresh_indexes -- {namespace}` to refresh FTS/HNSW."
    );
    Ok(())
}

/// URL を FNV-1a 64bit ハッシュ → i64 に変換。決定的なので再実行で同じ ID になる。
fn id_from_url(url: &str) -> i64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in url.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h as i64
}
