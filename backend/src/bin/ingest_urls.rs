//! 汎用 ingest: 指定 namespace の url_events テーブルに URL を登録する。
//!
//! ```
//! cargo run --bin ingest_urls -- <namespace> <url> [url ...]
//! cargo run --bin ingest_urls -- my_docs \
//!   https://example.com/article1 \
//!   https://example.com/article2
//! ```
//!
//! URL を pending として登録するだけで、実際の処理は `process_events` が行う。
//! 既に登録済みの URL はスキップされる (INSERT OR IGNORE)。

use anyhow::{Context, Result};
use tracing_subscriber::EnvFilter;
use wastest::config::SETTINGS;
use wastest::connect;
use wastest::db::events::{UrlEvent, enqueue_many};

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

    let pool = connect(&SETTINGS.database_url).await?;
    let sqlite_ddl = include_str!("../../../ddl/sqlite.sql");
    sqlx::raw_sql(sqlite_ddl).execute(&pool).await?;

    let events: Vec<UrlEvent> = urls_raw
        .into_iter()
        .map(|url| {
            let id = id_from_url(&url);
            UrlEvent { id, url, namespace: namespace.clone() }
        })
        .collect();

    let count = events.len();
    enqueue_many(&pool, &events).await?;
    println!("enqueued {count} URL(s) into namespace={namespace}");
    println!("run `cargo run --bin process_events -- {namespace}` to process.");
    Ok(())
}

/// URL を FNV-1a 64bit ハッシュ → i64。決定的なので再実行で同じ ID。
fn id_from_url(url: &str) -> i64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in url.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h as i64
}