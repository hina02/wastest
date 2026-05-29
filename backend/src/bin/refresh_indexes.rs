//! 指定 namespace の Lance dataset に FTS + Vector index を構築 (冪等)。
//!
//! ```
//! cargo run --bin refresh_indexes -- hn
//! cargo run --bin refresh_indexes -- smoke
//! ```
//!
//! Lance は append 時に index を自動更新するので、通常運用ではこの bin を
//! 別途叩く必要はない (main pipeline 末尾で `LanceStore::ensure_indexes` が走る)。
//! Vector index (IvfPq) は学習に最低 ~256 行必要なので、データが少ない初期段階では
//! `create_index` が失敗することがあるが、その場合は warn 留めで先に進む。

use anyhow::{Context, Result};
use tracing_subscriber::EnvFilter;
use wastest::config::SETTINGS;
use wastest::lance::LanceStore;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let namespace = std::env::args()
        .nth(1)
        .context("usage: refresh_indexes <namespace>")?;
    let uri = SETTINGS.lance_uri_for(&namespace);

    let store = LanceStore::open(&uri).await?;
    store.ensure_indexes().await?;
    store.optimize_indexes().await?;
    println!("indexes ensured & optimized for namespace={namespace} (uri={uri})");
    Ok(())
}
