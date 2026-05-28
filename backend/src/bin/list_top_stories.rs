//! HN namespace の top_stories 履歴を表示。

use anyhow::Result;
use wastest::config::SETTINGS;
use wastest::{DuckDBReader, DuckReadOps};

const HN_NAMESPACE: &str = "hn";

fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let db_path = SETTINGS.duckdb_path_for(HN_NAMESPACE);
    let client = DuckDBReader::new(&db_path)?;

    println!("--- DuckDB: top_stories 履歴一覧 (namespace=hn) ---");
    let stories = client.fetch_all_stories()?;

    for (fetched_at, item_ids) in stories {
        println!(
            "取得日時: {} | 含まれる要素数: {}件",
            fetched_at,
            item_ids.len()
        );
        let sample = item_ids
            .iter()
            .take(5)
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        println!("  -> 先頭データ(サンプル): [{}...]", sample);
    }

    Ok(())
}
