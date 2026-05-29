//! HN namespace の top_stories 履歴を表示 (Lance + DuckDB read-only)。

use anyhow::Result;
use wastest::LanceReader;
use wastest::config::SETTINGS;

const HN_NAMESPACE: &str = "hn";

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let uri = SETTINGS.lance_uri_for(HN_NAMESPACE);
    let reader = LanceReader::open(&uri).await?;

    println!("--- Lance: top_stories 履歴一覧 (namespace=hn, uri={uri}) ---");
    let stories = reader.fetch_all_top_stories().await?;

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
