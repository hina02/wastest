use anyhow::{Context, Result};
use wastest::db::duck::DuckDBClient;

fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let db_path =
        std::env::var("DUCKDB_PATH").context("DUCKDB_PATH が設定されていません (.env を確認)")?;
    let client = DuckDBClient::new(&db_path)?;

    println!("--- DuckDB: top_stories 履歴一覧 ---");
    let stories = client.fetch_all_stories()?;

    for (fetched_at, item_ids) in stories {
        println!(
            "取得日時: {} | 含まれる要素数: {}件",
            fetched_at,
            item_ids.len()
        );
        // 最初の5件だけサンプル表示
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
