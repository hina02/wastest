// backend/src/bin/archive_hn_items.rs
use anyhow::{Context, Result};
use wastest::DuckDBWriter;

fn main() -> Result<()> {
    // 1. 環境コンテキストの初期化
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();

    let db_path =
        std::env::var("DUCKDB_PATH").context("DUCKDB_PATH が設定されていません (.env を確認)")?;
    let s3_access_key = std::env::var("S3_ACCESS_KEY")
        .context("S3_ACCESS_KEY が設定されていません (.env を確認)")?;
    let s3_secret_key = std::env::var("S3_SECRET_KEY")
        .context("S3_SECRET_KEY が設定されていません (.env を確認)")?;
    let bucket_id =
        std::env::var("S3_BUCKET_ID").context("S3_BUCKET_ID が設定されていません (.env を確認)")?;

    // 2. クライアント初期化とS3環境セットアップ
    let client = DuckDBWriter::new(&db_path)?;
    client.setup_s3_environment(&s3_access_key, &s3_secret_key)?;

    println!("--- Task 2: S3へのHiveパーティション形式での日次アーカイブ ---");
    println!(
        "SQLiteのデータを S3バケット ({}) へエクスポート中...",
        bucket_id
    );

    // コアロジック呼び出し (Write)
    client.export_to_s3_lake()?;
    println!("✅ S3へのParquet出力が完了しました。\n");

    println!("--- S3の全アーカイブからのクエリ (最新状態の動的解決) ---");

    // コアロジック呼び出し (Read)
    let latest_items = client.query_latest_from_s3_lake(&bucket_id, 5)?;

    println!("▼ 重複排除された最新の5件 (S3データレイク全体からクエリ):");
    for (id, title, time, archive_date) in latest_items {
        println!(
            "[{}] {} (timestamp: {}, S3アーカイブ日: {})",
            id, title, time, archive_date
        );
    }

    Ok(())
}
