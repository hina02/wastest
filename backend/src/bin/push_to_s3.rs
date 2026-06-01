//! ローカル Lance namespace を S3 へコピーする。
//!
//! ```
//! cargo run --bin push_to_s3 -- <src_namespace> <dst_namespace>
//! # 例: rust_docs → houdini
//! cargo run --bin push_to_s3 -- rust_docs houdini
//! ```
//!
//! - src: `LANCE_DIR/<src_namespace>`  (ローカル)
//! - dst: `s3://<S3_BUCKET_ID>/<dst_namespace>`
//! - 各テーブルを全件 stream して append。既存テーブルには追記される。

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use futures::TryStreamExt;
use lancedb::Connection;
use lancedb::query::ExecutableQuery;
use secrecy::ExposeSecret;
use tracing_subscriber::EnvFilter;
use wastest::config::SETTINGS;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let mut args = std::env::args().skip(1);
    let src_ns = args
        .next()
        .unwrap_or_else(|| "rust_docs".to_string());
    let dst_ns = args
        .next()
        .unwrap_or_else(|| "houdini".to_string());

    let src_uri = SETTINGS.lance_uri_for(&src_ns);
    let dst_uri = SETTINGS.s3_lance_uri_for(&dst_ns);

    println!("src: {src_uri}");
    println!("dst: {dst_uri}");

    let src = lancedb::connect(&src_uri).execute().await?;
    let dst = connect_s3(&dst_uri).await?;

    let tables = src.table_names().execute().await?;
    println!("テーブル: {tables:?}");

    for table in &tables {
        print!("  コピー中: {table} ... ");
        let n = copy_table(&src, &dst, table)
            .await
            .with_context(|| format!("copy table {table}"))?;
        println!("{n} 行");
    }

    println!("完了: {src_ns} → s3 {dst_ns}");
    Ok(())
}

async fn connect_s3(uri: &str) -> Result<Connection> {
    let s = &*SETTINGS;
    Ok(lancedb::connect(uri)
        .storage_option("aws_access_key_id", s.s3_access_key.expose_secret())
        .storage_option("aws_secret_access_key", s.s3_secret_key.expose_secret())
        .storage_option("aws_region", &s.s3_region)
        .execute()
        .await?)
}

async fn copy_table(src: &Connection, dst: &Connection, name: &str) -> Result<usize> {
    let src_tbl = src.open_table(name).execute().await?;
    let schema = src_tbl.schema().await?;

    let dst_names = dst.table_names().execute().await?;
    if !dst_names.iter().any(|n| n == name) {
        let empty = RecordBatch::new_empty(schema);
        dst.create_table(name, empty).execute().await
            .with_context(|| format!("create_table {name}"))?;
    }
    let dst_tbl = dst.open_table(name).execute().await?;

    let mut stream = src_tbl.query().execute().await?;
    let mut total = 0usize;
    while let Some(batch) = stream.try_next().await? {
        let n = batch.num_rows();
        dst_tbl.add(batch).execute().await
            .with_context(|| format!("add batch to {name}"))?;
        total += n;
    }
    Ok(total)
}
