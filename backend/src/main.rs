use anyhow::{Context, Result};
use tracing_subscriber::EnvFilter;
use wastest::{CrawlerState, DuckDBClient, connect};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let database_url = std::env::var("DATABASE_URL")
        .context("DATABASE_URL が設定されていません (.env を確認)")?;
    let duckdb_path =
        std::env::var("DUCKDB_PATH").context("DUCKDB_PATH が設定されていません (.env を確認)")?;

    let sqlite_pool = connect(&database_url).await?;
    let sqlite_ddl = include_str!("../../ddl/sqlite.sql");
    sqlx::raw_sql(sqlite_ddl).execute(&sqlite_pool).await?;

    let duck = DuckDBClient::new(&duckdb_path)?;

    let mut state = CrawlerState::new(sqlite_pool, duck).await?;
    state.run_ingest_pipeline().await?;
    Ok(())
}
