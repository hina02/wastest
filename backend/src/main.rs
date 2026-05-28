use anyhow::Result;
use tracing_subscriber::EnvFilter;
use wastest::config::SETTINGS;
use wastest::{CrawlerState, DuckDBClient, connect};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let sqlite_pool = connect(&SETTINGS.database_url).await?;
    let sqlite_ddl = include_str!("../../ddl/sqlite.sql");
    sqlx::raw_sql(sqlite_ddl).execute(&sqlite_pool).await?;

    let duck = DuckDBClient::new(&SETTINGS.duckdb_path)?;

    let mut state = CrawlerState::new(sqlite_pool, duck).await?;
    state.run_ingest_pipeline().await?;
    Ok(())
}
