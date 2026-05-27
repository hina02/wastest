use anyhow::Result;
use tracing_subscriber::EnvFilter;
use wastest::{CrawlerState, connect};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let database_url = std::env::var("DATABASE_URL").ok();
    let pool = connect(database_url.as_deref()).await?;
    let mut state = CrawlerState::new(pool).await?;
    state.run_ingest_pipeline().await?;
    Ok(())
}
