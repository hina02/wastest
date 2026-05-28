pub mod agent;
pub mod api;
pub mod config;
pub mod db;
pub mod parse;
pub mod pipeline;

pub use agent::LlmProvider;
pub use agent::gemini::GeminiClient;
pub use agent::openai::OpenAIClient;
pub use api::hn::{CrawlerState, run_hn_pipeline};
pub use db::duck::{
    DuckDBReader, DuckDBWriter, DuckReadOps, FtsHit, HybridHit, VssConfidence, VssHit,
};
pub use pipeline::run_pipeline_with_urls;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};

pub async fn connect(database_url: &str) -> Result<SqlitePool, sqlx::Error> {
    let pool = SqlitePoolOptions::new()
        .max_connections(10)
        .connect(database_url)
        .await?;
    Ok(pool)
}
