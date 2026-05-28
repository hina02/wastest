pub mod agent;
pub mod api;
pub mod config;
pub mod db;
pub mod parse;

pub use api::hn::CrawlerState;
pub use db::duck::DuckDBClient;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};

pub async fn connect(database_url: &str) -> Result<SqlitePool, sqlx::Error> {
    let pool = SqlitePoolOptions::new()
        .max_connections(10)
        .connect(database_url)
        .await?;
    Ok(pool)
}
