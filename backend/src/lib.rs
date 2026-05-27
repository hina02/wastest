pub mod api;
pub mod db;

use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};

pub use api::hn::CrawlerState;
pub use db::duck::DuckDBClient;

pub async fn connect(database_url: &str) -> Result<SqlitePool, sqlx::Error> {
    let pool = SqlitePoolOptions::new()
        .max_connections(10)
        .connect(database_url)
        .await?;
    Ok(pool)
}
