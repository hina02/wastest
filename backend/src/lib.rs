pub mod api;
pub mod db;

use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};

pub use api::hn::fetch_top_stories;

const DEFAULT_DATABASE_URL: &str = "sqlite:wastest.db?mode=rwc";

pub async fn connect(database_url: Option<&str>) -> Result<SqlitePool, sqlx::Error> {
    let url = database_url.unwrap_or(DEFAULT_DATABASE_URL);
    let pool = SqlitePoolOptions::new()
        .max_connections(10)
        .connect(url)
        .await?;
    init_schema(&pool).await?;
    Ok(pool)
}

async fn init_schema(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS hn_items (
            id INTEGER PRIMARY KEY,
            item_type TEXT NOT NULL,
            "by" TEXT NOT NULL,
            time INTEGER NOT NULL,
            title TEXT NOT NULL,
            url TEXT NOT NULL
        )
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}
