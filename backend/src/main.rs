use wastest::{connect, fetch_top_stories};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL").ok();
    let pool = connect(database_url.as_deref()).await?;
    fetch_top_stories(&pool).await?;
    Ok(())
}
