use anyhow::Result;
use wastest::agent::extract::OpenAIClient;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let client = OpenAIClient::new().await?;
    client.chat("hello").await?;

    Ok(())
}
