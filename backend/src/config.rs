use secrecy::SecretString;
use serde::Deserialize;
use std::sync::LazyLock;

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub database_url: String,
    pub duckdb_path: String,
    pub openai_api_key: SecretString,
    pub s3_bucket_id: String,
    pub s3_access_key: SecretString,
    pub s3_secret_key: SecretString,
}

impl Settings {
    pub fn load() -> anyhow::Result<Self> {
        dotenvy::dotenv().ok();
        Ok(envy::from_env()?)
    }
}

pub static SETTINGS: LazyLock<Settings> =
    LazyLock::new(|| Settings::load().expect("failed to load settings"));
