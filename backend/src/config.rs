use secrecy::SecretString;
use serde::Deserialize;
use std::sync::LazyLock;

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub database_url: String,
    /// namespace ごとに DuckDB ファイルを置くディレクトリ。
    /// 各 namespace の物理パスは `{duckdb_dir}/{namespace}.duckdb`。
    pub duckdb_dir: String,
    pub openai_api_key: SecretString,
    pub gemini_api_key: SecretString,
    pub s3_bucket_id: String,
    pub s3_access_key: SecretString,
    pub s3_secret_key: SecretString,
}

impl Settings {
    pub fn load() -> anyhow::Result<Self> {
        dotenvy::dotenv().ok();
        Ok(envy::from_env()?)
    }

    /// 指定 namespace の DuckDB ファイルパスを返す。
    pub fn duckdb_path_for(&self, namespace: &str) -> String {
        format!("{}/{}.duckdb", self.duckdb_dir, namespace)
    }
}

pub static SETTINGS: LazyLock<Settings> =
    LazyLock::new(|| Settings::load().expect("failed to load settings"));
