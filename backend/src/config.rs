use secrecy::SecretString;
use serde::Deserialize;
use std::sync::LazyLock;

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub database_url: String,
    /// namespace ごとに Lance dataset ディレクトリ群を置く親ディレクトリ。
    /// 各 namespace のレイアウトは
    /// `{lance_dir}/{namespace}/{contents,statements,code_blocks}.lance/`。
    pub lance_dir: String,
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

    /// 指定 namespace の Lance dataset 親ディレクトリ (lancedb::connect の uri)。
    /// この下に `contents.lance/`, `statements.lance/`, `code_blocks.lance/` が並ぶ。
    pub fn lance_uri_for(&self, namespace: &str) -> String {
        format!("{}/{}", self.lance_dir, namespace)
    }
}

pub static SETTINGS: LazyLock<Settings> =
    LazyLock::new(|| Settings::load().expect("failed to load settings"));
