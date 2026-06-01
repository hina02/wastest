use secrecy::SecretString;
use serde::Deserialize;
use std::sync::LazyLock;

fn default_s3_region() -> String {
    "ap-northeast-1".to_string()
}

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
    #[serde(default = "default_s3_region")]
    pub s3_region: String,
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

    /// S3 上の指定 namespace の Lance dataset URI。
    pub fn s3_lance_uri_for(&self, namespace: &str) -> String {
        format!("s3://{}/{}", self.s3_bucket_id, namespace)
    }
}

pub static SETTINGS: LazyLock<Settings> =
    LazyLock::new(|| Settings::load().expect("failed to load settings"));
