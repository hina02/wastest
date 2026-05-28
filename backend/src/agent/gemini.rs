use crate::agent::prompts::EXTRACT_PROPOSITIONS;
use crate::agent::{LlmProvider, Statement, strip_code_fence};
use crate::config::SETTINGS;
use anyhow::Result;
use rig::agent::Agent;
use rig::client::{CompletionClient, EmbeddingsClient};
use rig::completion::Prompt;
use rig::embeddings::EmbeddingModel as _;
use rig::providers::gemini;
use rig::providers::gemini::completion::CompletionModel;
use rig::providers::gemini::embedding::EmbeddingModel;
use secrecy::ExposeSecret;
use serde::Deserialize;

const CHAT_MODEL: &str = "gemini-3-flash-preview";
const EMBEDDING_MODEL: &str = "gemini-embedding-2";

/// `gemini-embedding-2` が返すベクトル次元数。
/// DDL の `embedding FLOAT[3072]` と統一する必要がある。
/// provider を切り替えた場合はここと DDL の両方を直す。
pub const EMBEDDING_DIMS: usize = 3072;

pub struct GeminiClient {
    client: gemini::Client,
    extract_agent: Agent<CompletionModel>,
    embedding_model: EmbeddingModel,
}

impl GeminiClient {
    pub async fn new() -> Result<Self> {
        let client = gemini::Client::new(SETTINGS.gemini_api_key.expose_secret());
        let extract_agent = client
            .agent(CHAT_MODEL)
            .preamble(EXTRACT_PROPOSITIONS)
            .temperature(0.2)
            .additional_params(serde_json::json!({ "generationConfig": {} }))
            .build();
        let embedding_model = client.embedding_model(EMBEDDING_MODEL);

        Ok(Self {
            client,
            extract_agent,
            embedding_model,
        })
    }

    pub fn client(&self) -> &gemini::Client {
        &self.client
    }
}

impl LlmProvider for GeminiClient {
    async fn extract_statement(&self, text: &str) -> Result<Vec<Statement>> {
        let response = self.extract_agent.prompt(text).await?;
        let json = strip_code_fence(&response);
        let stmts: Statements = serde_json::from_str(json)?;
        Ok(stmts.statements)
    }

    /// rig 0.21 の Gemini embedding は 1 バッチで最大 1024 件。
    /// それを超える場合は呼び出し側で chunk すること。
    async fn embed_texts(&self, texts: Vec<String>) -> Result<Vec<Vec<f64>>> {
        let embeddings = self.embedding_model.embed_texts(texts).await?;
        Ok(embeddings.into_iter().map(|e| e.vec).collect())
    }
}

#[derive(Deserialize)]
struct Statements {
    statements: Vec<Statement>,
}
