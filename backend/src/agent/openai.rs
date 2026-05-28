//! OpenAI 実装。現状は Gemini に移行済みで使用していないが、
//! プロバイダ差し替えのリファレンスとして残す。chat = `gpt-5-nano`。
//! embedding 側は未実装 (必要になったら text-embedding-3-small 等を入れる)。

use crate::agent::prompts::EXTRACT_PROPOSITIONS;
use crate::agent::{LlmProvider, Statement, strip_code_fence};
use crate::config::SETTINGS;
use anyhow::{Result, bail};
use rig::agent::Agent;
use rig::client::CompletionClient;
use rig::completion::Prompt;
use rig::providers::openai;
use rig::providers::openai::responses_api::ResponsesCompletionModel;
use secrecy::ExposeSecret;
use serde::Deserialize;

const CHAT_MODEL: &str = "gpt-5-nano";

pub struct OpenAIClient {
    client: openai::Client,
    extract_agent: Agent<ResponsesCompletionModel>,
}

impl OpenAIClient {
    pub async fn new() -> Result<Self> {
        let client = openai::Client::builder(SETTINGS.openai_api_key.expose_secret()).build()?;
        let extract_agent = client
            .agent(CHAT_MODEL)
            .preamble(EXTRACT_PROPOSITIONS)
            .build();

        Ok(Self {
            client,
            extract_agent,
        })
    }

    pub fn client(&self) -> &openai::Client {
        &self.client
    }
}

impl LlmProvider for OpenAIClient {
    async fn extract_statement(&self, text: &str) -> Result<Vec<Statement>> {
        let response = self.extract_agent.prompt(text).await?;
        let json = strip_code_fence(&response);
        let stmts: Statements = serde_json::from_str(json)?;
        Ok(stmts.statements)
    }

    async fn embed_texts(&self, _texts: Vec<String>) -> Result<Vec<Vec<f64>>> {
        bail!("OpenAI embeddings not implemented; use GeminiClient or add text-embedding-3-small")
    }
}

#[derive(Deserialize)]
struct Statements {
    statements: Vec<Statement>,
}
