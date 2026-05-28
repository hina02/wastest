use crate::config::SETTINGS;
use anyhow::Result;
use rig::agent::Agent;
use rig::client::CompletionClient;
use rig::completion::Prompt;
use rig::providers::openai;
use rig::providers::openai::responses_api::ResponsesCompletionModel;
use secrecy::ExposeSecret;
use serde::Deserialize;

pub struct OpenAIClient {
    client: openai::Client,
    extract_agent: Agent<ResponsesCompletionModel>,
}

impl OpenAIClient {
    pub async fn new() -> Result<Self> {
        let client = openai::Client::builder(SETTINGS.openai_api_key.expose_secret()).build()?;
        let extract_agent = client
            .agent("gpt-5-nano")
            .preamble(EXTRACT_PROPOSITIONS_PROMPT)
            .temperature(0.2)
            .build();

        Ok(Self {
            client,
            extract_agent,
        })
    }

    pub fn client(&self) -> &openai::Client {
        &self.client
    }

    pub async fn chat(&self, text: &str) -> Result<(), anyhow::Error> {
        let response = self.extract_agent.prompt(text).await?;
        println!("{}", response);

        Ok(())
    }

    // split_text -> extract code blocks -> extract statement
    pub async fn extract_statement(&self, text: &str) -> Result<Vec<Statement>> {
        let response = self.extract_agent.prompt(text).await?;
        let json = strip_code_fence(&response);
        let stmts: Statements = serde_json::from_str(json)?;
        Ok(stmts.statements)
    }
}

fn strip_code_fence(s: &str) -> &str {
    let s = s.trim();
    let s = s
        .strip_prefix("```json")
        .or_else(|| s.strip_prefix("```"))
        .unwrap_or(s);
    s.strip_suffix("```").unwrap_or(s).trim()
}

#[derive(Deserialize)]
struct Statements {
    statements: Vec<Statement>,
}

#[derive(Deserialize, Debug)]
pub struct Statement {
    pub statement: String,
    pub keywords: Vec<String>,
}

const EXTRACT_PROPOSITIONS_PROMPT: &str = r#"
You are a top-tier algorithm designed for extracting information in structured formats to build a knowledge graph. Your task is to decompose the given text into clear, concise, and context-independent propositions.

# Steps: What to do
1. Break down complex sentences into simple, atomic statements.
2. Break down lists of factual content into individual statements. Tables should also be decomposed row by row.
3. Resolve coreferences (pronouns, abbreviations, partial names) to the most complete identifier for that word, using the conversation history of previously processed chunks where helpful.
4. Isolate descriptive information about named keywords into separate propositions.
5. Decontextualize each proposition:
   a) Replace pronouns (e.g., he, she, it, they) with the specific nouns they refer to.
   b) Expand acronyms to their full forms on first use per chunk.
   c) Add necessary modifiers so the proposition is self-explanatory without external context.
   d) Preserve quoted speech or dialogue as-is.
6. Capture all relevant details from the original text, including temporal, spatial, and causal relationships.

# Constraints: What makes a valid proposition
1. Every proposition MUST be a grammatically complete sentence with a subject and predicate. Bare labels, headings, or noun phrases are not valid propositions.
2. A list of section headings or topic labels (e.g. '⑧ Access management', '⑨ Cost management') is NOT factual content — fold such a list into a single summary proposition (e.g. 'The system covers access management, cost management, and data utilization.').
3. Each proposition must be verifiable against the original text. Do not introduce information from outside the text or from your training knowledge.
4. Do not use your training knowledge to identify, name, or fill in details about keywords — rely only on the provided text and conversation history.
5. Each proposition must be unique; do not repeat propositions.
6. All output must be in English, regardless of the input language.

# Exclusion Rules: What to skip
1. If the chunk consists entirely of a table of contents, cover page form fields, or a revision history table with no substantive content, output only the title line with no propositions.
2. Markers in the format [image:filename] are embedded images — ignore them entirely; do not create propositions about images.
3. Markers in the format [heading:text] are sub-section headings embedded in the chunk — use them as context to interpret surrounding content, but do not emit them as propositions.

# Keyword Extraction & Normalization
1. Extract named entities, concepts, or technical terms as 'keywords' for each statement. The keywords array may be empty [] if no suitable keyword appears in that proposition.
2. Proper nouns, product names, and service names must use Title Case (e.g., 'Unity Catalog', 'Lakeflow Pipelines', 'Delta Lake').
3. Acronyms and technical abbreviations must use UPPERCASE (e.g., 'AWS', 'S3', 'SQS', 'MDM', 'ETL', 'API').
4. When the same entity appears with variant spellings or in mixed languages, choose one canonical English form and use it consistently.
5. If an entity name was already established in the conversation history, use that exact canonical form.

# Output Format
Respond with a single JSON object in this exact schema:
{
  "statements": [
    {
      "statement": "<complete proposition>",
      "keywords": []
    }
  ]
}
"#;
