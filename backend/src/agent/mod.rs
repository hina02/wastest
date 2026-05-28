//! LLM プロバイダの抽象化。

pub mod gemini;
pub mod openai;
pub mod prompts;

use anyhow::Result;
use serde::Deserialize;

/// LLM が抽出した命題と関連キーワード。
#[derive(Deserialize, Debug, Clone)]
pub struct Statement {
    pub statement: String,
    pub keywords: Vec<String>,
}

/// 抽出 + 埋め込みの両方を提供する LLM クライアント抽象。
/// 静的ディスパッチ (`<P: LlmProvider>`) 前提。
pub trait LlmProvider: Send + Sync {
    /// テキストから命題を抽出する。
    fn extract_statement(
        &self,
        text: &str,
    ) -> impl std::future::Future<Output = Result<Vec<Statement>>> + Send;

    /// テキスト群をベクトル化する (バッチ呼び出し)。
    fn embed_texts(
        &self,
        texts: Vec<String>,
    ) -> impl std::future::Future<Output = Result<Vec<Vec<f64>>>> + Send;
}

/// LLM が返した文字列に ```json ... ``` のフェンスが付いていたら剥がす。
pub(crate) fn strip_code_fence(s: &str) -> &str {
    let s = s.trim();
    let s = s
        .strip_prefix("```json")
        .or_else(|| s.strip_prefix("```"))
        .unwrap_or(s);
    s.strip_suffix("```").unwrap_or(s).trim()
}
