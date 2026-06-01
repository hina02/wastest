//! ハイブリッド検索 + コンテンツ取得ツール (rig::tool::Tool 実装)。
//!
//! - [`HybridSearchTool`]: クエリを FTS+VSS でハイブリッド検索し、statement 一覧を返す。
//! - [`GetContentTool`]: statement_id からソース全文を取得する。
//!
//! # 使い方
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use wastest::agent::search_tools::{GetContentTool, HybridSearchTool};
//! # use wastest::config::SETTINGS;
//! # use wastest::GeminiClient;
//! # use rig::providers::gemini;
//! # use rig::client::CompletionClient;
//! # use secrecy::ExposeSecret;
//! # async fn example() -> anyhow::Result<()> {
//! let llm = Arc::new(GeminiClient::new().await?);
//! let lance_db = lancedb::connect(&SETTINGS.lance_uri_for("hn")).execute().await?;
//!
//! let search_tool = HybridSearchTool::new(lance_db.clone(), Arc::clone(&llm));
//! let content_tool = GetContentTool::new(lance_db);
//!
//! let gemini_client = gemini::Client::new(SETTINGS.gemini_api_key.expose_secret());
//! let agent = gemini_client
//!     .agent("gemini-3-flash-preview")
//!     .preamble("あなたはドキュメント検索アシスタントです。")
//!     .tool(search_tool)
//!     .tool(content_tool)
//!     .build();
//! # Ok(())
//! # }
//! ```

use crate::agent::LlmProvider;
use crate::lance::reader::{FTS_SCORE_MIN, RRF_K};
use crate::lance::tables::{contents, statements};
use anyhow::{Context, Result};
use arrow_array::{Array, Float32Array, Int64Array, ListArray, StringArray};
use futures::TryStreamExt;
use lance_index::scalar::FullTextSearchQuery;
use lancedb::query::{ExecutableQuery, QueryBase, QueryExecutionOptions, Select};
use lancedb::rerankers::rrf::RRFReranker;
use lancedb::DistanceType;
use lancedb::Connection as LanceConnection;
use rig::completion::ToolDefinition;
use rig::tool::Tool;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

// ================================================================
// ツール共通のエラー型
// ================================================================

/// `anyhow::Error` を `std::error::Error` 境界に適合させるラッパー。
#[derive(Debug)]
pub struct SearchError(anyhow::Error);

impl std::fmt::Display for SearchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for SearchError {}

impl From<anyhow::Error> for SearchError {
    fn from(e: anyhow::Error) -> Self {
        Self(e)
    }
}

// ================================================================
// HybridSearchTool
// ================================================================

/// `hybrid_search` ツールの入力引数。
#[derive(Deserialize)]
pub struct HybridSearchArgs {
    /// 検索クエリ文字列。
    pub query: String,
    /// 返す最大件数 (省略時 5)。
    pub limit: Option<usize>,
}

/// `hybrid_search` ツールが返す 1 件の結果。
#[derive(Serialize)]
pub struct HybridSearchResult {
    pub statement_id: String,
    pub content_id: i64,
    pub statement: String,
    pub keywords: Vec<String>,
    pub relevance_score: f32,
}

/// FTS + VSS ハイブリッド検索ツール。
pub struct HybridSearchTool<L: LlmProvider> {
    lance_db: LanceConnection,
    llm: Arc<L>,
}

impl<L: LlmProvider> HybridSearchTool<L> {
    pub fn new(lance_db: LanceConnection, llm: Arc<L>) -> Self {
        Self { lance_db, llm }
    }

    /// 実処理。`anyhow::Result` で書き、Tool::call からエラー変換して呼ぶ。
    async fn run(&self, args: HybridSearchArgs) -> Result<Vec<HybridSearchResult>> {
        let limit = args.limit.unwrap_or(5);

        let vecs = self.llm.embed_texts(vec![args.query.clone()]).await?;
        let q_vec: Vec<f32> = vecs
            .into_iter()
            .next()
            .context("embed_texts returned empty")?
            .into_iter()
            .map(|x| x as f32)
            .collect();

        let tbl = self
            .lance_db
            .open_table(statements::TBL)
            .execute()
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        let mut stream = tbl
            .query()
            .full_text_search(FullTextSearchQuery::new(args.query.clone()))
            .nearest_to(q_vec)
            .map_err(|e| anyhow::anyhow!(e))?
            .distance_type(DistanceType::Cosine)
            .rerank(Arc::new(RRFReranker::new(RRF_K)))
            .limit(limit)
            .execute_hybrid(QueryExecutionOptions::default())
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        let mut results = Vec::new();
        while let Some(b) = stream.try_next().await.map_err(|e| anyhow::anyhow!(e))? {
            let ids = downcast_str(&b, "id")?;
            let cids = downcast_i64(&b, "content_id")?;
            let stmts = downcast_str(&b, "statement")?;
            let kws = b.column_by_name("keywords").cloned();
            let relevance = downcast_f32_opt(&b, "_relevance_score");
            let fts_scores = downcast_f32_opt(&b, "_score");
            let vss_dists = downcast_f32_opt(&b, "_distance");

            for i in 0..b.num_rows() {
                let fts_score = fts_scores.as_ref().map(|a| a.value(i));
                let vss_sim = vss_dists.as_ref().map(|a| 1.0 - a.value(i));
                if vss_sim.is_none() && !fts_score.map_or(false, |s| s > FTS_SCORE_MIN) {
                    continue;
                }
                results.push(HybridSearchResult {
                    statement_id: ids.value(i).to_string(),
                    content_id: cids.value(i),
                    statement: stmts.value(i).to_string(),
                    keywords: keywords_at(&kws, i),
                    relevance_score: relevance
                        .as_ref()
                        .map(|a| a.value(i))
                        .unwrap_or(f32::NAN),
                });
            }
        }
        Ok(results)
    }
}

impl<L: LlmProvider + 'static> Tool for HybridSearchTool<L> {
    const NAME: &'static str = "hybrid_search";
    type Error = SearchError;
    type Args = HybridSearchArgs;
    type Output = Vec<HybridSearchResult>;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description: "クエリに関連する statement を FTS+VSS ハイブリッド検索で探す。\
                結果に statement_id が含まれるため、ソース全文が必要なら \
                get_content ツールを続けて呼ぶこと。"
                .to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "検索クエリ文字列"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "返す最大件数 (省略時 5)"
                    }
                },
                "required": ["query"]
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, SearchError> {
        self.run(args).await.map_err(SearchError)
    }
}

// ================================================================
// GetContentTool
// ================================================================

/// `get_content` ツールの入力引数。
#[derive(Deserialize)]
pub struct GetContentArgs {
    /// コンテンツを取得したい statement の UUID 文字列。
    pub statement_id: String,
}

/// `get_content` ツールが返す結果。
#[derive(Serialize)]
pub struct GetContentResult {
    pub statement_id: String,
    pub content_id: i64,
    /// ソースコンテンツ全文。
    pub content: String,
}

/// statement_id → ソースコンテンツ全文取得ツール。
pub struct GetContentTool {
    lance_db: LanceConnection,
}

impl GetContentTool {
    pub fn new(lance_db: LanceConnection) -> Self {
        Self { lance_db }
    }

    async fn run(&self, args: GetContentArgs) -> Result<GetContentResult> {
        let sid = Uuid::parse_str(&args.statement_id)
            .with_context(|| format!("invalid statement_id: {}", args.statement_id))?;

        // statements テーブルから content_id を引く
        let tbl = self
            .lance_db
            .open_table(statements::TBL)
            .execute()
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        let mut stream = tbl
            .query()
            .only_if(format!("id = '{sid}'"))
            .select(Select::columns(&["content_id"]))
            .limit(1)
            .execute()
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        let mut content_id: Option<i64> = None;
        while let Some(b) = stream.try_next().await.map_err(|e| anyhow::anyhow!(e))? {
            let cids = downcast_i64(&b, "content_id")?;
            if cids.len() > 0 {
                content_id = Some(cids.value(0));
                break;
            }
        }
        let content_id = content_id
            .with_context(|| format!("statement not found: {sid}"))?;

        // contents テーブルから全文を引く
        let tbl = self
            .lance_db
            .open_table(contents::TBL)
            .execute()
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        let mut stream = tbl
            .query()
            .only_if(format!("id = {content_id}"))
            .select(Select::columns(&["content"]))
            .limit(1)
            .execute()
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        while let Some(b) = stream.try_next().await.map_err(|e| anyhow::anyhow!(e))? {
            let col = downcast_str(&b, "content")?;
            if col.len() > 0 {
                return Ok(GetContentResult {
                    statement_id: args.statement_id,
                    content_id,
                    content: col.value(0).to_string(),
                });
            }
        }

        anyhow::bail!("content not found for content_id={content_id}")
    }
}

impl Tool for GetContentTool {
    const NAME: &'static str = "get_content";
    type Error = SearchError;
    type Args = GetContentArgs;
    type Output = GetContentResult;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description:
                "statement_id を受け取り、その statement が属するソースコンテンツの全文を返す。"
                    .to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "statement_id": {
                        "type": "string",
                        "description": "全文を取得したい statement の UUID"
                    }
                },
                "required": ["statement_id"]
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, SearchError> {
        self.run(args).await.map_err(SearchError)
    }
}

// ================================================================
// 内部ヘルパ
// ================================================================

fn downcast_str(b: &arrow_array::RecordBatch, name: &str) -> Result<StringArray> {
    let col = b
        .column_by_name(name)
        .with_context(|| format!("column '{name}' missing"))?;
    Ok(col
        .as_any()
        .downcast_ref::<StringArray>()
        .with_context(|| format!("column '{name}' is not Utf8"))?
        .clone())
}

fn downcast_i64(b: &arrow_array::RecordBatch, name: &str) -> Result<Int64Array> {
    let col = b
        .column_by_name(name)
        .with_context(|| format!("column '{name}' missing"))?;
    Ok(col
        .as_any()
        .downcast_ref::<Int64Array>()
        .with_context(|| format!("column '{name}' is not Int64"))?
        .clone())
}

fn downcast_f32_opt(b: &arrow_array::RecordBatch, name: &str) -> Option<Float32Array> {
    b.column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<Float32Array>())
        .cloned()
}

fn keywords_at(col: &Option<Arc<dyn Array>>, row: usize) -> Vec<String> {
    col.as_ref()
        .and_then(|c| c.as_any().downcast_ref::<ListArray>())
        .map(|la| extract_string_list(la, row))
        .unwrap_or_default()
}

fn extract_string_list(la: &ListArray, row: usize) -> Vec<String> {
    if la.is_null(row) {
        return Vec::new();
    }
    let start = la.value_offsets()[row] as usize;
    let end = la.value_offsets()[row + 1] as usize;
    let values = la.values();
    let strs = values
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("List<Utf8>");
    (start..end).map(|i| strs.value(i).to_string()).collect()
}
