//! wastest web server。
//! `cargo run --bin serve` で起動、http://localhost:3000 にアクセス。
//!
//! FRONTEND_DIR 環境変数でフロントエンドのディレクトリを指定できる (デフォルト: ../frontend)。

use anyhow::{Context, Result};
use axum::{
    Form, Json, Router,
    extract::{Query, State},
    response::{Html, IntoResponse},
    routing::{get, post},
};
use futures::StreamExt;
use serde::Deserialize;
use std::{collections::HashSet, sync::Arc};
use tower_http::services::ServeDir;
use tracing_subscriber::EnvFilter;
use url::Url;
use wastest::{
    GeminiClient, LanceSearcher, LlmProvider,
    api::parse::fetch_html,
    config::SETTINGS,
    connect,
    lance::LanceStore,
    parse::html::extract_all_links,
    pipeline::run_pipeline_with_urls,
};

// ----------------------------------------------------------------
// State & Error
// ----------------------------------------------------------------

#[derive(Clone)]
struct AppState {
    llm: Arc<GeminiClient>,
    sqlite: sqlx::SqlitePool,
    http: reqwest::Client,
}

struct AppError(anyhow::Error);
impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        // 200 OK + HTML で返す → HTMX がターゲットにスワップできる
        Html(format!("<span class='error'>{}</span>", esc(&self.0.to_string()))).into_response()
    }
}
impl<E: Into<anyhow::Error>> From<E> for AppError {
    fn from(e: E) -> Self {
        Self(e.into())
    }
}
type HtmlResp = Result<Html<String>, AppError>;

fn esc(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

fn ok_span(msg: &str) -> Html<String> {
    Html(format!("<span class='ok'>{}</span>", esc(msg)))
}

// ----------------------------------------------------------------
// Search
// ----------------------------------------------------------------

#[derive(Deserialize)]
struct SearchParams {
    namespace: String,
    #[serde(rename = "type", default = "default_search_type")]
    search_type: String,
    q: String,
    #[serde(default = "default_limit")]
    limit: usize,
}
fn default_search_type() -> String {
    "hybrid".to_string()
}
fn default_limit() -> usize {
    5
}

fn hits_html(items: Vec<(String, Vec<String>, f32)>) -> Html<String> {
    if items.is_empty() {
        return Html("<li class='status'>結果なし</li>".to_string());
    }
    let html: String = items
        .iter()
        .map(|(stmt, kws, score)| {
            format!(
                "<li><p>{}</p><small class='hit-score'>score: {score:.3}</small>\
                 <div class='hit-keywords'>{}</div></li>",
                esc(stmt),
                kws.iter().map(|k| esc(k)).collect::<Vec<_>>().join(", ")
            )
        })
        .collect();
    Html(html)
}

async fn search(State(s): State<AppState>, Query(p): Query<SearchParams>) -> HtmlResp {
    let reader = LanceSearcher::open(&SETTINGS.lance_uri_for(&p.namespace)).await?;
    match p.search_type.as_str() {
        "fts" => {
            let hits = reader.search_fts(&p.q, p.limit).await?;
            Ok(hits_html(
                hits.into_iter().map(|h| (h.statement, h.keywords, h.score)).collect(),
            ))
        }
        "vss" => {
            let vecs = s.llm.embed_texts(vec![p.q]).await?;
            let q_vec: Vec<f32> = vecs
                .into_iter()
                .next()
                .context("empty embedding")?
                .into_iter()
                .map(|v| v as f32)
                .collect();
            let hits = reader.search_vss(q_vec, p.limit).await?;
            Ok(hits_html(
                hits.into_iter().map(|h| (h.statement, h.keywords, h.similarity)).collect(),
            ))
        }
        _ => {
            // hybrid (default)
            let vecs = s.llm.embed_texts(vec![p.q.clone()]).await?;
            let q_vec: Vec<f32> = vecs
                .into_iter()
                .next()
                .context("empty embedding")?
                .into_iter()
                .map(|v| v as f32)
                .collect();
            let hits = reader.search_hybrid(&p.q, q_vec, p.limit).await?;
            Ok(hits_html(
                hits.into_iter()
                    .map(|h| (h.statement, h.keywords, h.relevance_score))
                    .collect(),
            ))
        }
    }
}

// ----------------------------------------------------------------
// Chat
// ----------------------------------------------------------------

const CHAT_MODEL: &str = "gemini-2.5-flash";
const PREAMBLE: &str = "あなたはドキュメント検索アシスタントです。\
ユーザーの質問に答えるために、必要に応じて以下のツールを使ってください。\n\
- hybrid_search: クエリに関連する statement を検索する。\n\
- get_content: statement_id からソース全文を取得する。\n\
得られた情報を基に、簡潔かつ正確に回答してください。";

#[derive(Deserialize)]
struct ChatReq {
    namespace: String,
    session_id: String,
    message: String,
}

async fn chat_send(State(s): State<AppState>, Form(req): Form<ChatReq>) -> HtmlResp {
    use rig::client::CompletionClient;
    use rig::completion::Chat;
    use secrecy::ExposeSecret;
    use wastest::agent::search_tools::{GetContentTool, HybridSearchTool};
    use wastest::db::chat;

    chat::ensure_table(&s.sqlite).await?;
    let lance_db = lancedb::connect(&SETTINGS.lance_uri_for(&req.namespace))
        .execute()
        .await?;
    let search_tool = HybridSearchTool::new(lance_db.clone(), Arc::clone(&s.llm));
    let content_tool = GetContentTool::new(lance_db);
    let gemini_client =
        rig::providers::gemini::Client::new(SETTINGS.gemini_api_key.expose_secret());
    let agent = gemini_client
        .agent(CHAT_MODEL)
        .preamble(PREAMBLE)
        .additional_params(serde_json::json!({ "generationConfig": {} }))
        .tool(search_tool)
        .tool(content_tool)
        .build();

    let history = chat::load_history(&s.sqlite, &req.session_id).await?;
    chat::append(&s.sqlite, &req.session_id, "user", &req.message).await?;
    let answer = agent.chat(req.message.clone(), history).await?;
    chat::append(&s.sqlite, &req.session_id, "assistant", &answer).await?;

    Ok(Html(format!(
        "<div class='chat-msg user'><div class='role'>User</div><div class='body'>{}</div></div>\
         <div class='chat-msg assistant'><div class='role'>Assistant</div><div class='body'>{}</div></div>",
        esc(&req.message),
        esc(&answer)
    )))
}

#[derive(Deserialize)]
struct ClearReq {
    session_id: String,
}

async fn chat_clear(State(s): State<AppState>, Form(req): Form<ClearReq>) -> HtmlResp {
    wastest::db::chat::clear(&s.sqlite, &req.session_id).await?;
    Ok(Html(String::new()))
}

// ----------------------------------------------------------------
// Admin: Ingest URLs
// ----------------------------------------------------------------

#[derive(Deserialize)]
struct IngestForm {
    namespace: String,
    urls: String,
}

fn id_from_url(url: &str) -> i64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in url.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h as i64
}

async fn ingest(State(s): State<AppState>, Form(req): Form<IngestForm>) -> HtmlResp {
    let uri = SETTINGS.lance_uri_for(&req.namespace);
    let store = Arc::new(LanceStore::open(&uri).await?);
    let existing = store.existing_content_ids().await?;
    let all_urls: Vec<String> = req
        .urls
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect();
    let total = all_urls.len();
    let urls: Vec<(i64, String)> = all_urls
        .into_iter()
        .map(|u| (id_from_url(&u), u))
        .filter(|(id, _)| !existing.contains(id))
        .collect();
    let ingested = urls.len();
    if !urls.is_empty() {
        run_pipeline_with_urls(urls, Arc::clone(&s.llm), store).await?;
    }
    Ok(ok_span(&format!("ingested {ingested}, skipped {}", total - ingested)))
}

// ----------------------------------------------------------------
// Admin: Crawl Links
// ----------------------------------------------------------------

#[derive(Deserialize)]
struct CrawlForm {
    depth: usize,
    seeds: String,
}

async fn crawl(State(s): State<AppState>, Form(req): Form<CrawlForm>) -> HtmlResp {
    let seed_urls: Vec<String> = req
        .seeds
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect();

    let allowed_hosts: Arc<HashSet<String>> = Arc::new(
        seed_urls
            .iter()
            .filter_map(|u| Url::parse(u).ok())
            .filter_map(|u| u.host_str().map(|h| h.to_lowercase()))
            .collect(),
    );
    if allowed_hosts.is_empty() {
        return Err(anyhow::anyhow!("有効なシードURLがありません").into());
    }
    let mut visited: HashSet<String> = seed_urls.iter().map(|u| normalize_url(u)).collect();
    let mut frontier: Vec<String> = visited.iter().cloned().collect();

    for _ in 1..=req.depth {
        let current = std::mem::take(&mut frontier);
        if current.is_empty() {
            break;
        }
        let discovered: Vec<Vec<String>> = futures::stream::iter(current)
            .map(|url| {
                let http = s.http.clone();
                let hosts = allowed_hosts.clone();
                async move { links_from_page(&http, &url, &hosts).await }
            })
            .buffer_unordered(10)
            .collect()
            .await;
        for links in discovered {
            for link in links {
                if visited.insert(link.clone()) {
                    frontier.push(link);
                }
            }
        }
    }

    let mut urls: Vec<String> = visited.into_iter().collect();
    urls.sort();

    let mut html = format!("<li class='status ok'>発見: {}件</li>", urls.len());
    for u in &urls {
        html.push_str(&format!("<li>{}</li>", esc(u)));
    }
    Ok(Html(html))
}

async fn links_from_page(
    http: &reqwest::Client,
    url: &str,
    allowed_hosts: &HashSet<String>,
) -> Vec<String> {
    let Ok(body) = fetch_html(http, url).await else {
        return vec![];
    };
    extract_all_links(&body, url)
        .into_iter()
        .map(|l| normalize_url(&l.href))
        .filter(|href| {
            Url::parse(href)
                .ok()
                .and_then(|u| u.host_str().map(|h| h.to_lowercase()))
                .map_or(false, |h| allowed_hosts.contains(&h))
        })
        .collect()
}

fn normalize_url(url: &str) -> String {
    let Ok(mut u) = Url::parse(url) else {
        return url.to_string();
    };
    u.set_fragment(None);
    let s = u.to_string();
    if s.ends_with('/') && u.path().len() > 1 {
        s.trim_end_matches('/').to_string()
    } else {
        s
    }
}

// ----------------------------------------------------------------
// Admin: Refresh Indexes
// ----------------------------------------------------------------

#[derive(Deserialize)]
struct RefreshForm {
    namespace: String,
}

async fn refresh_indexes(Form(req): Form<RefreshForm>) -> HtmlResp {
    let store = LanceStore::open(&SETTINGS.lance_uri_for(&req.namespace)).await?;
    store.ensure_indexes().await?;
    Ok(ok_span("インデックスを更新しました"))
}

// ----------------------------------------------------------------
// Admin: Top Stories
// ----------------------------------------------------------------

#[derive(Deserialize)]
struct TopStoriesParams {
    #[serde(default = "default_hn")]
    namespace: String,
}
fn default_hn() -> String {
    "hn".to_string()
}

async fn top_stories(Query(p): Query<TopStoriesParams>) -> HtmlResp {
    let reader = LanceSearcher::open(&SETTINGS.lance_uri_for(&p.namespace)).await?;
    let stories = reader.fetch_all_top_stories().await?;
    if stories.is_empty() {
        return Ok(Html("<li class='status'>データなし</li>".to_string()));
    }
    let html: String = stories
        .into_iter()
        .map(|(at, ids)| format!("<li>{} ({}件)</li>", esc(&at), ids.len()))
        .collect();
    Ok(Html(html))
}

// ----------------------------------------------------------------
// Namespaces (JSON — JS snippet で使用)
// ----------------------------------------------------------------

async fn list_namespaces() -> Result<Json<Vec<String>>, AppError> {
    let lance_dir = &SETTINGS.lance_dir;
    let mut names = Vec::new();
    let mut rd = tokio::fs::read_dir(lance_dir).await?;
    while let Some(entry) = rd.next_entry().await? {
        let path = entry.path();
        if path.is_dir() && path.join("__manifest").exists() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                names.push(name.to_string());
            }
        }
    }
    names.sort();
    Ok(Json(names))
}

// ----------------------------------------------------------------
// Main
// ----------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let sqlite = connect(&SETTINGS.database_url).await?;
    let llm = Arc::new(GeminiClient::new().await?);
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .user_agent("wastest/0.1")
        .build()?;
    let state = AppState { llm, sqlite, http };

    let frontend_dir = std::env::var("FRONTEND_DIR")
        .unwrap_or_else(|_| concat!(env!("CARGO_MANIFEST_DIR"), "/../frontend").to_string());

    let app = Router::new()
        .route("/api/search", get(search))
        .route("/api/chat", post(chat_send))
        .route("/api/chat/clear", post(chat_clear))
        .route("/api/ingest", post(ingest))
        .route("/api/crawl", post(crawl))
        .route("/api/refresh", post(refresh_indexes))
        .route("/api/top-stories", get(top_stories))
        .route("/api/namespaces", get(list_namespaces))
        .fallback_service(ServeDir::new(frontend_dir))
        .with_state(state);

    let addr = "0.0.0.0:3000";
    tracing::info!("Listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
