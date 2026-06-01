//! hybrid_search + get_content ツールを持つ検索エージェント (チャット履歴付き)。
//!
//! ```
//! cargo run --bin search_agent -- <namespace> [session_id]
//! cargo run --bin search_agent -- hn
//! cargo run --bin search_agent -- hn my_session
//! ```
//!
//! - 会話履歴は SQLite (`DATABASE_URL`) の `chat_messages` テーブルに永続化される。
//! - 同じ session_id を指定すると前回の会話を引き継ぐ。
//! - `/clear` と入力するとセッション履歴を削除してリセット。
//! - `Ctrl-D` または空行 Enter で終了。

use anyhow::{Context, Result};
use rig::client::CompletionClient;
use rig::completion::Chat;
use secrecy::ExposeSecret;
use std::io::{self, BufRead, Write};
use std::sync::Arc;
use wastest::agent::search_tools::{GetContentTool, HybridSearchTool};
use wastest::config::SETTINGS;
use wastest::db::chat;
use wastest::{connect, GeminiClient};

const CHAT_MODEL: &str = "gemini-3-flash-preview";

const PREAMBLE: &str = "\
あなたはドキュメント検索アシスタントです。\
ユーザーの質問に答えるために、必要に応じて以下のツールを使ってください。\n\
- hybrid_search: クエリに関連する statement を検索する。\n\
- get_content: statement_id からソース全文を取得する。\n\
得られた情報を基に、簡潔かつ正確に回答してください。";

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let mut args = std::env::args().skip(1);
    let namespace = args
        .next()
        .context("usage: search_agent <namespace> [session_id]")?;
    let session_id = args.next().unwrap_or_else(|| "default".to_string());

    // SQLite 接続 & テーブル初期化
    let pool = connect(&SETTINGS.database_url).await?;
    chat::ensure_table(&pool).await?;

    // LLM + Lance 接続
    let llm = Arc::new(GeminiClient::new().await?);
    let lance_db = lancedb::connect(&SETTINGS.lance_uri_for(&namespace))
        .execute()
        .await?;

    let search_tool = HybridSearchTool::new(lance_db.clone(), Arc::clone(&llm));
    let content_tool = GetContentTool::new(lance_db);

    let gemini_client =
        rig::providers::gemini::Client::new(SETTINGS.gemini_api_key.expose_secret());
    let agent = gemini_client
        .agent(CHAT_MODEL)
        .preamble(PREAMBLE)
        .tool(search_tool)
        .tool(content_tool)
        .build();

    println!("namespace={namespace}  session={session_id}");
    println!("('/clear' で履歴削除, Ctrl-D で終了)");
    println!("---");

    let stdin = io::stdin();
    loop {
        // プロンプト表示
        print!("> ");
        io::stdout().flush()?;

        let mut line = String::new();
        match stdin.lock().read_line(&mut line) {
            Ok(0) => break, // EOF
            Ok(_) => {}
            Err(e) => return Err(e.into()),
        }
        let question = line.trim();
        if question.is_empty() {
            break;
        }

        // /clear コマンド
        if question == "/clear" {
            chat::clear(&pool, &session_id).await?;
            println!("(履歴を削除しました)");
            continue;
        }

        // SQLite から履歴ロード
        let history = chat::load_history(&pool, &session_id).await?;

        // ユーザーメッセージを DB に保存
        chat::append(&pool, &session_id, "user", question).await?;

        // Agent に送信 (履歴付き)
        let answer = agent.chat(question.to_string(), history).await?;

        // アシスタント応答を DB に保存
        chat::append(&pool, &session_id, "assistant", &answer).await?;

        println!("{answer}");
        println!();
    }

    Ok(())
}
