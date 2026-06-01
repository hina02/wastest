//! ドキュメントサイトのリンクを BFS で再帰収集する。
//!
//! シード URL から最大 `depth` ホップ辿り、発見した全 URL を
//! `<namespace>_links.txt` に書き出す。同一ホストのみ追跡。
//!
//! ```sh
//! cargo run --bin crawl_links -- <namespace> <depth> <url> [url ...]
//!
//! # 出力を ingest_urls に渡す例
//! cargo run --bin ingest_urls -- <namespace> $(cat <namespace>_links.txt)
//! ```

use anyhow::{Context, Result};
use futures::stream::StreamExt;
use std::collections::HashSet;
use std::sync::Arc;
use url::Url;
use wastest::api::parse::fetch_html;
use wastest::parse::html::extract_all_links;

const FETCH_CONCURRENCY: usize = 10;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let mut args = std::env::args().skip(1);
    let namespace = args
        .next()
        .context("usage: crawl_links <namespace> <depth> <url> [url ...]")?;
    let max_depth: usize = args
        .next()
        .context("usage: crawl_links <namespace> <depth> <url> [url ...]")?
        .parse()
        .context("depth must be a non-negative integer")?;
    let seeds: Vec<String> = args.collect();
    if seeds.is_empty() {
        anyhow::bail!("at least one seed URL is required");
    }

    // シード URL のホスト一覧 → 追跡フィルタ
    let allowed_hosts: Arc<HashSet<String>> = Arc::new(
        seeds
            .iter()
            .filter_map(|u| Url::parse(u).ok())
            .filter_map(|u| u.host_str().map(|h| h.to_lowercase()))
            .collect(),
    );
    if allowed_hosts.is_empty() {
        anyhow::bail!("no valid seed URLs provided");
    }
    println!("allowed hosts: {:?}", allowed_hosts);

    let http = Arc::new(
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .user_agent("wastest/0.1")
            .build()?,
    );

    let mut visited: HashSet<String> = seeds.iter().map(|u| normalize_url(u)).collect();
    let mut frontier: Vec<String> = seeds.iter().map(|u| normalize_url(u)).collect();

    for hop in 1..=max_depth {
        let current = std::mem::take(&mut frontier);
        if current.is_empty() {
            break;
        }
        println!(
            "hop {hop}/{max_depth}: fetching {} page(s) ...",
            current.len()
        );

        let discovered: Vec<Vec<String>> = futures::stream::iter(current)
            .map(|url| {
                let http = http.clone();
                let hosts = allowed_hosts.clone();
                async move { links_from_page(&http, &url, &hosts).await }
            })
            .buffer_unordered(FETCH_CONCURRENCY)
            .collect()
            .await;

        let mut new_count = 0usize;
        for links in discovered {
            for link in links {
                if visited.insert(link.clone()) {
                    frontier.push(link);
                    new_count += 1;
                }
            }
        }
        println!(
            "  -> {new_count} new URL(s) (total {})",
            visited.len()
        );
    }

    // 出力ファイル
    let out_path = format!("{namespace}_links.txt");
    let content: String = {
        let mut urls: Vec<&String> = visited.iter().collect();
        urls.sort();
        urls.iter().map(|u| format!("{u}\n")).collect()
    };
    std::fs::write(&out_path, &content).context("write links file")?;

    println!("\n{} URL(s) -> {out_path}", visited.len());
    println!(
        "next: cargo run --bin ingest_urls -- {namespace} $(cat {out_path})"
    );
    Ok(())
}

/// ページを fetch し、同一ホストの絶対 URL 一覧を返す。失敗時は空 Vec。
///
/// `<nav>`/`<aside>` 含む DOM 全体の `<a>` を対象にするため、
/// CETD スコアリングなしの `extract_all_links` を使う。
async fn links_from_page(
    http: &reqwest::Client,
    url: &str,
    allowed_hosts: &HashSet<String>,
) -> Vec<String> {
    let body = match fetch_html(http, url).await {
        Ok(b) => b,
        Err(e) => {
            eprintln!("  warn: fetch failed [{url}]: {e}");
            return vec![];
        }
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

/// フラグメント除去 + パスが "/" より長い場合のみ末尾スラッシュ除去。
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
