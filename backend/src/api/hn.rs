use crate::agent::LlmProvider;
use crate::db::duck::{DuckDBWriter, DuckReadOps};
use crate::db::hn::{CreateHNItem, create_many, exists, list_urls};
use crate::pipeline::run_pipeline_with_urls;
use anyhow::Result;
use bloomfilter::Bloom;
use futures::stream::{self, StreamExt};
use serde::Deserialize;
use sqlx::SqlitePool;
use std::sync::Arc;
use tracing::{info, warn};

const BASE_URL: &str = "https://hacker-news.firebaseio.com";
const BATCH_SIZE: usize = 5;

/// HN 専用 ingest エントリ: SQLite `hn_items` を起点に未処理 URL を抽出 pipeline に流す。
/// `writer` は HN 用 DuckDB ファイル (`<duckdb_dir>/hn.duckdb`) を開いたもの。
/// 新しいドメインを足すときはこれを真似て別の関数を作るか、
/// `run_pipeline_with_urls` を直接呼べばよい。
pub async fn run_hn_pipeline<P>(
    pool: &SqlitePool,
    client: Arc<P>,
    writer: &DuckDBWriter,
) -> Result<()>
where
    P: LlmProvider + 'static,
{
    let all_urls = list_urls(pool).await?;
    let existing = writer.existing_content_ids()?;
    let urls: Vec<(i64, String)> = all_urls
        .into_iter()
        .filter(|(id, _)| !existing.contains(id))
        .collect();
    run_pipeline_with_urls(urls, client, writer).await
}

pub struct CrawlerState {
    pub pool: SqlitePool,
    pub client: reqwest::Client,
    pub bloom: Bloom<i64>,
    pub duck: DuckDBWriter,
}

impl CrawlerState {
    pub async fn new(pool: SqlitePool, duck: DuckDBWriter) -> Result<Self> {
        let client = reqwest::Client::new();
        let mut bloom = Bloom::new_for_fp_rate(10_000_000, 0.01).map_err(anyhow::Error::msg)?;

        println!("SQLiteから既存のIDを読み込み、Bloom Filterを構築中...");
        {
            let mut rows = sqlx::query_scalar!("SELECT id FROM hn_items").fetch(&pool);
            while let Some(row) = rows.next().await {
                let id = row?;
                bloom.set(&id);
            }
        }
        println!("Bloom Filterの構築完了");
        Ok(Self {
            pool,
            client,
            bloom,
            duck,
        })
    }

    // bloom で大雑把に除去 → SQL で存在確認 → 未保存 ID を chunk ごとに fetch & upsert
    pub async fn run_ingest_pipeline(&mut self) -> Result<()> {
        let top_story_ids: Vec<i64> = fetch_top_stories(&self.client).await?;
        println!("取得したトップストーリーの総数: {}件", top_story_ids.len());
        self.duck.insert_top_stories(&top_story_ids)?;

        let mut target_ids: Vec<i64> = Vec::new();
        for id in top_story_ids {
            if self.bloom.check(&id) {
                if !exists(&self.pool, id).await? {
                    target_ids.push(id);
                }
            } else {
                target_ids.push(id);
            }
        }

        if target_ids.is_empty() {
            println!("未保存のストーリーはありません");
            return Ok(());
        }
        println!("{}件を取得します...\n", target_ids.len());

        for chunk in target_ids.chunks(BATCH_SIZE) {
            process_chunk(&self.pool, &self.client, chunk).await?;
            for id in chunk {
                self.bloom.set(id);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct HnItem {
    id: u64,
    by: String,
    time: i64,
    title: String,
    #[serde(rename = "type")]
    item_type: String,
    url: Option<String>,
}

async fn process_chunk(
    pool: &SqlitePool,
    client: &reqwest::Client,
    chunk_ids: &[i64],
) -> Result<()> {
    let items: Vec<HnItem> = stream::iter(chunk_ids.iter().copied())
        .map(|id| {
            let client = client.clone();
            async move { (id, fetch_item(&client, id as u64).await) }
        })
        .buffer_unordered(BATCH_SIZE)
        .filter_map(|(id, result)| async move {
            match result {
                Ok(Some(item)) => Some(item),
                Ok(None) => {
                    warn!(id, "パースに失敗しました");
                    None
                }
                Err(e) => {
                    warn!(id, error = %e, "取得に失敗しました");
                    None
                }
            }
        })
        .collect()
        .await;

    if items.is_empty() {
        return Ok(());
    }

    for item in &items {
        info!(
            id = item.id,
            title = %item.title,
            by = %item.by,
            url = item.url.as_deref().unwrap_or(""),
            "fetched item"
        );
    }

    let rows: Vec<CreateHNItem> = items
        .into_iter()
        .map(|item| CreateHNItem {
            id: item.id as i64,
            item_type: item.item_type,
            by: item.by,
            time: item.time,
            title: item.title,
            url: item.url.unwrap_or_default(),
        })
        .collect();

    create_many(pool, &rows).await?;
    Ok(())
}

async fn fetch_item(client: &reqwest::Client, id: u64) -> Result<Option<HnItem>, reqwest::Error> {
    let item_url = format!("{}/v0/item/{}.json", BASE_URL, id);
    let response = client.get(item_url).send().await?;
    Ok(response.json().await.ok())
}

pub async fn fetch_top_stories(client: &reqwest::Client) -> Result<Vec<i64>, reqwest::Error> {
    let top_stories_url = format!("{}/v0/topstories.json", BASE_URL);
    let story_ids: Vec<i64> = client.get(top_stories_url).send().await?.json().await?;
    Ok(story_ids)
}
