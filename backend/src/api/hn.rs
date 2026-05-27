use crate::db::hn::{CreateHNItem, create, exists};
use futures::future::join_all;
use serde::Deserialize;
use sqlx::SqlitePool;
use std::error::Error;

const BATCH_SIZE: usize = 5;

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

fn filter_top_stories(ids: impl IntoIterator<Item = u64>) -> impl Iterator<Item = u64> {
    ids.into_iter().take(5)
}

async fn filter_unsaved(
    pool: &SqlitePool,
    ids: impl IntoIterator<Item = u64>,
) -> Result<Vec<u64>, sqlx::Error> {
    let mut out = Vec::new();
    for id in ids {
        if !exists(pool, id as i64).await? {
            out.push(id);
        }
    }
    Ok(out)
}

async fn fetch_item(
    client: &reqwest::Client,
    base_url: &str,
    id: u64,
) -> Result<Option<HnItem>, reqwest::Error> {
    let item_url = format!("{}/v0/item/{}.json", base_url, id);
    let response = client.get(item_url).send().await?;
    Ok(response.json().await.ok())
}

async fn save_item(pool: &SqlitePool, item: HnItem) -> Result<(), sqlx::Error> {
    create(
        pool,
        &CreateHNItem {
            id: item.id as i64,
            item_type: item.item_type,
            by: item.by.clone(),
            time: item.time,
            title: item.title.clone(),
            url: item.url.clone().unwrap_or_default(),
        },
    )
    .await?;

    println!("----------------------------------------");
    println!("タイトル: {}", item.title);
    println!("投稿者  : {}", item.by);
    if let Some(url) = item.url {
        println!("URL     : {}", url);
    }
    Ok(())
}

pub async fn fetch_top_stories(pool: &SqlitePool) -> Result<(), Box<dyn Error>> {
    let client = reqwest::Client::new();
    let base_url = "https://hacker-news.firebaseio.com";
    let top_stories_url = format!("{}/v0/topstories.json", base_url);
    let story_ids: Vec<u64> = client.get(top_stories_url).send().await?.json().await?;
    println!("取得したトップストーリーの総数: {}件", story_ids.len());

    let ids = filter_unsaved(pool, filter_top_stories(story_ids)).await?;
    if ids.is_empty() {
        println!("未保存のストーリーはありません");
        return Ok(());
    }
    println!("{}件の詳細を取得します...\n", ids.len());

    for chunk in ids.chunks(BATCH_SIZE) {
        let fetches = chunk.iter().map(|&id| {
            let client = client.clone();
            let base_url = base_url.to_string();
            async move {
                let result = fetch_item(&client, &base_url, id).await;
                (id, result)
            }
        });
        for (id, result) in join_all(fetches).await {
            match result {
                Ok(Some(item)) => save_item(pool, item).await?,
                Ok(None) => eprintln!("ID {} のパースに失敗しました", id),
                Err(e) => eprintln!("ID {} の取得に失敗しました: {}", id, e),
            }
        }
    }
    Ok(())
}
