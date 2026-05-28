use chrono::Utc;
use sqlx::{Result, SqlitePool};

pub struct CreateHNItem {
    pub id: i64,
    pub item_type: String,
    pub by: String,
    pub time: i64,
    pub title: String,
    pub url: String,
}

pub async fn exists(pool: &SqlitePool, id: i64) -> Result<bool, sqlx::Error> {
    let result: i64 = sqlx::query_scalar!("SELECT EXISTS(SELECT 1 FROM hn_items WHERE id = ?)", id)
        .fetch_one(pool)
        .await?;
    Ok(result == 1)
}

pub async fn create_many(pool: &SqlitePool, items: &[CreateHNItem]) -> Result<(), sqlx::Error> {
    if items.is_empty() {
        return Ok(());
    }
    let placeholders = std::iter::repeat("(?, ?, ?, ?, ?, ?)")
        .take(items.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        r#"INSERT INTO hn_items (id, item_type, "by", time, title, url) VALUES {}"#,
        placeholders
    );
    let q = items.iter().fold(sqlx::query(&sql), |q, item| {
        q.bind(item.id)
            .bind(&item.item_type)
            .bind(&item.by)
            .bind(item.time)
            .bind(&item.title)
            .bind(&item.url)
    });
    q.execute(pool).await?;
    Ok(())
}

pub async fn delete_old_items(pool: &SqlitePool) -> Result<u64> {
    let now = Utc::now();
    let seven_days_ago = now - chrono::Duration::days(7);
    let threshold_time = seven_days_ago.timestamp();

    let result = sqlx::query!("DELETE FROM hn_items WHERE time <= ?", threshold_time)
        .execute(pool)
        .await?;

    Ok(result.rows_affected())
}

pub async fn list_urls(pool: &SqlitePool) -> Result<Vec<(i64, String)>, sqlx::Error> {
    let rows = sqlx::query!(r#"SELECT id, url FROM hn_items WHERE url != ''"#)
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().map(|r| (r.id, r.url)).collect())
}
