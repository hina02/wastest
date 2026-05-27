use sqlx::SqlitePool;
pub struct CreateHNItem {
    pub id: i64,
    pub item_type: String,
    pub by: String,
    pub time: i64,
    pub title: String,
    pub url: String,
}

pub async fn exists(pool: &SqlitePool, id: i64) -> Result<bool, sqlx::Error> {
    let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM hn_items WHERE id = ?)")
        .bind(id)
        .fetch_one(pool)
        .await?;
    Ok(exists)
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
