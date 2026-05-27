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
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM hn_items WHERE id = ?")
        .bind(id)
        .fetch_one(pool)
        .await?;
    Ok(count.0 > 0)
}

pub async fn create(pool: &SqlitePool, item: &CreateHNItem) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO hn_items (id, item_type, "by", time, title, url)
        VALUES (?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(item.id)
    .bind(&item.item_type)
    .bind(&item.by)
    .bind(item.time)
    .bind(&item.title)
    .bind(&item.url)
    .execute(pool)
    .await?;
    Ok(())
}
