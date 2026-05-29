//! `contents` テーブル定義。
//! Lance 上の schema は `(id BIGINT, url Utf8, content Utf8)`。

use anyhow::{Context, Result};
use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use std::sync::{Arc, LazyLock};

pub const TBL: &str = "contents";

pub static SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("url", DataType::Utf8, false),
        Field::new("content", DataType::Utf8, false),
    ]))
});

#[derive(Debug)]
pub struct Row {
    pub id: i64,
    pub url: String,
    pub content: String,
}

pub fn to_batch(rows: Vec<Row>) -> Result<RecordBatch> {
    let schema = SCHEMA.clone();
    let ids = Int64Array::from_iter_values(rows.iter().map(|r| r.id));
    let urls = StringArray::from_iter_values(rows.iter().map(|r| r.url.as_str()));
    let contents = StringArray::from_iter_values(rows.iter().map(|r| r.content.as_str()));
    Ok(RecordBatch::try_new(
        schema,
        vec![Arc::new(ids), Arc::new(urls), Arc::new(contents)],
    )?)
}

/// `id` 列のみを batch から取り出す (writer の dedupe 用)。
pub fn extract_ids(batch: &RecordBatch) -> Result<Vec<i64>> {
    let col = batch
        .column_by_name("id")
        .context("contents.id missing")?;
    let arr = col
        .as_any()
        .downcast_ref::<Int64Array>()
        .context("contents.id not Int64")?;
    let mut ids = Vec::with_capacity(arr.len());
    for i in 0..arr.len() {
        if arr.is_valid(i) {
            ids.push(arr.value(i));
        }
    }
    Ok(ids)
}