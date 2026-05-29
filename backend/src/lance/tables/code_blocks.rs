//! `code_blocks` テーブル定義。
//! Lance 上の schema は `(id Utf8(=UUID), content_id BIGINT, code_content Utf8)`。

use anyhow::Result;
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use std::sync::{Arc, LazyLock};
use uuid::Uuid;

pub const TBL: &str = "code_blocks";

pub static SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false), // UUID as string
        Field::new("content_id", DataType::Int64, false),
        Field::new("code_content", DataType::Utf8, false),
    ]))
});

#[derive(Debug)]
pub struct Row {
    pub id: Uuid,
    pub content_id: i64,
    pub code: String,
}

pub fn to_batch(rows: Vec<Row>) -> Result<RecordBatch> {
    let schema = SCHEMA.clone();
    let ids = StringArray::from_iter_values(rows.iter().map(|r| r.id.to_string()));
    let cids = Int64Array::from_iter_values(rows.iter().map(|r| r.content_id));
    let codes = StringArray::from_iter_values(rows.iter().map(|r| r.code.as_str()));
    Ok(RecordBatch::try_new(
        schema,
        vec![Arc::new(ids), Arc::new(cids), Arc::new(codes)],
    )?)
}