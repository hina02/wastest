//! `statements` テーブル定義。
//! Lance 上の schema:
//! `(id Utf8(=UUID), content_id BIGINT, statement Utf8,
//!   keywords List<Utf8>, embedding FixedSizeList<Float32, EMBEDDING_DIMS>)`

use crate::agent::gemini::EMBEDDING_DIMS;
use anyhow::Result;
use arrow_array::{
    FixedSizeListArray, Float32Array, Int64Array, ListArray, RecordBatch, StringArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Schema};
use std::sync::{Arc, LazyLock};
use uuid::Uuid;

pub const TBL: &str = "statements";

pub static SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false), // UUID as string
        Field::new("content_id", DataType::Int64, false),
        Field::new("statement", DataType::Utf8, false),
        Field::new(
            "keywords",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "embedding",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                EMBEDDING_DIMS as i32,
            ),
            true,
        ),
    ]))
});

#[derive(Debug)]
pub struct Row {
    pub id: Uuid,
    pub content_id: i64,
    pub statement: String,
    pub keywords: Vec<String>,
    pub embedding: Option<Vec<f32>>,
}

pub fn to_batch(rows: Vec<Row>) -> Result<RecordBatch> {
    let schema = SCHEMA.clone();
    let ids = StringArray::from_iter_values(rows.iter().map(|r| r.id.to_string()));
    let cids = Int64Array::from_iter_values(rows.iter().map(|r| r.content_id));
    let stmts = StringArray::from_iter_values(rows.iter().map(|r| r.statement.as_str()));
    let keywords = build_keywords_list(&rows);
    let embedding = build_embedding_fixed_list(&rows)?;
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ids),
            Arc::new(cids),
            Arc::new(stmts),
            Arc::new(keywords),
            Arc::new(embedding),
        ],
    )?)
}

fn build_keywords_list(rows: &[Row]) -> ListArray {
    let item_field = Arc::new(Field::new("item", DataType::Utf8, true));
    let mut values: Vec<String> = Vec::new();
    let mut offsets: Vec<i32> = Vec::with_capacity(rows.len() + 1);
    offsets.push(0);
    for r in rows {
        for kw in &r.keywords {
            values.push(kw.clone());
        }
        offsets.push(values.len() as i32);
    }
    let value_array = StringArray::from(values);
    ListArray::new(
        item_field,
        OffsetBuffer::new(offsets.into()),
        Arc::new(value_array),
        None,
    )
}

fn build_embedding_fixed_list(rows: &[Row]) -> Result<FixedSizeListArray> {
    let dim = EMBEDDING_DIMS;
    let item_field = Arc::new(Field::new("item", DataType::Float32, true));

    let mut flat: Vec<f32> = Vec::with_capacity(rows.len() * dim);
    let mut validity = Vec::with_capacity(rows.len());
    for r in rows {
        match &r.embedding {
            Some(v) => {
                if v.len() != dim {
                    anyhow::bail!("embedding dim mismatch: expected {dim}, got {}", v.len());
                }
                flat.extend_from_slice(v);
                validity.push(true);
            }
            None => {
                flat.extend(std::iter::repeat(0.0f32).take(dim));
                validity.push(false);
            }
        }
    }
    let values = Float32Array::from(flat);
    let nulls = NullBuffer::from(validity);
    Ok(FixedSizeListArray::new(
        item_field,
        dim as i32,
        Arc::new(values),
        Some(nulls),
    ))
}