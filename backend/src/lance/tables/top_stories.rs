//! `top_stories` テーブル定義 (HN namespace でのみ使う)。
//! Lance 上の schema は `(fetched_at Timestamp(μs), item_ids List<Int64>)`。

use anyhow::{Context, Result};
use arrow_array::{Array, Int64Array, ListArray, RecordBatch, TimestampMicrosecondArray};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use std::sync::{Arc, LazyLock};

pub const TBL: &str = "top_stories";

pub static SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new(
            "fetched_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new(
            "item_ids",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            false,
        ),
    ]))
});

/// 現在時刻のスナップショット 1 行を `RecordBatch` に変換する。
pub fn snapshot_to_batch(item_ids: &[i64]) -> Result<RecordBatch> {
    let schema = SCHEMA.clone();
    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0);
    let ts = TimestampMicrosecondArray::from_iter_values([now_us]);

    let item_field = Arc::new(Field::new("item", DataType::Int64, true));
    let values = Int64Array::from_iter_values(item_ids.iter().copied());
    let offsets = OffsetBuffer::new(vec![0i32, item_ids.len() as i32].into());
    let list = ListArray::new(item_field, offsets, Arc::new(values), None);

    Ok(RecordBatch::try_new(schema, vec![Arc::new(ts), Arc::new(list)])?)
}

/// batch の各行を `(fetched_at, item_ids)` のタプル列に展開する (reader 用)。
pub fn extract_rows(batch: &RecordBatch) -> Result<Vec<(String, Vec<i64>)>> {
    let ts = batch
        .column_by_name("fetched_at")
        .context("fetched_at column")?
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .context("fetched_at not TimestampMicrosecond")?;
    let lists = batch
        .column_by_name("item_ids")
        .context("item_ids column")?
        .as_any()
        .downcast_ref::<ListArray>()
        .context("item_ids not List")?;
    let mut rows = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        let secs = ts.value(i) / 1_000_000;
        let nanos = ((ts.value(i) % 1_000_000) * 1000) as u32;
        let ts_str = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nanos)
            .map(|d| d.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
            .unwrap_or_default();
        rows.push((ts_str, extract_i64_list(lists, i)));
    }
    Ok(rows)
}

fn extract_i64_list(la: &ListArray, row: usize) -> Vec<i64> {
    if la.is_null(row) {
        return Vec::new();
    }
    let start = la.value_offsets()[row] as usize;
    let end = la.value_offsets()[row + 1] as usize;
    let values = la.values();
    let arr = values
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("List<Int64>");
    (start..end).map(|i| arr.value(i)).collect()
}