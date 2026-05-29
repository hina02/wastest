//! Lance / LanceDB の write/read split が動くかの実機検証。
//!
//! ```
//! cargo run --bin lance_check
//! ```
//!
//! 流れ:
//! 1. (Writer 側) lancedb::connect → create_table → create_index (FTS + Vector)
//! 2. (Writer 側) lancedb の table.query() で hybrid search 動作確認
//! 3. (Reader 側) DuckDB を別途立てて `INSTALL lance; LOAD lance;`
//!    `.lance` ファイルを SQL で読み戻し
//! 4. (Reader 側) `lance_hybrid_search()` テーブル関数の動作確認

use anyhow::Result;
use arrow_array::{
    FixedSizeListArray, Float32Array, Int64Array, RecordBatch, RecordBatchIterator, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use duckdb::Connection as DuckConn;
use futures::TryStreamExt;
use lance_index::scalar::FullTextSearchQuery;
use lancedb::index::Index;
use lancedb::index::scalar::FtsIndexBuilder;
use lancedb::index::vector::IvfPqIndexBuilder;
use arrow_array::RecordBatchReader;
use lancedb::query::{QueryBase, QueryExecutionOptions};
use std::sync::Arc;

const EMB_DIM: usize = 8;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let tmp = tempfile::tempdir()?;
    let uri = tmp.path().to_string_lossy().to_string();
    let table_dir = format!("{uri}/statements.lance");
    println!("--- workspace: {uri} ---\n");

    // ============================================================
    // Phase 1: Writer (lancedb) で create_table + add + create_index
    // ============================================================
    println!("--- Phase 1: lancedb writer ---");

    let db = lancedb::connect(&uri).execute().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("statement", DataType::Utf8, false),
        Field::new(
            "embedding",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                EMB_DIM as i32,
            ),
            true,
        ),
    ]));

    // サンプル: 512 件投入 (IvfPq の PQ 学習に最低 256 行必要なので余裕を持って 512)
    let n = 512usize;
    let ids = Int64Array::from_iter_values((0..n as i64).map(|i| i + 1));
    let texts = StringArray::from_iter_values((0..n).map(|i| match i % 4 {
        0 => format!("DuckDB is an in-process OLAP database #{}", i),
        1 => format!("Lance is a columnar vector store format #{}", i),
        2 => format!("Hacker News uses Arc, a Lisp dialect #{}", i),
        _ => format!("Embeddings are stored as fixed-size float arrays #{}", i),
    }));
    // 簡易 embedding: id によって少しずつズラした 8 次元ベクトル
    let mut emb_values: Vec<f32> = Vec::with_capacity(n * EMB_DIM);
    for i in 0..n {
        for k in 0..EMB_DIM {
            emb_values.push(((i * EMB_DIM + k) as f32).sin());
        }
    }
    let emb_flat = Float32Array::from(emb_values);
    let emb = FixedSizeListArray::try_new(
        Arc::new(Field::new("item", DataType::Float32, true)),
        EMB_DIM as i32,
        Arc::new(emb_flat),
        None,
    )?;

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(ids), Arc::new(texts), Arc::new(emb)],
    )?;
    let batches: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
        vec![Ok(batch)].into_iter(),
        schema.clone(),
    ));

    println!("  create_table 'statements' ({n} rows)");
    let table = db.create_table("statements", batches).execute().await?;

    println!("  create_index FTS on statement");
    table
        .create_index(&["statement"], Index::FTS(FtsIndexBuilder::default()))
        .execute()
        .await?;

    println!("  create_index IvfPq on embedding (num_partitions=4)");
    table
        .create_index(
            &["embedding"],
            Index::IvfPq(IvfPqIndexBuilder::default().num_partitions(4)),
        )
        .execute()
        .await?;

    // ============================================================
    // Phase 2: Writer 側 (lancedb) で hybrid search 動作確認
    // ============================================================
    println!("\n--- Phase 2: lancedb hybrid search ---");
    let q_text = "Lance vector store";
    let q_vec: Vec<f32> = (0..EMB_DIM).map(|k| (k as f32).sin()).collect();
    let mut results = table
        .query()
        .full_text_search(FullTextSearchQuery::new(q_text.to_owned()))
        .nearest_to(q_vec.clone())?
        .limit(5)
        .execute_hybrid(QueryExecutionOptions::default())
        .await?;
    while let Some(b) = results.try_next().await? {
        let ids = b
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>().cloned())
            .unwrap();
        let stmts = b
            .column_by_name("statement")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>().cloned())
            .unwrap();
        for i in 0..b.num_rows() {
            println!("  [{}] {}", ids.value(i), stmts.value(i));
        }
    }

    // ============================================================
    // Phase 3: Reader 側 (DuckDB) で .lance を SQL で読み戻し
    // ============================================================
    println!("\n--- Phase 3: DuckDB read via lance extension ---");
    let dconn = DuckConn::open_in_memory()?;
    dconn.execute_batch("INSTALL lance; LOAD lance;")?;

    let count: i64 = dconn.query_row(
        &format!("SELECT COUNT(*) FROM '{table_dir}'"),
        [],
        |r| r.get(0),
    )?;
    println!("  SELECT COUNT(*) FROM '*.lance' -> {count}");

    let mut stmt = dconn.prepare(&format!(
        "SELECT id, statement FROM '{table_dir}' ORDER BY id LIMIT 3"
    ))?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let id: i64 = row.get(0)?;
        let s: String = row.get(1)?;
        println!("  [{id}] {s}");
    }

    // ============================================================
    // Phase 4: DuckDB lance extension の lance_hybrid_search() 関数
    // ============================================================
    println!("\n--- Phase 4: DuckDB lance_hybrid_search() ---");
    let q_vec_lit: String = q_vec
        .iter()
        .map(|x| x.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT id, _hybrid_score, _distance, _score
         FROM lance_hybrid_search(
             '{table_dir}',
             'embedding', [{q_vec_lit}]::FLOAT[{EMB_DIM}],
             'statement', '{q_text}',
             k = 5
         )
         ORDER BY _hybrid_score DESC"
    );
    match dconn.prepare(&sql) {
        Ok(mut stmt) => {
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                let id: i64 = row.get(0)?;
                let hs: f64 = row.get(1).unwrap_or(f64::NAN);
                let dist: f64 = row.get(2).unwrap_or(f64::NAN);
                let bm25: f64 = row.get(3).unwrap_or(f64::NAN);
                let sim = 1.0 - dist;
                println!("  [{id}] hybrid={hs:.4}  sim={sim:.4}  bm25={bm25:.4}");
            }
        }
        Err(e) => {
            println!("  lance_hybrid_search prepare failed: {e}");
            println!("  (フォールバック: 関数名やシグネチャが違う可能性あり。下記で関数列挙)");
            let mut s = dconn.prepare(
                "SELECT function_name FROM duckdb_functions()
                 WHERE function_name LIKE 'lance_%' ORDER BY function_name",
            )?;
            let mut r = s.query([])?;
            while let Some(row) = r.next()? {
                let f: String = row.get(0)?;
                println!("    found: {f}");
            }
        }
    }

    println!("\nALL PHASES executed.");
    Ok(())
}
