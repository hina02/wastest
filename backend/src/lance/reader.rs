//! Lance dataset 群への read-only アクセス。
//!
//! 1つの namespace ディレクトリに対し、以下 2 種類の read 接続を持つ:
//! - **lancedb 側**: `table.query()` 経由でネイティブ FTS / VSS / hybrid 検索
//! - **DuckDB 側**: `INSTALL lance; LOAD lance;` した in-memory connection で
//!   `.lance` を SQL から JOIN/集計/ad-hoc クエリ
//!
//! 書き込みは [`crate::lance::LanceStore`] が担当。本モジュールには INSERT 系は無い。

use crate::lance::tables::{code_blocks, contents, statements, top_stories};
use anyhow::{Context, Result};
use arrow_array::{Array, Float32Array, Int64Array, ListArray, StringArray};
use duckdb::Connection as DuckConnection;
use futures::TryStreamExt;
use lance_index::scalar::FullTextSearchQuery;
use lancedb::DistanceType;
use lancedb::query::{ExecutableQuery, QueryBase, QueryExecutionOptions};
use lancedb::rerankers::rrf::RRFReranker;
use lancedb::{Connection as LanceConnection, Table};
use std::sync::Arc;
use uuid::Uuid;

// ----------------------------------------------------------------
// 検索パラメータ
// ----------------------------------------------------------------

/// VSS の cosine similarity cutoff。これ未満は捨てる。
/// Gemini embedding でのチューニング値 (経験則)。
pub const VSS_SIM_CUTOFF: f32 = 0.45;

/// Hybrid 用: FTS `_score` がこれ以下なら「FTS が実質マッチしていない」とみなす。
/// VSS 寄与も無ければ hit ごと捨てる。
pub const FTS_SCORE_MIN: f32 = 0.0;

/// RRF reranker の k 定数。元論文 (Cormack 2009) で k=60 が near-optimal。
/// 将来的にColbert採用
pub const RRF_K: f32 = 60.0;

// ----------------------------------------------------------------
// 検索ヒット型 (lancedb が返す `_score` / `_distance` / `_relevance_score` を保持)
// ----------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct FtsHit {
    pub statement_id: Uuid,
    pub content_id: i64,
    pub statement: String,
    pub keywords: Vec<String>,
    /// BM25 スコア (lancedb の `_score` 列)
    pub score: f32,
}

#[derive(Debug, Clone)]
pub struct VssHit {
    pub statement_id: Uuid,
    pub content_id: i64,
    pub statement: String,
    pub keywords: Vec<String>,
    /// Cosine similarity = `1.0 - lancedb _distance`。範囲 [-1, 1]、高い=近い。
    pub similarity: f32,
}

#[derive(Debug, Clone)]
pub struct HybridHit {
    pub statement_id: Uuid,
    pub content_id: i64,
    pub statement: String,
    pub keywords: Vec<String>,
    /// lancedb の `_relevance_score` (FTS と VSS をマージした合成スコア)。
    pub relevance_score: f32,
    /// 内訳: FTS rank に基づく `_score`
    pub fts_score: Option<f32>,
    /// 内訳: VSS の cosine similarity (`1.0 - _distance`)
    pub vss_similarity: Option<f32>,
}

// ----------------------------------------------------------------
// LanceReader
// ----------------------------------------------------------------

pub struct LanceReader {
    uri: String,
    lance_db: LanceConnection,
    duck: DuckConnection,
}

impl LanceReader {
    /// `lance_uri = {lance_dir}/{namespace}` を渡す。
    /// env の `DATABASE_URL` が指す SQLite ファイルが存在すれば `hn` schema として ATTACH する
    /// (HN namespace で `hn.hn_items` ⨝ `contents.lance` を 1 つの DuckDB SQL で書けるように)。
    pub async fn open(uri: &str) -> Result<Self> {
        let lance_db = lancedb::connect(uri).execute().await?;
        let duck = DuckConnection::open_in_memory()?;
        duck.execute_batch("INSTALL lance; LOAD lance;")?;
        if let Err(e) = maybe_attach_hn_sqlite(&duck) {
            tracing::warn!(error = %e, "skip ATTACH hn sqlite");
        }
        Ok(Self {
            uri: uri.to_string(),
            lance_db,
            duck,
        })
    }

    /// HN namespace 用: `hn.hn_items` と `contents.lance` を id (= HN item_id) で JOIN した行数。
    /// ATTACH 失敗 / 非 HN namespace のときは Err。
    pub fn count_hn_with_content(&self) -> Result<i64> {
        let path = self.contents_path();
        let sql = format!(
            "SELECT COUNT(*) FROM hn.hn_items h JOIN '{path}' c ON h.id = c.id"
        );
        self.scalar_i64(&sql).with_context(|| "JOIN hn_items × contents")
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    // ---- DuckDB-on-Lance: 任意 SQL / 集計 ----

    /// `<uri>/<table>.lance` の物理パス。
    pub fn dataset_path(&self, table: &str) -> String {
        format!("{}/{}.lance", self.uri, table)
    }
    pub fn contents_path(&self) -> String {
        self.dataset_path(contents::TBL)
    }
    pub fn statements_path(&self) -> String {
        self.dataset_path(statements::TBL)
    }
    pub fn code_blocks_path(&self) -> String {
        self.dataset_path(code_blocks::TBL)
    }
    pub fn top_stories_path(&self) -> String {
        self.dataset_path(top_stories::TBL)
    }

    /// 任意 SQL を実行し、最初の i64 カラムを 1 件だけ取り出す簡易ヘルパ。
    pub fn scalar_i64(&self, sql: &str) -> Result<i64> {
        let v: i64 = self.duck.query_row(sql, [], |r| r.get(0))?;
        Ok(v)
    }

    /// 指定テーブルの行数を返す。
    pub fn count(&self, table: &str) -> Result<i64> {
        let path = self.dataset_path(table);
        let sql = format!("SELECT COUNT(*) FROM '{path}'");
        self.scalar_i64(&sql)
            .with_context(|| format!("count {table}"))
    }

    /// 任意の SQL クエリを直接叩きたい場合の生 connection (ad-hoc 用)。
    pub fn duck(&self) -> &DuckConnection {
        &self.duck
    }

    /// top_stories 全履歴を fetched_at 降順で返す。 (HN namespace のみ）
    pub async fn fetch_all_top_stories(&self) -> Result<Vec<(String, Vec<i64>)>> {
        let tbl = self.lance_db.open_table(top_stories::TBL).execute().await?;
        let mut stream = tbl.query().execute().await?;
        let mut rows = Vec::new();
        while let Some(batch) = stream.try_next().await? {
            rows.extend(top_stories::extract_rows(&batch)?);
        }
        rows.sort_by(|a, b| b.0.cmp(&a.0));
        Ok(rows)
    }

    // ---- lancedb ネイティブ検索 ----

    async fn statements_table(&self) -> Result<Table> {
        Ok(self.lance_db.open_table(statements::TBL).execute().await?)
    }

    /// statements.statement に対する FTS (BM25) 検索。
    pub async fn search_fts(&self, query: &str, limit: usize) -> Result<Vec<FtsHit>> {
        let tbl = self.statements_table().await?;
        let mut stream = tbl
            .query()
            .full_text_search(FullTextSearchQuery::new(query.to_owned()))
            .limit(limit)
            .execute()
            .await?;
        let mut hits = Vec::new();
        while let Some(b) = stream.try_next().await? {
            let ids = downcast_str(&b, "id")?;
            let cids = downcast_i64(&b, "content_id")?;
            let stmts = downcast_str(&b, "statement")?;
            let scores = downcast_f32_opt(&b, "_score");
            let kws = b.column_by_name("keywords").cloned();
            for i in 0..b.num_rows() {
                hits.push(FtsHit {
                    statement_id: parse_uuid(ids.value(i)),
                    content_id: cids.value(i),
                    statement: stmts.value(i).to_string(),
                    keywords: keywords_at(&kws, i),
                    score: scores.as_ref().map(|s| s.value(i)).unwrap_or(f32::NAN),
                });
            }
        }
        Ok(hits)
    }

    /// statements.embedding に対する VSS (最近傍) 検索 (cosine)。
    /// `VSS_SIM_CUTOFF` 未満の hit は捨てる。
    pub async fn search_vss(&self, query_vec: Vec<f32>, limit: usize) -> Result<Vec<VssHit>> {
        let tbl = self.statements_table().await?;
        let mut stream = tbl
            .query()
            .nearest_to(query_vec)?
            .distance_type(DistanceType::Cosine)
            .limit(limit)
            .execute()
            .await?;
        let mut hits = Vec::new();
        while let Some(b) = stream.try_next().await? {
            let ids = downcast_str(&b, "id")?;
            let cids = downcast_i64(&b, "content_id")?;
            let stmts = downcast_str(&b, "statement")?;
            let dists = downcast_f32_opt(&b, "_distance");
            let kws = b.column_by_name("keywords").cloned();
            for i in 0..b.num_rows() {
                let d = dists.as_ref().map(|s| s.value(i)).unwrap_or(f32::NAN);
                let sim = 1.0 - d;
                if sim < VSS_SIM_CUTOFF {
                    continue;
                }
                hits.push(VssHit {
                    statement_id: parse_uuid(ids.value(i)),
                    content_id: cids.value(i),
                    statement: stmts.value(i).to_string(),
                    keywords: keywords_at(&kws, i),
                    similarity: sim,
                });
            }
        }
        Ok(hits)
    }

    /// FTS + VSS のハイブリッド検索 (lancedb の `execute_hybrid`)。
    pub async fn search_hybrid(
        &self,
        query_text: &str,
        query_vec: Vec<f32>,
        limit: usize,
    ) -> Result<Vec<HybridHit>> {
        let tbl = self.statements_table().await?;
        let mut stream = tbl
            .query()
            .full_text_search(FullTextSearchQuery::new(query_text.to_owned()))
            .nearest_to(query_vec)?
            .distance_type(DistanceType::Cosine)
            .rerank(Arc::new(RRFReranker::new(RRF_K)))
            .limit(limit)
            .execute_hybrid(QueryExecutionOptions::default())
            .await?;
        let mut hits = Vec::new();
        while let Some(b) = stream.try_next().await? {
            let ids = downcast_str(&b, "id")?;
            let cids = downcast_i64(&b, "content_id")?;
            let stmts = downcast_str(&b, "statement")?;
            let kws = b.column_by_name("keywords").cloned();
            let relevance = downcast_f32_opt(&b, "_relevance_score");
            let fts_scores = downcast_f32_opt(&b, "_score");
            let vss_dists = downcast_f32_opt(&b, "_distance");
            for i in 0..b.num_rows() {
                hits.push(HybridHit {
                    statement_id: parse_uuid(ids.value(i)),
                    content_id: cids.value(i),
                    statement: stmts.value(i).to_string(),
                    keywords: keywords_at(&kws, i),
                    relevance_score: relevance.as_ref().map(|s| s.value(i)).unwrap_or(f32::NAN),
                    fts_score: fts_scores.as_ref().map(|s| s.value(i)),
                    vss_similarity: vss_dists.as_ref().map(|s| 1.0 - s.value(i)),
                });
            }
        }
        // 「VSS hit 有り」or「FTS が実質マッチ」のいずれかを満たすものだけ keep
        hits.retain(|h| {
            h.vss_similarity.is_some() || h.fts_score.map_or(false, |s| s > FTS_SCORE_MIN)
        });
        Ok(hits)
    }
}

// ----------------------------------------------------------------
// 内部ヘルパ
// ----------------------------------------------------------------

fn downcast_str(b: &arrow_array::RecordBatch, name: &str) -> Result<StringArray> {
    let col = b
        .column_by_name(name)
        .with_context(|| format!("column {name} missing"))?;
    Ok(col
        .as_any()
        .downcast_ref::<StringArray>()
        .with_context(|| format!("column {name} not Utf8"))?
        .clone())
}

fn downcast_i64(b: &arrow_array::RecordBatch, name: &str) -> Result<Int64Array> {
    let col = b
        .column_by_name(name)
        .with_context(|| format!("column {name} missing"))?;
    Ok(col
        .as_any()
        .downcast_ref::<Int64Array>()
        .with_context(|| format!("column {name} not Int64"))?
        .clone())
}

/// score / distance 系列のオプショナル downcast (列が存在しない or 型違いなら None)。
fn downcast_f32_opt(b: &arrow_array::RecordBatch, name: &str) -> Option<Float32Array> {
    b.column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<Float32Array>())
        .cloned()
}

fn keywords_at(col: &Option<std::sync::Arc<dyn Array>>, row: usize) -> Vec<String> {
    col.as_ref()
        .and_then(|c| c.as_any().downcast_ref::<ListArray>())
        .map(|la| extract_string_list(la, row))
        .unwrap_or_default()
}

fn extract_string_list(la: &ListArray, row: usize) -> Vec<String> {
    if la.is_null(row) {
        return Vec::new();
    }
    let start = la.value_offsets()[row] as usize;
    let end = la.value_offsets()[row + 1] as usize;
    let values = la.values();
    let strs = values
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("List<Utf8>");
    (start..end).map(|i| strs.value(i).to_string()).collect()
}

fn parse_uuid(s: &str) -> Uuid {
    Uuid::parse_str(s).unwrap_or_else(|_| Uuid::nil())
}

/// best-effort: env の DATABASE_URL が指す SQLite ファイルが存在すれば DuckDB に ATTACH する。
/// 失敗ケース (env 無し / ファイル無し / 拡張ロード失敗) は呼び出し側で warn 留めにする。
fn maybe_attach_hn_sqlite(duck: &DuckConnection) -> Result<()> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        return Ok(());
    };
    let path = url.strip_prefix("sqlite://").unwrap_or(&url);
    if !std::path::Path::new(path).exists() {
        return Ok(());
    }
    duck.execute_batch(&format!(
        "INSTALL sqlite; LOAD sqlite; ATTACH '{path}' AS hn (TYPE sqlite);"
    ))?;
    Ok(())
}
