//! Lance dataset 群への書き込みを担う `LanceStore`。
//!
//! 1 namespace = 1 親ディレクトリ + 配下に複数の `*.lance/` データセット。
//! 各テーブルのスキーマ・Row 型・Arrow batch 変換は `tables::*` 配下のモジュールに集約してあり、
//! `LanceStore` は orchestration (open / append / index 構築 / writer-flow 読み) のみを担当する。

use crate::lance::tables::{code_blocks, contents, statements, top_stories};
use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use futures::TryStreamExt;
use lancedb::DistanceType;
use lancedb::index::Index;
use lancedb::index::scalar::FtsIndexBuilder;
use lancedb::index::vector::IvfPqIndexBuilder;
use lancedb::query::{ExecutableQuery, QueryBase};
use lancedb::table::OptimizeAction;
use lancedb::{Connection, Table};
use std::collections::HashSet;
use std::sync::Arc;

/// IvfPq の PQ 学習に必要な最小行数 (lance-index の builder 制約)。
pub const IVF_PQ_MIN_ROWS: usize = 256;

pub struct LanceStore {
    /// `{lance_dir}/{namespace}` 配下に `.lance/` ディレクトリ群が並ぶ。
    pub uri: String,
    db: Connection,
}

impl LanceStore {
    /// 汎用 namespace 向け: contents / code_blocks / statements の 3 テーブルを保証する。
    pub async fn open(uri: &str) -> Result<Self> {
        let db = lancedb::connect(uri).execute().await?;
        let store = Self {
            uri: uri.to_string(),
            db,
        };
        store
            .ensure_table(contents::TBL, contents::SCHEMA.clone())
            .await?;
        store
            .ensure_table(code_blocks::TBL, code_blocks::SCHEMA.clone())
            .await?;
        store
            .ensure_table(statements::TBL, statements::SCHEMA.clone())
            .await?;
        Ok(store)
    }

    /// HN namespace 向け: `top_stories` テーブルを追加で保証する。
    pub async fn open_hn(uri: &str) -> Result<Self> {
        let store = Self::open(uri).await?;
        store
            .ensure_table(top_stories::TBL, top_stories::SCHEMA.clone())
            .await?;
        Ok(store)
    }

    async fn ensure_table(&self, name: &str, schema: Arc<Schema>) -> Result<()> {
        let names = self.db.table_names().execute().await?;
        if names.iter().any(|n| n == name) {
            return Ok(());
        }
        let empty = RecordBatch::new_empty(schema);
        self.db
            .create_table(name, empty)
            .execute()
            .await
            .with_context(|| format!("create_table {name}"))?;
        Ok(())
    }

    async fn open_table(&self, name: &str) -> Result<Table> {
        Ok(self.db.open_table(name).execute().await?)
    }

    async fn append(&self, name: &str, batch: RecordBatch) -> Result<()> {
        let tbl = self.open_table(name).await?;
        tbl.add(batch).execute().await?;
        Ok(())
    }

    // ------------- INSERT 系 -------------

    pub async fn insert_contents(&self, rows: Vec<contents::Row>) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let batch = contents::to_batch(rows)?;
        self.append(contents::TBL, batch).await
    }

    pub async fn insert_code_blocks(&self, rows: Vec<code_blocks::Row>) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let batch = code_blocks::to_batch(rows)?;
        self.append(code_blocks::TBL, batch).await
    }

    pub async fn insert_statements(&self, rows: Vec<statements::Row>) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let batch = statements::to_batch(rows)?;
        self.append(statements::TBL, batch).await
    }

    /// HN top_stories の現在時刻スナップショットを 1 行 append する。
    pub async fn insert_top_stories(&self, item_ids: &[i64]) -> Result<()> {
        let batch = top_stories::snapshot_to_batch(item_ids)?;
        self.append(top_stories::TBL, batch).await
    }

    // ------------- writer-flow read (dedupe 用) -------------

    /// `contents.id` HashSet。pipeline で 既知URL を除外するのに使う。
    pub async fn existing_content_ids(&self) -> Result<HashSet<i64>> {
        let tbl = self.open_table(contents::TBL).await?;
        let mut stream = tbl
            .query()
            .select(lancedb::query::Select::Columns(vec!["id".to_owned()]))
            .execute()
            .await?;
        let mut ids = HashSet::new();
        while let Some(batch) = stream.try_next().await? {
            for id in contents::extract_ids(&batch)? {
                ids.insert(id);
            }
        }
        Ok(ids)
    }

    // ------------- Index 管理 -------------

    /// FTS index と Vector (IvfPq) index を構築する (冪等)。
    /// - Lance(OOS) では append 後の新 fragment は index に含まれない。手動Optimize必要。
    /// - IvfPq は PQ 学習に最低 `IVF_PQ_MIN_ROWS` 行必要なので、それ未満は skip
    pub async fn ensure_indexes(&self) -> Result<()> {
        let tbl = self.open_table(statements::TBL).await?;

        if let Err(e) = tbl
            .create_index(&["statement"], Index::FTS(FtsIndexBuilder::default()))
            .execute()
            .await
        {
            tracing::warn!(error = %e, "create FTS index (may already exist)");
        }

        let n = tbl.count_rows(None).await?;
        if n < IVF_PQ_MIN_ROWS {
            tracing::info!(
                rows = n,
                threshold = IVF_PQ_MIN_ROWS,
                "skip IvfPq index (insufficient rows for PQ training)"
            );
            return Ok(());
        }
        if let Err(e) = tbl
            .create_index(
                &["embedding"],
                Index::IvfPq(
                    IvfPqIndexBuilder::default()
                        .distance_type(DistanceType::Cosine)
                        .num_partitions(16),
                ),
            )
            .execute()
            .await
        {
            tracing::warn!(error = %e, "create IvfPq index (may already exist)");
        }
        Ok(())
    }

    /// 既存 index に未 index fragment を incremental に取り込む (冪等)。
    /// 公式の目安: 約10万行の変更、または 20 回の append ごとに 1 回。
    pub async fn optimize_indexes(&self) -> Result<()> {
        let tbl = self.open_table(statements::TBL).await?;
        match tbl
            .optimize(OptimizeAction::Index(Default::default()))
            .await
        {
            Ok(_) => tracing::info!("optimize statements indexes"),
            Err(e) => {
                tracing::warn!(error = %e, "optimize statements indexes (may have no index yet)")
            }
        }
        Ok(())
    }
}
