use crate::config::SETTINGS;
use duckdb::types::Value;
use duckdb::{Connection, Result};
use std::collections::HashSet;
use tracing::warn;

// ----------------------------------------------------------------
// Read 共通インタフェース
//
// Writer / Reader どちらからでも呼べる SELECT 系の操作をまとめる。
// 「DuckDB に読み専で触りたい」用途は `DuckDBReader` を使い、
// Pipeline のように書き込みもしたい用途は `DuckDBWriter` を使う。
// ----------------------------------------------------------------

pub trait DuckReadOps {
    fn conn(&self) -> &Connection;

    fn fetch_all_stories(&self) -> Result<Vec<(String, Vec<i64>)>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT fetched_at::VARCHAR, item_ids FROM top_stories ORDER BY fetched_at DESC",
        )?;
        let mut rows = stmt.query([])?;
        let mut results = Vec::new();
        while let Some(row) = rows.next()? {
            let fetched_at: String = row.get(0)?;
            let value: Value = row.get(1)?;
            let mut item_ids = Vec::new();
            if let Value::List(values) = value {
                for v in values {
                    if let Value::BigInt(id) = v {
                        item_ids.push(id);
                    }
                }
            }
            results.push((fetched_at, item_ids));
        }
        Ok(results)
    }

    /// 既に hn_contents に保存済みの id 集合。pipeline で URL を除外するのに使う。
    fn existing_content_ids(&self) -> Result<HashSet<i64>> {
        let mut stmt = self.conn().prepare("SELECT id FROM hn_contents")?;
        let mut rows = stmt.query([])?;
        let mut ids = HashSet::new();
        while let Some(row) = rows.next()? {
            ids.insert(row.get::<_, i64>(0)?);
        }
        Ok(ids)
    }
}

// ----------------------------------------------------------------
// Writer: 初期化 (DDL) + INSERT/UPDATE + S3 設定 + Index 作成
// ----------------------------------------------------------------

pub struct DuckDBWriter {
    conn: Connection,
}

impl DuckDBWriter {
    pub fn new(db_path: &str) -> Result<Self> {
        let conn = Connection::open(db_path)?;
        conn.execute_batch(include_str!("../../../ddl/duckdb.sql"))?;
        Ok(Self { conn })
    }

    /// 別タスク (Writer Actor 等) に渡す用の追加コネクション。
    pub fn try_clone_conn(&self) -> Result<Connection> {
        self.conn.try_clone()
    }

    pub fn insert_top_stories(&self, item_ids: &[i64]) -> Result<()> {
        let ids_str = item_ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!("INSERT INTO top_stories (item_ids) VALUES ([{}])", ids_str);
        self.conn.execute(&sql, [])?;
        Ok(())
    }
}

impl DuckReadOps for DuckDBWriter {
    fn conn(&self) -> &Connection {
        &self.conn
    }
}

// S3 関連 (Writer 専用: CREATE SECRET / COPY TO)
impl DuckDBWriter {
    pub fn setup_s3_environment(&self, access_key: &str, secret_key: &str) -> Result<()> {
        self.conn.execute_batch("INSTALL sqlite; LOAD sqlite;")?;

        let s3_query = format!(
            "
            INSTALL httpfs;
            LOAD httpfs;
            CREATE SECRET (
                TYPE S3,
                KEY_ID '{}',
                SECRET '{}',
                REGION 'ap-northeast-1'
            );
            ",
            access_key, secret_key
        );
        self.conn.execute_batch(&s3_query)?;
        Ok(())
    }

    /// SQLite のデータを S3 へ Hive パーティション形式でエクスポート
    pub fn export_to_s3_lake(&self) -> Result<()> {
        let generate_sql = format!(
            "SELECT format(
                'COPY (SELECT * FROM sqlite_scan(''wastest.db'', ''hn_items''))
                 TO ''s3://{}/archive/hn_items/year=%s/month=%s/day=%s/hn_items.parquet'' (FORMAT PARQUET, COMPRESSION ''ZSTD'');',
                strftime(current_timestamp, '%Y'),
                strftime(current_timestamp, '%m'),
                strftime(current_timestamp, '%d')
            );",
            SETTINGS.s3_bucket_id
        );

        let export_sql: String = self.conn.query_row(&generate_sql, [], |row| row.get(0))?;
        self.conn.execute_batch(&export_sql)?;
        Ok(())
    }

    /// S3 データレイク全体の全履歴から、各アイテムの最新状態を取得。
    /// `setup_s3_environment` の Secret 設定が必要なので Writer 側に置く。
    pub fn query_latest_from_s3_lake(
        &self,
        bucket_id: &str,
        limit: usize,
    ) -> Result<Vec<(i64, String, i64, String)>> {
        let read_sql = format!(
            "
            WITH ranked_items AS (
                SELECT
                    id,
                    title,
                    time,
                    year, month, day,
                    ROW_NUMBER() OVER (PARTITION BY id ORDER BY time DESC) as rn
                FROM 's3://{}/archive/hn_items/year=*/month=*/day=*/*.parquet'
            )
            SELECT id, title, time, (year || '-' || month || '-' || day) as archive_date
            FROM ranked_items
            WHERE rn = 1
            ORDER BY time DESC
            LIMIT {};
            ",
            bucket_id, limit
        );

        let mut stmt = self.conn.prepare(&read_sql)?;
        let mut rows = stmt.query([])?;
        let mut results = Vec::new();
        while let Some(row) = rows.next()? {
            results.push((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?));
        }
        Ok(results)
    }

    /// VSS extension を読み込む。bundled DuckDB と extension のバージョン不整合等で
    /// 失敗することがあるので、warn に留めて続行可能にしてある。
    /// `create_vector_index` を呼ぶ前にこれを呼ぶ。
    pub fn setup_vss(&self) -> Result<()> {
        if let Err(e) = self.conn.execute_batch("INSTALL vss; LOAD vss;") {
            warn!(error = %e, "setup_vss failed (network or version mismatch?)");
        }
        Ok(())
    }

    /// FTS extension を読み込む。失敗は warn のみ。
    pub fn setup_fts(&self) -> Result<()> {
        if let Err(e) = self.conn.execute_batch("INSTALL fts; LOAD fts;") {
            warn!(error = %e, "setup_fts failed (network or version mismatch?)");
        }
        Ok(())
    }

    /// HNSW ベクトルインデックスを作成。embedding 列が埋まった後に呼ぶ。
    /// 事前に `setup_vss` を呼んでおく必要がある。
    pub fn create_vector_index(&self) -> Result<()> {
        self.conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_statements_embedding
             ON statements USING HNSW (embedding);",
        )?;
        Ok(())
    }

    /// statements.statement と code_blocks.code_content の FTS index を作る。
    /// 事前に `setup_fts` を呼んでおく必要がある。
    /// DuckDB FTS の PRAGMA はデフォルトで既存 index があるとエラーになるので、
    /// 毎回上書き再構築するため `overwrite=1` を明示する。
    pub fn create_fts_indexes(&self) -> Result<()> {
        self.conn.execute_batch(
            "PRAGMA create_fts_index('statements', 'id', 'statement', overwrite=1);
             PRAGMA create_fts_index('code_blocks', 'id', 'code_content', overwrite=1);",
        )?;
        Ok(())
    }

    /// パイプライン後の FTS index リフレッシュ用エントリ。
    /// `setup_fts` の extension load 失敗は warn 留めにし、
    /// `create_fts_indexes` のエラーは握り潰さず呼び出し側に伝播させる
    /// (extension がロードできていれば PRAGMA は基本通るはず)。
    /// 将来 embedding が pipeline に組み込まれたら、本メソッドと
    /// VSS index 作成をまとめた `refresh_search_indexes` を別途追加する想定。
    pub fn refresh_fts_indexes(&self) -> Result<()> {
        self.setup_fts()?;
        self.create_fts_indexes()?;
        Ok(())
    }
}

// ----------------------------------------------------------------
// Reader: SELECT 限定の薄いラッパ
//
// 注意: Writer::new で DDL が流れていることを前提とする。
// Reader を先に作っても DB は作られるがテーブルがないので SELECT で失敗する。
// ----------------------------------------------------------------

pub struct DuckDBReader {
    conn: Connection,
}

impl DuckDBReader {
    pub fn new(db_path: &str) -> Result<Self> {
        let conn = Connection::open(db_path)?;
        Ok(Self { conn })
    }
}

impl DuckReadOps for DuckDBReader {
    fn conn(&self) -> &Connection {
        &self.conn
    }
}
