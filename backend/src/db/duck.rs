use duckdb::types::Value;
use duckdb::{Connection, Result};

pub struct DuckDBClient {
    pub conn: Connection,
}

impl DuckDBClient {
    pub fn new(db_path: &str) -> Result<Self> {
        let conn = Connection::open(db_path)?;
        conn.execute_batch(include_str!("../../../ddl/duckdb.sql"))?;
        Ok(Self { conn })
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

    pub fn fetch_all_stories(&self) -> Result<Vec<(String, Vec<i64>)>> {
        let mut stmt = self.conn.prepare(
            "SELECT fetched_at::VARCHAR, item_ids FROM top_stories ORDER BY fetched_at DESC",
        )?;

        let mut rows = stmt.query([])?;
        let mut results = Vec::new();

        while let Some(row) = rows.next()? {
            let fetched_at: String = row.get(0)?;

            // 1. 一度 duckdb::types::Value として取得
            let value: Value = row.get(1)?;
            let mut item_ids = Vec::new();

            // 2. Value が List(配列) であることを確認して内部のBigIntを取り出す
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
}

// s3 upload
impl DuckDBClient {
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

    /// Task 2 (Write): SQLite のデータを S3 へ Hive パーティション形式でエクスポート
    pub fn export_to_s3_lake(&self, bucket_id: &str) -> Result<()> {
        // DuckDBに動的にコピー文を作らせて実行する堅牢なアプローチ
        let generate_sql = format!(
            "SELECT format(
                'COPY (SELECT * FROM sqlite_scan(''wastest.db'', ''hn_items'')) 
                 TO ''s3://{}/archive/hn_items/year=%s/month=%s/day=%s/hn_items.parquet'' (FORMAT PARQUET, COMPRESSION ''ZSTD'');',
                strftime(current_timestamp, '%Y'),
                strftime(current_timestamp, '%m'),
                strftime(current_timestamp, '%d')
            );",
            bucket_id
        );

        let export_sql: String = self.conn.query_row(&generate_sql, [], |row| row.get(0))?;
        self.conn.execute_batch(&export_sql)?;
        Ok(())
    }

    /// Task 2 (Read): S3 データレイク全体の全履歴から、各アイテムの最新状態を動的解決して取得
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
}
