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
