//! Lance dataset 上の各テーブル定義 (1 テーブル = 1 モジュール)。
//!
//! 各サブモジュールは以下を公開する:
//! - `TBL: &str` — Lance dataset 名 (= サブディレクトリ `<TBL>.lance/`)
//! - `SCHEMA: LazyLock<Arc<Schema>>` — Arrow schema (source of truth)
//! - `Row` — 1 レコード分のメモリ表現 (pipeline などからも参照される)
//! - `to_batch(rows: Vec<Row>) -> Result<RecordBatch>` — 書き込み用
//! - 必要に応じて `extract_*` ヘルパー (writer-flow read や reader 用)

pub mod code_blocks;
pub mod contents;
pub mod statements;
pub mod top_stories;