//! Lance フォーマット (LanceDB / Lance dataset) バックエンド。
//!
//! 構成:
//! - [`store`]  — 書き込み (`LanceStore`、append + index 構築 + writer-flow read)
//! - [`reader`] — 読み込み (`LanceReader`、lancedb 経由の検索 + DuckDB-on-Lance の SQL)
//! - [`tables`] — テーブル定義 (1 ファイル = 1 テーブルの schema + Row + 変換ヘルパー)
//!
//! 設計:
//! - 1 namespace = 1 親ディレクトリ + 配下に複数の `*.lance/` データセット
//! - スキーマの真実の源 (source of truth) は `tables::*::SCHEMA`
//! - Row 構造体も `tables::*::Row` (module-prefixed) で参照する

pub mod reader;
pub mod store;
pub mod tables;

pub use reader::{FtsHit, HybridHit, LanceReader, VssHit};
pub use store::{IVF_PQ_MIN_ROWS, LanceStore};