//! SQLite ベースの DB アクセス層。
//! 現状は HN ingest 用の `hn_items` テーブル CRUD のみ。
//! Lance フォーマット側は [`crate::lance`] を参照。

pub mod chat;
pub mod hn;