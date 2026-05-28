-- コアスキーマ。`DuckDBWriter::new` で実行される。
-- VSS / FTS extension の INSTALL/LOAD は実行時の bundled DuckDB バージョンに
-- 依存するので、ここでは扱わない。
-- なので VSS なしでもカラム自体は作れる。
-- HNSW index / FTS index の作成は DuckDBWriter::create_vector_index() /
-- create_fts_indexes() を必要なタイミングで明示的に呼ぶ。

INSTALL sqlite;
LOAD sqlite;

CREATE TABLE IF NOT EXISTS top_stories (
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    item_ids BIGINT[]
);

CREATE INDEX IF NOT EXISTS idx_top_stories_fetched_at ON top_stories(fetched_at);

CREATE TABLE IF NOT EXISTS hn_contents (
    id BIGINT PRIMARY KEY NOT NULL,
    url TEXT NOT NULL,
    content TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS statements (
    id UUID PRIMARY KEY NOT NULL,
    content_id BIGINT NOT NULL,
    statement TEXT NOT NULL,
    keywords TEXT[],
    embedding FLOAT[3072]
);

CREATE TABLE IF NOT EXISTS code_blocks (
    id UUID PRIMARY KEY NOT NULL,
    content_id BIGINT NOT NULL,
    code_content TEXT NOT NULL
);
