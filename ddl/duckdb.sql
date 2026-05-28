-- コアスキーマ。`DuckDBWriter::new` で実行される。
--
-- 設計:
-- - 1 namespace = 1 DuckDB ファイル (物理分離)。ファイルパスは呼び出し側が決める。
-- - スキーマ・テーブル名は全 namespace で共通。`namespace` 列は持たない。
-- - クロス namespace のクエリが必要になれば DuckDB の ATTACH で対応する。
--
-- VSS / FTS extension の INSTALL/LOAD と HNSW/FTS index 作成は実行時の
-- bundled DuckDB バージョンに依存するので、ここでは扱わない。
-- 必要なタイミングで `setup_vss` / `create_vector_index` / `create_fts_indexes` を呼ぶ。

INSTALL sqlite;
LOAD sqlite;

-- HN 専用 (HN namespace の DuckDB ファイルにのみ存在することを想定)
CREATE TABLE IF NOT EXISTS top_stories (
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    item_ids BIGINT[]
);

CREATE INDEX IF NOT EXISTS idx_top_stories_fetched_at ON top_stories(fetched_at);

CREATE TABLE IF NOT EXISTS contents (
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
