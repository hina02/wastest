CREATE TABLE hn_items (
    id INTEGER PRIMARY KEY,
    item_type TEXT NOT NULL,
    by TEXT NOT NULL,
    time INTEGER NOT NULL,
    title TEXT NOT NULL,
    url TEXT NOT NULL
);

CREATE INDEX idx_item_id ON hn_events(item_id);

-- DuckDB
-- CREATE TABLE top_stories (
--     fetched_at TIMESTAMP, -- 取得した日時
--     item_ids UINTEGER[],    -- itemsテーブルのid
-- );
