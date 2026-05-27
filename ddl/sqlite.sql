CREATE TABLE IF NOT EXISTS hn_items (
    id INTEGER PRIMARY KEY,
    item_type TEXT NOT NULL,
    by TEXT NOT NULL,
    time INTEGER NOT NULL,
    title TEXT NOT NULL,
    url TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_items_time ON hn_items(time);
