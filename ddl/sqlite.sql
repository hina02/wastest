CREATE TABLE IF NOT EXISTS hn_items (
    id INTEGER PRIMARY KEY NOT NULL,
    item_type TEXT NOT NULL,
    by TEXT NOT NULL,
    time INTEGER NOT NULL,
    title TEXT NOT NULL,
    url TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_items_time ON hn_items(time);

CREATE TABLE IF NOT EXISTS url_events (
    id         INTEGER PRIMARY KEY NOT NULL,
    url        TEXT    NOT NULL,
    namespace  TEXT    NOT NULL,
    status     TEXT    NOT NULL DEFAULT 'pending',
    error      TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_url_events_ns_status ON url_events(namespace, status);
