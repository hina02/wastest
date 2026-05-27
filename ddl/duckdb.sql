INSTALL sqlite;
LOAD sqlite;

CREATE TABLE IF NOT EXISTS top_stories (
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    item_ids BIGINT[]
);

CREATE INDEX IF NOT EXISTS idx_top_stories_fetched_at ON top_stories(fetched_at);