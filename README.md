Hacker News API
Event Sourcing
SQLte + S3, Iceberg + DuckDB

## Write
Hacker news API fetch (per hour)
item append to sqlite　(url textは保留。後で別テーブルでOK)
item_ids to duckdb

## Archive
S3 Parquet Commit (per day)

## Read
DuckDB on S3
DuckDB on SQLite