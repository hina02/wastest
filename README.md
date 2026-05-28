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

## s3 architecture
s3://{bucket_id}/
 └── archive/
      ├── hn_items/       <-- 元データのメタデータ
      │    ├── year=2026/month=05/day=27/data.parquet
      │    └── year=2026/month=05/day=28/data.parquet
      │
      └── url_texts/      <-- 重いスクレイピングデータ
           ├── year=2026/month=05/day=27/data.parquet
           └── year=2026/month=05/day=28/data.parquet


per hour
HN取得してsqlite更新
ids読み取りは、SQlite + duckdbから？

per day
hn_items を全件アーカイブへ
「作成/更新から 7日経過したレコード」をSQLiteから単純に DELETE

query
ローカルのSQLite（直近7日間の超最新データ）と、S3上のParquet（アーカイブ）を、1つのSQLで透過的に結合（UNION）

## Cron jobs
Dev Phase: GitHub Actions
Stable Phase: Lambda + EventBridge

## 🚀 Architecture Expansion Plan (アーキテクチャ拡張構想)

現在のHacker News(HN)取得基盤をベースに、LLM Agentを用いた「ステートメント（ファクト）抽出」と「ローカルファーストな高速検索」を組み合わせたナレッジベースへと拡張します。

### 📌 Core Concepts
- **Structure over Probability**: 曖昧な要約（Summary）を避け、Agentに厳格なスキーマで「ステートメント（事実・主張）」と「キーワード」を抽出させます。
- **Traceability**: 抽出されたステートメントは全て元のHNの `item_id` と紐付き、常に一次ソースへ遡れるようにします。
- **Local-First & Edge Native**: サーバーサイドに重いDBを置かず、DuckDB (Parquet) や LanceDB を活用し、クライアント・エッジ側で高速にソート・検索できる基盤を構築します。

### 🔄 Data Pipeline
1. **Fetch & Parse (Rust)**
2. **Agent Extraction (Rig-core + tokio)**
3. **Storage & Search (DuckDB / LanceDB)**
   - **コード検索**
   - **メタデータ・キーワード検索**: DuckDB + Parquetファイルにより、過去のニュースを高速に全文検索・ソート。
   - **セマンティック検索（ベクトル検索）**: ステートメントのEmbeddingベクトルを保存し、クエリの意味に近い過去のニュースを検索。
   - 上記2つを `item_id` でJOINし、瞬時に元の記事を特定する。

#### LanceDB Arrow format 


## Extend
YouTube Data API v3
with gemini-embedding-2 