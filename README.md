# wastest

Hacker News (および任意 URL) を入力に、LLM で「ステートメント (事実・主張)」と「コードブロック」を抽出し、
**FTS / VSS / Hybrid** で検索できるローカルファーストなナレッジ基盤。

ストレージは Apache Arrow ベースの **Lance** 形式。書き込みは Rust LanceDB、読み出しは
`lancedb` ネイティブ API (indexed search) と `DuckDB on Lance` (ad-hoc SQL) の 2 系統。

---

## Concepts

- **Structure over Probability** — 曖昧な summary は避け、LLM に厳密スキーマで statement / keyword / code を抽出させる。
- **Traceability** — 抽出物は全て元ソース (`content_id` = URL ハッシュ) に紐付き、原典に遡れる。
- **Local-First** — サーバ DB 不要。Lance ディレクトリと SQLite ファイルだけで完結。
- **Namespace 分離** — `{lance_dir}/{namespace}/` 配下に物理分離。HN 以外のドメイン知識も同じスキーマで蓄積可能。
- **No-noise Retrieval** — false negative を許容してでも false positive (ノイズ) を入れない。VSS cutoff + Hybrid 実マッチ要求。

---

## Architecture

```
┌───────────────────────────────────┐
│  HN API  /  任意 URL リスト         │
└───────────────┬───────────────────┘
                │
                ▼
       ┌──────────────────┐
       │  pipeline (Rust) │  Stream: fetch+parse → LLM extract → Writer Actor
       └────────┬─────────┘
                │
   ┌────────────┴─────────────┐
   ▼                          ▼
┌──────────────┐     ┌─────────────────────────────────────┐
│   SQLite     │     │  Lance datasets (per namespace)      │
│  hn_items    │     │  {lance_dir}/{ns}/                   │
│  (HN メタのみ) │     │   ├─ contents.lance                  │
└──────────────┘     │   ├─ statements.lance (FTS + IvfPq)  │
                     │   ├─ code_blocks.lance               │
                     │   └─ top_stories.lance (HN namespace) │
                     └─────────────┬───────────────────────┘
                                   │
                ┌──────────────────┴──────────────────┐
                ▼                                     ▼
       ┌───────────────────┐                ┌─────────────────────┐
       │  lancedb (native) │                │  DuckDB on Lance     │
       │  FTS / VSS /      │                │  ad-hoc SQL / JOIN   │
       │  Hybrid (RRF)     │                │  / 集計               │
       └───────────────────┘                └─────────────────────┘
```

### なぜ Lance か
- FTS (BM25) と Vector index (IvfPq) を **データセットに同梱**できる
- `_distance` / `_score` / `_relevance_score` 列がネイティブで返る
- DuckDB から `INSTALL lance; LOAD lance;` で透過的に SQL クエリ可能

### SQLite ⨝ Lance contents (HN namespace)
HN ルートでは `contents.id == hn_items.id (HN item_id)` で完全一致するので、`LanceReader::open` 時に
`DATABASE_URL` の SQLite を `hn` schema として自動 ATTACH する。1 本の DuckDB SQL で結合可能:

```sql
SELECT h.title, h.score, h.time, c.content
FROM hn.hn_items h
JOIN '{LANCE_DIR}/hn/contents.lance' c
  ON h.id = c.id
ORDER BY h.time DESC
LIMIT 50;
```

(非 HN namespace では SQLite が無くても ATTACH は best-effort で skip される)

---

## Modules

```
backend/src/
├── lib.rs            re-export (LanceStore, LanceReader, GeminiClient ...)
├── main.rs           HN 専用 entry (cron で叩く想定)
├── config.rs         env + namespace → lance_uri 解決
├── pipeline.rs       Stream pipeline + Writer Actor (channel-based)
├── parse.rs          HTML → 構造化テキスト
│
├── agent/            LLM プロバイダ抽象
│   ├── mod.rs        LlmProvider trait
│   ├── gemini.rs     gemini-3-flash-preview + gemini-embedding-2 (3072 dim)
│   ├── openai.rs     差し替え可
│   └── prompts.rs    extraction schema
│
├── db/               SQLite (HN メタ専用)
│   └── hn.rs         hn_items CRUD
│
├── lance/
│   ├── store.rs      LanceStore: 書き込み + index 管理
│   ├── reader.rs     LanceReader: lancedb + DuckDB の 2 系統 read
│   └── tables/       テーブルごと: SCHEMA / Row / to_batch
│       ├── contents.rs
│       ├── statements.rs
│       ├── code_blocks.rs
│       └── top_stories.rs
│
├── api/
│   └── hn.rs         HN ingest + run_hn_pipeline
│
└── bin/              CLI ツール群 (詳細は後述)
```

---

## Pipeline Flow (Writer Actor)

```
URLs ──▶ fetch+parse (buffer_unordered) ──▶ LLM extract (buffer_unordered) ──▶ mpsc ──▶ Writer Actor ──▶ Lance
```

- Stage 1 (fetch+parse) と Stage 2 (LLM 抽出) は並列、書き込みは単一 Writer
- バッチサイズは Lance fragment 効率を意識して調整
- 同じ URL は再ハッシュで id が決定的に決まり、`existing_content_ids()` で重複除去

---

## Retrieval

### Index 管理
- `LanceStore::ensure_indexes()` — `statements.statement` に FTS、`statements.embedding` に IvfPq (Cosine) を作成 (冪等)
- `LanceStore::optimize_indexes()` — append 後の未 index fragment を incremental に取り込む (Lance OSS は自動更新しないため必須)

### `LanceReader` の検索 API
| メソッド | 出力 | フィルタ |
|---------|------|---------|
| `search_fts(q, limit)` | `FtsHit { score (BM25) }` | なし |
| `search_vss(q_vec, limit)` | `VssHit { similarity (cosine) }` | `similarity >= VSS_SIM_CUTOFF (0.45)` |
| `search_hybrid(q, q_vec, limit)` | `HybridHit { relevance_score, fts_score, vss_similarity }` | VSS 寄与あり **or** FTS スコア > 0 |

- Distance ではなく **Cosine similarity** で統一 (higher = better)
- Hybrid の reranker は **RRF (k=60)** を明示指定
- 将来的に ColBERT cross-encoder reranker への置き換えを想定 (Rust 側未提供のため別サービス前提)

---

## CLI Bins

```sh
# HN pipeline (cron で叩く想定 = main.rs)
cargo run

# 任意 URL を namespace に投入
cargo run --bin ingest_urls -- <namespace> <url> [url ...]

# smoke (namespace=smoke, 毎回再生成)
cargo run --bin smoke_pipeline
cargo run --bin smoke_pipeline -- https://example.com/a https://example.com/b

# index 構築 + optimize (手動メンテ)
cargo run --bin refresh_indexes -- <namespace>

# 検索
cargo run --bin search_fts    -- <namespace> "<query>" [limit]
cargo run --bin search_vss    -- <namespace> "<query>" [limit]
cargo run --bin search_hybrid -- <namespace> "<query>" [limit]

# HN top_stories スナップショット履歴
cargo run --bin list_top_stories

# Lance 検証 (lancedb / DuckDB on Lance / lance_hybrid_search UDF を比較)
cargo run --bin lance_check -- <namespace> "<query>"

# 単体検証
cargo run --bin embed_check
cargo run --bin agent
```

---

## Configuration

`.env` (envy で読み込み):

```env
DATABASE_URL=sqlite://./wastest.db
LANCE_DIR=./data/lance
OPENAI_API_KEY=...
GEMINI_API_KEY=...
S3_BUCKET_ID=...
S3_ACCESS_KEY=...
S3_SECRET_KEY=...
```

Lance ディレクトリ配置: `{LANCE_DIR}/{namespace}/{contents,statements,code_blocks}.lance/`

---

## Roadmap
- Main pipeline
   - S3 への Lance dataset エクスポート (per day)
   - S3 アーカイブ読み出し(older than 90days)、Local読み出し(in 90days)
   - Cron 化 (Dev: GitHub Actions, Stable: Lambda + EventBridge)
- Option
   - ColBERT  reranker 統合 — rust lance crateに適用されてから
   - HN 以外の namespace 運用例 (例: 特定テーマ(Houdinie等)でのKnowledge蓄積)
