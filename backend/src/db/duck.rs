use crate::config::SETTINGS;
use duckdb::params;
use duckdb::types::Value;
use duckdb::{Connection, Result};
use std::collections::{HashMap, HashSet};
use tracing::warn;
use uuid::Uuid;

/// VSS の cosine **similarity** に対する閾値 (高いほど近い、OpenAI 系の慣習と同じ)。
/// 内部は DuckDB の `array_cosine_distance` で計算し、Rust 側で `similarity = 1 - distance` に
/// 変換してから判定する (HNSW index は cosine distance で構築されているのでクエリは
/// distance のまま、ORDER BY が変わらないのでインデックスも効く)。
///
/// - similarity >= hard           → Confident (高精度マッチ)
/// - soft <= similarity < hard    → Marginal (周辺マッチ)
/// - similarity < soft            → 結果から除外
///
/// 値は Gemini embedding-2 の similarity 分布を観察して暫定設定したもの。
/// OpenAI text-embedding-3 系で慣れていた 0.70/0.72 はそのままだと厳しすぎる
/// (Gemini は systematically similarity が低めに出る)。実データが増えたら再調整。
pub const VSS_SOFT_SIMILARITY: f64 = 0.45;
pub const VSS_HARD_SIMILARITY: f64 = 0.55;

/// RRF (Reciprocal Rank Fusion) の定数。標準値は 60。
const RRF_K: f64 = 60.0;

/// RRF に使う各ランキングの取得件数。
/// `search_hybrid` の `limit` よりも十分大きく取って融合する。
const HYBRID_FETCH: usize = 50;

/// FTS 検索の1ヒット。
#[derive(Debug, Clone)]
pub struct FtsHit {
    pub statement_id: Uuid,
    pub content_id: i64,
    pub statement: String,
    pub keywords: Vec<String>,
    pub score: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VssConfidence {
    /// similarity >= hard 閾値。高精度のマッチ。
    Confident,
    /// soft <= similarity < hard 閾値。周辺マッチ (UI 等で別表示するのを想定)。
    Marginal,
}

/// VSS (ベクトル) 検索の1ヒット。`similarity` は cosine 類似度 (大きいほど近い、0..1)。
#[derive(Debug, Clone)]
pub struct VssHit {
    pub statement_id: Uuid,
    pub content_id: i64,
    pub statement: String,
    pub keywords: Vec<String>,
    pub similarity: f64,
    pub confidence: VssConfidence,
}

/// FTS と VSS を Reciprocal Rank Fusion でマージした1ヒット。
#[derive(Debug, Clone)]
pub struct HybridHit {
    pub statement_id: Uuid,
    pub content_id: i64,
    pub statement: String,
    pub keywords: Vec<String>,
    pub rrf_score: f64,
    pub fts_rank: Option<usize>,
    pub vss_rank: Option<usize>,
    pub vss_similarity: Option<f64>,
}

// ----------------------------------------------------------------
// Read 共通インタフェース
//
// Writer / Reader どちらからでも呼べる SELECT 系の操作をまとめる。
// 「DuckDB に読み専で触りたい」用途は `DuckDBReader` を使い、
// Pipeline のように書き込みもしたい用途は `DuckDBWriter` を使う。
// ----------------------------------------------------------------

pub trait DuckReadOps {
    fn conn(&self) -> &Connection;

    fn fetch_all_stories(&self) -> Result<Vec<(String, Vec<i64>)>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT fetched_at::VARCHAR, item_ids FROM top_stories ORDER BY fetched_at DESC",
        )?;
        let mut rows = stmt.query([])?;
        let mut results = Vec::new();
        while let Some(row) = rows.next()? {
            let fetched_at: String = row.get(0)?;
            let value: Value = row.get(1)?;
            let mut item_ids = Vec::new();
            if let Value::List(values) = value {
                for v in values {
                    if let Value::BigInt(id) = v {
                        item_ids.push(id);
                    }
                }
            }
            results.push((fetched_at, item_ids));
        }
        Ok(results)
    }

    /// 既に `contents` に保存済みの id 集合。
    /// pipeline で URL を除外するのに使う。
    fn existing_content_ids(&self) -> Result<HashSet<i64>> {
        let mut stmt = self.conn().prepare("SELECT id FROM contents")?;
        let mut rows = stmt.query([])?;
        let mut ids = HashSet::new();
        while let Some(row) = rows.next()? {
            ids.insert(row.get::<_, i64>(0)?);
        }
        Ok(ids)
    }

    /// statements.embedding に対する VSS (cosine similarity) 検索。
    /// 事前に呼び出し側が VSS extension をロードし、クエリベクトルを
    /// statements.embedding と同じ次元 (Gemini embedding-2 = 3072) の `&[f32]` で
    /// 渡す必要がある。HNSW index がなくても動くが、その場合は full scan になる。
    ///
    /// 結果は similarity `>= VSS_SOFT_SIMILARITY` のものに絞り、
    /// `>= VSS_HARD_SIMILARITY` を `Confident`、それ未満を `Marginal` でラベル付け。
    fn search_statements_vss(&self, query_vec: &[f32], limit: usize) -> Result<Vec<VssHit>> {
        let dim = query_vec.len();
        let vec_lit: Vec<String> = query_vec.iter().map(|x| x.to_string()).collect();
        // クエリベクトルは List binding 不可なので SQL に直接埋め込み。
        // similarity フィルタは Rust 側で early break する (HNSW の order に乗ったまま処理)。
        let sql = format!(
            "SELECT id, content_id, statement, keywords,
                    array_cosine_distance(embedding, [{vec}]::FLOAT[{dim}]) AS dist
             FROM statements
             WHERE embedding IS NOT NULL
             ORDER BY dist ASC
             LIMIT ?",
            vec = vec_lit.join(","),
        );
        let mut stmt = self.conn().prepare(&sql)?;
        let mut rows = stmt.query(params![limit as i64])?;
        let mut hits = Vec::new();
        while let Some(row) = rows.next()? {
            let statement_id: Uuid = row.get(0)?;
            let content_id: i64 = row.get(1)?;
            let statement: String = row.get(2)?;
            let kw_value: Value = row.get(3)?;
            let distance: f64 = row.get(4)?;
            let similarity = 1.0 - distance;
            if similarity < VSS_SOFT_SIMILARITY {
                // ORDER BY distance ASC = similarity DESC なので、ここで切れば以降も全部 soft 未満。
                break;
            }
            let confidence = if similarity >= VSS_HARD_SIMILARITY {
                VssConfidence::Confident
            } else {
                VssConfidence::Marginal
            };
            hits.push(VssHit {
                statement_id,
                content_id,
                statement,
                keywords: extract_text_list(kw_value),
                similarity,
                confidence,
            });
        }
        Ok(hits)
    }

    /// statements.statement に対する FTS (BM25) 検索。
    /// 事前に `refresh_fts_indexes` (= setup_fts + create_fts_indexes) を済ませておく必要がある。
    /// score は BM25 スコア。降順で `limit` 件返す。
    fn search_statements_fts(&self, query: &str, limit: usize) -> Result<Vec<FtsHit>> {
        // DuckDB FTS は対象テーブル毎に `fts_main_<table>.match_bm25(id_col, query)` 関数を生やす。
        // 副問い合わせで score を計算し、NULL (= ヒットなし) を除外して並べる。
        let mut stmt = self.conn().prepare(
            "SELECT id, content_id, statement, keywords, score
             FROM (
                 SELECT
                     id,
                     content_id,
                     statement,
                     keywords,
                     fts_main_statements.match_bm25(id, ?) AS score
                 FROM statements
             ) t
             WHERE score IS NOT NULL
             ORDER BY score DESC
             LIMIT ?",
        )?;
        let mut rows = stmt.query(params![query, limit as i64])?;
        let mut hits = Vec::new();
        while let Some(row) = rows.next()? {
            let statement_id: Uuid = row.get(0)?;
            let content_id: i64 = row.get(1)?;
            let statement: String = row.get(2)?;
            let kw_value: Value = row.get(3)?;
            let score: f64 = row.get(4)?;
            hits.push(FtsHit {
                statement_id,
                content_id,
                statement,
                keywords: extract_text_list(kw_value),
                score,
            });
        }
        Ok(hits)
    }

    /// FTS + VSS を Reciprocal Rank Fusion (RRF) でマージしたハイブリッド検索。
    ///
    /// 各検索で内部的に上位 `HYBRID_FETCH` 件を取り、`statement_id` をキーに重複排除しつつ
    /// `rrf_score = sum over rankings: 1 / (RRF_K + rank)` でスコアリングし、降順に `limit` 件返す。
    ///
    /// VSS 側は通常の `search_statements_vss` を経由するため soft 閾値 (`VSS_SOFT_SIMILARITY`)
    /// 未満の類似度は混ぜない。
    fn search_hybrid(
        &self,
        query_text: &str,
        query_vec: &[f32],
        limit: usize,
    ) -> Result<Vec<HybridHit>> {
        let fts = self.search_statements_fts(query_text, HYBRID_FETCH)?;
        let vss = self.search_statements_vss(query_vec, HYBRID_FETCH)?;

        let mut merged: HashMap<Uuid, HybridHit> = HashMap::new();

        for (rank0, h) in fts.into_iter().enumerate() {
            let rank = rank0 + 1;
            let id = h.statement_id;
            let entry = merged.entry(id).or_insert_with(|| HybridHit {
                statement_id: id,
                content_id: h.content_id,
                statement: h.statement,
                keywords: h.keywords,
                rrf_score: 0.0,
                fts_rank: None,
                vss_rank: None,
                vss_similarity: None,
            });
            entry.fts_rank = Some(rank);
            entry.rrf_score += 1.0 / (RRF_K + rank as f64);
        }

        for (rank0, h) in vss.into_iter().enumerate() {
            let rank = rank0 + 1;
            let id = h.statement_id;
            let sim = h.similarity;
            let entry = merged.entry(id).or_insert_with(|| HybridHit {
                statement_id: id,
                content_id: h.content_id,
                statement: h.statement,
                keywords: h.keywords,
                rrf_score: 0.0,
                fts_rank: None,
                vss_rank: None,
                vss_similarity: None,
            });
            entry.vss_rank = Some(rank);
            entry.vss_similarity = Some(sim);
            entry.rrf_score += 1.0 / (RRF_K + rank as f64);
        }

        let mut results: Vec<HybridHit> = merged.into_values().collect();
        results.sort_by(|a, b| {
            b.rrf_score
                .partial_cmp(&a.rrf_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(limit);
        Ok(results)
    }
}

/// DuckDB の List(Text) を `Vec<String>` に flatten する小ヘルパ。
fn extract_text_list(v: Value) -> Vec<String> {
    match v {
        Value::List(items) => items
            .into_iter()
            .filter_map(|x| {
                if let Value::Text(t) = x {
                    Some(t)
                } else {
                    None
                }
            })
            .collect(),
        _ => Vec::new(),
    }
}

// ----------------------------------------------------------------
// Writer: 初期化 (DDL) + INSERT/UPDATE + S3 設定 + Index 作成
// ----------------------------------------------------------------

pub struct DuckDBWriter {
    conn: Connection,
}

impl DuckDBWriter {
    pub fn new(db_path: &str) -> Result<Self> {
        let conn = Connection::open(db_path)?;
        conn.execute_batch(include_str!("../../../ddl/duckdb.sql"))?;
        let writer = Self { conn };
        // statements に HNSW index がある場合、INSERT/UPDATE/DELETE は VSS extension を
        // 要求するので、初期化時にロードしておく (DDL から外したのは「コアスキーマ作成を
        // 拡張に依存させない」ため、ここでは index 操作可能性を確保するためにロードする)。
        writer.setup_vss()?;
        Ok(writer)
    }

    /// 別タスク (Writer Actor 等) に渡す用の追加コネクション。
    /// extension は connection ごとに LOAD が必要なので、ここで VSS をロードする
    /// (statements への INSERT/DELETE が HNSW index を経由するため必須)。
    /// ロード失敗は warn 留め (extension が無い環境でも全体は壊さない)。
    pub fn try_clone_conn(&self) -> Result<Connection> {
        let conn = self.conn.try_clone()?;
        if let Err(e) = conn.execute_batch("INSTALL vss; LOAD vss;") {
            warn!(error = %e, "failed to load vss on cloned connection");
        }
        Ok(conn)
    }

    pub fn insert_top_stories(&self, item_ids: &[i64]) -> Result<()> {
        let ids_str = item_ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!("INSERT INTO top_stories (item_ids) VALUES ([{}])", ids_str);
        self.conn.execute(&sql, [])?;
        Ok(())
    }
}

impl DuckReadOps for DuckDBWriter {
    fn conn(&self) -> &Connection {
        &self.conn
    }
}

// S3 関連 (Writer 専用: CREATE SECRET / COPY TO)
impl DuckDBWriter {
    pub fn setup_s3_environment(&self, access_key: &str, secret_key: &str) -> Result<()> {
        self.conn.execute_batch("INSTALL sqlite; LOAD sqlite;")?;

        let s3_query = format!(
            "
            INSTALL httpfs;
            LOAD httpfs;
            CREATE SECRET (
                TYPE S3,
                KEY_ID '{}',
                SECRET '{}',
                REGION 'ap-northeast-1'
            );
            ",
            access_key, secret_key
        );
        self.conn.execute_batch(&s3_query)?;
        Ok(())
    }

    /// SQLite の `hn_items` を S3 へ Hive パーティション形式でエクスポート。
    /// path: `s3://{bucket}/archive/{namespace}/hn_items/year=YYYY/month=MM/day=DD/hn_items.parquet`
    ///
    /// 現状 `hn_items` は HN 専属テーブルなので呼び出し側は `namespace="hn"` を渡すことが多いが、
    /// 別の SQLite ingest source ができた場合に備えて引数化してある。
    /// DuckDB 側の contents/statements/code_blocks の export は別関数として追加予定。
    pub fn export_hn_items_to_s3(&self, namespace: &str) -> Result<()> {
        let generate_sql = format!(
            "SELECT format(
                'COPY (SELECT * FROM sqlite_scan(''wastest.db'', ''hn_items''))
                 TO ''s3://{}/archive/{namespace}/hn_items/year=%s/month=%s/day=%s/hn_items.parquet'' (FORMAT PARQUET, COMPRESSION ''ZSTD'');',
                strftime(current_timestamp, '%Y'),
                strftime(current_timestamp, '%m'),
                strftime(current_timestamp, '%d')
            );",
            SETTINGS.s3_bucket_id
        );

        let export_sql: String = self.conn.query_row(&generate_sql, [], |row| row.get(0))?;
        self.conn.execute_batch(&export_sql)?;
        Ok(())
    }

    /// S3 データレイクの `hn_items` 履歴から、各アイテムの最新状態を取得。
    /// `setup_s3_environment` の Secret 設定が必要なので Writer 側に置く。
    /// `namespace` は path 上の prefix (例: "hn") で、`export_hn_items_to_s3` と対称的に指定する。
    pub fn query_latest_hn_items_from_s3(
        &self,
        namespace: &str,
        bucket_id: &str,
        limit: usize,
    ) -> Result<Vec<(i64, String, i64, String)>> {
        let read_sql = format!(
            "
            WITH ranked_items AS (
                SELECT
                    id,
                    title,
                    time,
                    year, month, day,
                    ROW_NUMBER() OVER (PARTITION BY id ORDER BY time DESC) as rn
                FROM 's3://{bucket_id}/archive/{namespace}/hn_items/year=*/month=*/day=*/*.parquet'
            )
            SELECT id, title, time, (year || '-' || month || '-' || day) as archive_date
            FROM ranked_items
            WHERE rn = 1
            ORDER BY time DESC
            LIMIT {limit};
            "
        );

        let mut stmt = self.conn.prepare(&read_sql)?;
        let mut rows = stmt.query([])?;
        let mut results = Vec::new();
        while let Some(row) = rows.next()? {
            results.push((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?));
        }
        Ok(results)
    }

    /// VSS extension を読み込み、永続 DB ファイルでも HNSW を作れるようにする。
    /// `hnsw_enable_experimental_persistence` は Connection ごとにセットが必要なので
    /// 検索 Reader 側でも同じ手順を踏むこと。
    /// 失敗は warn 留めにして続行可能にする (extension が無い環境でも全体は壊れない)。
    ///
    /// execute_batch に複数 statement を渡すと後段が黙ってスキップされる場合があるので
    /// 個別 call に分割している。
    pub fn setup_vss(&self) -> Result<()> {
        if let Err(e) = self.conn.execute_batch("INSTALL vss;") {
            warn!(error = %e, "INSTALL vss failed");
            return Ok(());
        }
        if let Err(e) = self.conn.execute_batch("LOAD vss;") {
            warn!(error = %e, "LOAD vss failed");
            return Ok(());
        }
        if let Err(e) = self
            .conn
            .execute_batch("SET hnsw_enable_experimental_persistence = true;")
        {
            warn!(error = %e, "SET hnsw_enable_experimental_persistence failed");
        }
        Ok(())
    }

    /// FTS extension を読み込む。失敗は warn のみ。
    pub fn setup_fts(&self) -> Result<()> {
        if let Err(e) = self.conn.execute_batch("INSTALL fts; LOAD fts;") {
            warn!(error = %e, "setup_fts failed (network or version mismatch?)");
        }
        Ok(())
    }

    /// HNSW ベクトルインデックスを作成。embedding 列が埋まった後に呼ぶ。
    /// 事前に `setup_vss` を呼んでおく必要がある。
    /// `IF NOT EXISTS` で冪等なので、pipeline 末尾で毎回呼んで問題ない。
    /// metric = 'cosine': テキスト埋め込みは cosine 距離が標準
    /// (Gemini embedding は正規化済みなので inner_product でも等価)。
    pub fn create_vector_index(&self) -> Result<()> {
        self.conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_statements_embedding
             ON statements USING HNSW (embedding)
             WITH (metric = 'cosine');",
        )?;
        Ok(())
    }

    /// statements.statement と code_blocks.code_content の FTS index を作る。
    /// 事前に `setup_fts` を呼んでおく必要がある。
    /// DuckDB FTS の PRAGMA はデフォルトで既存 index があるとエラーになるので、
    /// 毎回上書き再構築するため `overwrite=1` を明示する。
    pub fn create_fts_indexes(&self) -> Result<()> {
        self.conn.execute_batch(
            "PRAGMA create_fts_index('statements', 'id', 'statement', overwrite=1);
             PRAGMA create_fts_index('code_blocks', 'id', 'code_content', overwrite=1);",
        )?;
        Ok(())
    }

    /// パイプライン後の FTS index リフレッシュ用エントリ。
    /// `setup_fts` の extension load 失敗は warn 留めにし、
    /// `create_fts_indexes` のエラーは握り潰さず呼び出し側に伝播させる
    /// (extension がロードできていれば PRAGMA は基本通るはず)。
    pub fn refresh_fts_indexes(&self) -> Result<()> {
        self.setup_fts()?;
        self.create_fts_indexes()?;
        Ok(())
    }

    /// FTS + HNSW の両方をまとめてリフレッシュ。pipeline 末尾用。
    /// FTS は必ず再構築 (snapshot 型)、HNSW は IF NOT EXISTS の冪等作成。
    /// VSS extension のロードに失敗した場合は HNSW 作成を skip して FTS だけ残す。
    pub fn refresh_search_indexes(&self) -> Result<()> {
        self.refresh_fts_indexes()?;
        self.setup_vss()?;
        if let Err(e) = self.create_vector_index() {
            warn!(error = %e, "create_vector_index failed; FTS only");
        }
        Ok(())
    }
}

// ----------------------------------------------------------------
// Reader: SELECT 限定の薄いラッパ
//
// 注意: Writer::new で DDL が流れていることを前提とする。
// Reader を先に作っても DB は作られるがテーブルがないので SELECT で失敗する。
// ----------------------------------------------------------------

pub struct DuckDBReader {
    conn: Connection,
}

impl DuckDBReader {
    pub fn new(db_path: &str) -> Result<Self> {
        let conn = Connection::open(db_path)?;
        Ok(Self { conn })
    }
}

impl DuckReadOps for DuckDBReader {
    fn conn(&self) -> &Connection {
        &self.conn
    }
}
