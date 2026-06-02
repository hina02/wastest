from pathlib import Path

import duckdb
import lancedb
from google import genai
from google.genai import types
from lancedb.rerankers import RRFReranker

from config import EMBEDDING_MODEL, GEMINI_API_KEY, LANCE_DIR

_genai = genai.Client(api_key=GEMINI_API_KEY)


def embed(text: str) -> list[float]:
    res = _genai.models.embed_content(
        model=EMBEDDING_MODEL,
        contents=text,
        config=types.EmbedContentConfig(task_type="RETRIEVAL_QUERY"),
    )
    return list(res.embeddings[0].values)


def _open(namespace: str):
    return lancedb.connect(f"{LANCE_DIR}/{namespace}")


def search_fts(namespace: str, query: str, limit: int = 10) -> list[dict]:
    tbl = _open(namespace).open_table("statements")
    return (
        tbl.search(query, query_type="fts")
        .select(["id", "content_id", "statement", "keywords"])
        .limit(limit)
        .to_list()
    )


def search_hybrid(namespace: str, query: str, limit: int = 10) -> list[dict]:
    q_vec = embed(query)
    tbl = _open(namespace).open_table("statements")
    return (
        tbl.search(query_type="hybrid")
        .vector(q_vec)
        .text(query)
        .rerank(RRFReranker(return_score="relevance"))
        .select(["id", "content_id", "statement", "keywords"])
        .limit(limit)
        .to_list()
    )


def get_content(namespace: str, statement_id: str) -> str:
    uri = f"{LANCE_DIR}/{namespace}"
    con = duckdb.connect()
    con.execute("INSTALL lance; LOAD lance;")

    row = con.execute(
        f"SELECT content_id FROM lance_scan('{uri}/statements') WHERE id = ?",
        [statement_id],
    ).fetchone()
    if not row:
        return "該当する statement が見つかりません"

    content_id = row[0]
    row2 = con.execute(
        f"SELECT content FROM lance_scan('{uri}/contents') WHERE id = ?",
        [content_id],
    ).fetchone()
    return row2[0] if row2 else "コンテンツが見つかりません"


def list_namespaces() -> list[str]:
    d = Path(LANCE_DIR)
    if not d.exists():
        return []
    return sorted(p.name for p in d.iterdir() if p.is_dir() and (p / "__manifest").exists())
