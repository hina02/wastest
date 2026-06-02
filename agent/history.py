from pathlib import Path

import duckdb

from config import HISTORY_DB


class History:
    def __init__(self):
        Path(HISTORY_DB).parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(HISTORY_DB)
        self._init()

    def _init(self):
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS chat_messages (
                session_id VARCHAR,
                ts         TIMESTAMPTZ DEFAULT now(),
                role       VARCHAR,
                content    TEXT
            )
        """)
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS search_log (
                ts          TIMESTAMPTZ DEFAULT now(),
                namespace   VARCHAR,
                query       TEXT,
                search_type VARCHAR,
                hit_count   INTEGER
            )
        """)

    def load(self, session_id: str) -> list[dict]:
        rows = self.con.execute(
            "SELECT role, content FROM chat_messages WHERE session_id = ? ORDER BY ts",
            [session_id],
        ).fetchall()
        return [{"role": r, "content": c} for r, c in rows]

    def append(self, session_id: str, role: str, content: str):
        self.con.execute(
            "INSERT INTO chat_messages(session_id, role, content) VALUES (?, ?, ?)",
            [session_id, role, content],
        )

    def clear(self, session_id: str):
        self.con.execute(
            "DELETE FROM chat_messages WHERE session_id = ?", [session_id]
        )

    def log_search(self, namespace: str, query: str, search_type: str, hit_count: int):
        self.con.execute(
            "INSERT INTO search_log(namespace, query, search_type, hit_count) VALUES (?, ?, ?, ?)",
            [namespace, query, search_type, hit_count],
        )

    def list_sessions(self) -> list[str]:
        rows = self.con.execute(
            "SELECT DISTINCT session_id FROM chat_messages ORDER BY 1"
        ).fetchall()
        return [r[0] for r in rows]
