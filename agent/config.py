import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / "backend" / ".env")

LANCE_DIR: str = os.environ["LANCE_DIR"]
GEMINI_API_KEY: str = os.environ["GEMINI_API_KEY"]
HISTORY_DB: str = os.path.expanduser(os.getenv("HISTORY_DB", "~/.wastest/history.duckdb"))

EMBEDDING_MODEL = "gemini-embedding-2"
EMBEDDING_DIMS = 3072
CHAT_MODEL = "google-gla:gemini-2.5-flash"
