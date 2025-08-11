# storage.py
import os
import json
import sqlite3
from typing import Iterable, Optional, Tuple

DEFAULT_DB_PATH = os.getenv("GROK_DB_PATH", "grok_data/grok.sqlite3")

DDL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
CREATE TABLE IF NOT EXISTS tweets (
  id TEXT PRIMARY KEY,
  conversation_id TEXT,
  author_username TEXT,
  created_at TEXT,
  is_reply INTEGER,
  json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tweets_conversation ON tweets(conversation_id);

CREATE TABLE IF NOT EXISTS checkpoints (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
"""

def _connect(db_path: Optional[str] = None) -> sqlite3.Connection:
    path = db_path or DEFAULT_DB_PATH
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    conn = sqlite3.connect(path, isolation_level=None)  # autocommit mode; we’ll use explicit transactions
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

def init_db(db_path: Optional[str] = None) -> sqlite3.Connection:
    conn = _connect(db_path)
    with conn:
        for stmt in DDL.strip().split(";"):
            s = stmt.strip()
            if s:
                conn.execute(s + ";")
    return conn

def upsert_tweets(conn: sqlite3.Connection, tweets: Iterable[dict], batch_size: int = 500) -> int:
    """
    Upsert tweets by id. Returns number of *attempted* inserts (conflicts are ignored).
    Expects each tweet dict to contain at least: id, conversationId, author.userName, createdAt, isReply.
    Stores the full tweet json under `json`.
    """
    rows = []
    count = 0
    for t in tweets:
        if not isinstance(t, dict):
            continue
        tid = t.get("id")
        if not tid:
            continue
        rows.append((
            tid,
            t.get("conversationId"),
            (t.get("author") or {}).get("userName"),
            t.get("createdAt"),
            1 if t.get("isReply") else 0,
            json.dumps(t, ensure_ascii=False),
        ))
        if len(rows) >= batch_size:
            _do_upsert(conn, rows)
            count += len(rows)
            rows.clear()
    if rows:
        _do_upsert(conn, rows)
        count += len(rows)
    return count

def _do_upsert(conn: sqlite3.Connection, rows: list[Tuple]):
    with conn:  # transaction
        conn.executemany(
            """
            INSERT INTO tweets (id, conversation_id, author_username, created_at, is_reply, json)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO NOTHING
            """,
            rows
        )

def save_checkpoint(conn: sqlite3.Connection, key: str, value: str) -> None:
    with conn:
        conn.execute(
            "INSERT INTO checkpoints(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )

def load_checkpoint(conn: sqlite3.Connection, key: str) -> Optional[str]:
    cur = conn.execute("SELECT value FROM checkpoints WHERE key=?", (key,))
    row = cur.fetchone()
    return row[0] if row else None
