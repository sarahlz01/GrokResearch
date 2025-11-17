# storage.py
import os
import json
import sqlite3
from typing import Iterable, Optional, Tuple
from datetime import datetime


BASE_DDL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
CREATE TABLE IF NOT EXISTS tweets (
  id TEXT PRIMARY KEY,
  conversation_id TEXT,
  author_username TEXT,
  created_at TEXT,
  created_at_ts INTEGER,
  is_reply INTEGER,
  is_grok_reply INTEGER,
  parent_id TEXT,
  json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tweets_conversation ON tweets(conversation_id);
CREATE INDEX IF NOT EXISTS idx_tweets_created ON tweets(created_at_ts);
"""

CHECKPOINTS_DDL = """
CREATE TABLE IF NOT EXISTS checkpoints (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
"""

def _connect(db_path: Optional[str] = None) -> sqlite3.Connection:
    path = db_path
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    conn = sqlite3.connect(path, isolation_level=None)
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

def _ensure_schema(conn: sqlite3.Connection):
    with conn:
        for stmt in BASE_DDL.strip().split(";"):
            s = stmt.strip()
            if s:
                conn.execute(s + ";")
        for stmt in CHECKPOINTS_DDL.strip().split(";"):
            s = stmt.strip()
            if s:
                conn.execute(s + ";")
    # migration for older DBs missing new columns
    cur = conn.execute("PRAGMA table_info(tweets)")
    cols = {r[1] for r in cur.fetchall()}
    missing = []
    if "parent_id" not in cols: missing.append(("parent_id", "TEXT"))
    if "created_at_ts" not in cols: missing.append(("created_at_ts", "INTEGER"))
    if "is_grok_reply" not in cols: missing.append(("is_grok_reply", "INTEGER"))
    if missing:
        with conn:
            for name, typ in missing:
                try:
                    conn.execute(f"ALTER TABLE tweets ADD COLUMN {name} {typ};")
                except sqlite3.OperationalError:
                    pass  # column may already exist in a race
        # add indexes if needed
        with conn:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tweets_created ON tweets(created_at_ts);")

def init_db(db_path: Optional[str] = None) -> sqlite3.Connection:
    conn = _connect(db_path)
    _ensure_schema(conn)
    return conn

def _parse_created_at(s: Optional[str]) -> int:
    if not s:
        return 0
    try:
        # Example: "Mon Aug 04 17:13:55 +0000 2025"
        dt = datetime.strptime(s, "%a %b %d %H:%M:%S %z %Y")
        return int(dt.timestamp())
    except Exception:
        return 0

def upsert_tweets(conn: sqlite3.Connection, tweets: Iterable[dict], batch_size: int = 500, grok_username: str = "grok") -> int:
    """
    Upsert tweets by id. Returns number of attempted inserts/updates.
    Populates:
      - parent_id from inReplyToId
      - created_at_ts parsed once from createdAt
      - is_grok_reply from (author.userName == grok_username and isReply)
    """
    rows = []
    count = 0
    gname = (grok_username or "").lower()
    for t in tweets:
        if not isinstance(t, dict):
            continue
        tid = t.get("id")
        if not tid:
            continue
        author = (t.get("author") or {})
        is_grok = 1 if ((author.get("userName") or "").lower() == gname and t.get("isReply")) else 0
        rows.append((
            tid,
            t.get("conversationId"),
            author.get("userName"),
            t.get("createdAt"),
            _parse_created_at(t.get("createdAt")),
            1 if t.get("isReply") else 0,
            is_grok,
            t.get("inReplyToId"),
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
    with conn:
        conn.executemany(
            """
            INSERT INTO tweets (id, conversation_id, author_username, created_at, created_at_ts, is_reply, is_grok_reply, parent_id, json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
              conversation_id=excluded.conversation_id,
              author_username=excluded.author_username,
              created_at=excluded.created_at,
              created_at_ts=excluded.created_at_ts,
              is_reply=excluded.is_reply,
              is_grok_reply=excluded.is_grok_reply,
              parent_id=excluded.parent_id,
              json=excluded.json
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

def tweet_exists(conn: sqlite3.Connection, tweet_id: str) -> bool:
    """
    Return True iff a tweet with this id exists in the DB.
    """
    if not tweet_id:
        return False
    cur = conn.execute("SELECT 1 FROM tweets WHERE id = ? LIMIT 1;", (tweet_id,))
    return cur.fetchone() is not None


from typing import Optional, Tuple
import os
import sqlite3

def merge_databases(db1_path: str, db2_path: str, out_path: str="./grok_data_COPY/merged.sqlite3") -> Tuple[str, int, int]:
    """
    Merge two SQLite DBs (same schema as this module) into a new DB.
    Output name defaults to MERGED_{basename(db1)}_{basename(db2)}.sqlite3 (without extensions).
    Conflict policy:
      - tweets: keep the row with the greater created_at_ts (ties prefer db2)
      - checkpoints: db2 wins on key
    Returns: (out_path, tweet_count, checkpoint_count)
    """

    def _base(p: str) -> str:
        b = os.path.basename(p)
        if b.endswith(".sqlite3"):
            b = b[:-8]
        else:
            b = os.path.splitext(b)[0]
        # sanitize path separators just in case
        return b.replace(os.sep, "_")


    # Make sure both sources are on the current schema (your init_db handles migrations/PRAGMAs)
    init_db(db1_path).close()
    init_db(db2_path).close()

    # Create/overwrite output DB
    if os.path.exists(out_path):
        os.remove(out_path)
    out = init_db(out_path)

    with out:
        out.execute("PRAGMA foreign_keys = OFF;")
        # Attach both sources using the exact paths you pass in (relative is OK)
        out.execute("ATTACH ? AS a;", (db1_path,))
        out.execute("ATTACH ? AS b;", (db2_path,))

        # 1) Copy everything from DB1 first
        out.execute("""
            INSERT INTO tweets (id, conversation_id, author_username, created_at, created_at_ts,
                                is_reply, is_grok_reply, parent_id, json)
            SELECT id, conversation_id, author_username, created_at, created_at_ts,
                   is_reply, is_grok_reply, parent_id, json
            FROM a.tweets;
        """)

        # 2a) For rows that exist in both, update OUT.tweets from DB2 *only if DB2 is newer or equal*
        # (No UPSERT; use a correlated subquery + WHERE EXISTS)
        out.execute("""
            UPDATE tweets
               SET conversation_id = (SELECT conversation_id FROM b.tweets WHERE b.tweets.id = tweets.id),
                   author_username = (SELECT author_username FROM b.tweets WHERE b.tweets.id = tweets.id),
                   created_at      = (SELECT created_at      FROM b.tweets WHERE b.tweets.id = tweets.id),
                   created_at_ts   = (SELECT created_at_ts   FROM b.tweets WHERE b.tweets.id = tweets.id),
                   is_reply        = (SELECT is_reply        FROM b.tweets WHERE b.tweets.id = tweets.id),
                   is_grok_reply   = (SELECT is_grok_reply   FROM b.tweets WHERE b.tweets.id = tweets.id),
                   parent_id       = (SELECT parent_id       FROM b.tweets WHERE b.tweets.id = tweets.id),
                   json            = (SELECT json            FROM b.tweets WHERE b.tweets.id = tweets.id)
             WHERE EXISTS (
                   SELECT 1
                     FROM b.tweets AS bb
                    WHERE bb.id = tweets.id
                      AND IFNULL(bb.created_at_ts, 0) >= IFNULL(tweets.created_at_ts, 0)
             );
        """)

        # 2b) Insert rows that are only in DB2 (or where DB1 was newer; ignore PK conflicts)
        out.execute("""
            INSERT OR IGNORE INTO tweets (id, conversation_id, author_username, created_at, created_at_ts,
                                          is_reply, is_grok_reply, parent_id, json)
            SELECT id, conversation_id, author_username, created_at, created_at_ts,
                   is_reply, is_grok_reply, parent_id, json
              FROM b.tweets;
        """)

        # 3) checkpoints: start with DB1, then let DB2 win (REPLACE)
        out.execute("""
            INSERT OR IGNORE INTO checkpoints(key, value)
            SELECT key, value FROM a.checkpoints;
        """)
        out.execute("""
            INSERT OR REPLACE INTO checkpoints(key, value)
            SELECT key, value FROM b.checkpoints;
        """)

        out.execute("DETACH a;")
        out.execute("DETACH b;")
        out.execute("PRAGMA foreign_keys = ON;")

    tweet_count = out.execute("SELECT COUNT(*) FROM tweets;").fetchone()[0]
    cp_count = out.execute("SELECT COUNT(*) FROM checkpoints;").fetchone()[0]
    out.close()
    return out_path, tweet_count, cp_count

