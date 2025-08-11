import os
import sys
assert sys.prefix != sys.base_prefix, "Make sure you have setup the venv and activated it by calling:\tsource venv/bin/activate"

import time
import json
import logging
from typing import Dict, List, Optional, Tuple, Set
import requests
from dotenv import load_dotenv

from format_objects import build_query, export_json_from_db, save_fields  # build_conversation_objects_by_threads no longer needed for export
from storage import init_db, upsert_tweets

load_dotenv()
API_BASE = "https://api.twitterapi.io"
API_KEY = os.getenv("TWITTERIO_API_KEY")
HEADERS = {"X-API-Key": API_KEY}

assert API_KEY, "Set TWITTERIO_API_KEY env var."

# ---------------- Logging ----------------
logging.basicConfig(
    level=logging.INFO,  # set DEBUG for deeper tracing
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# ---------------- HTTP helper with logs/backoff ----------------
def http_get(path: str, params: Optional[dict] = None, max_retries: int = 4, timeout: int = 30) -> dict:
    url = f"{API_BASE}{path}"
    backoff = 1.0
    for attempt in range(max_retries):
        resp = requests.get(url, headers=HEADERS, params=params, timeout=timeout)
        if resp.status_code == 200:
            logging.info("✅\t%s (attempt %d/%d)", path, attempt + 1, max_retries)
            return resp.json()
        if resp.status_code in (429, 500, 502, 503, 504):
            logging.warning(
                "⚠️\tHTTP %s on %s (attempt %d/%d). Backing off %.1f sec...",
                resp.status_code, path, attempt + 1, max_retries, backoff
            )
            time.sleep(backoff)
            backoff *= 2
            continue
        logging.error("⚠️\tHTTP %s on %s. No retry for this status.", resp.status_code, path)
        resp.raise_for_status()
    logging.error("⚠️\tFailed after %d attempts on %s", max_retries, path)
    resp.raise_for_status()

# ---------------- Original (non-streaming) search + grouping ----------------
def search_grok_replies(handle="grok",
                        since=None, until=None,
                        query_type="Latest",
                        max_pages=None,
                        include_self_threads=False,
                        include_quotes=False,
                        include_retweets=False) -> List[dict]:
    """Return a list of RAW pages from Advanced Search (no cleaning)."""
    query = build_query(handle, include_self_threads, include_quotes, include_retweets, since, until)
    pages: List[dict] = []
    cursor, n = "", 0
    while True:
        params = {"query": query, "queryType": query_type, "cursor": cursor}
        page = http_get("/twitter/tweet/advanced_search", params)
        pages.append(page)
        cursor = page.get("next_cursor") or ""
        n += 1
        if not cursor or (max_pages and n >= max_pages):
            break
    return pages

def collect_reply_ids_by_conversation(search_pages: List[dict]) -> Dict[str, List[str]]:
    """Build {conversationId: [grok_reply_tweet_ids...]} from raw search pages (preserve order)."""
    conv_to_ids: Dict[str, List[str]] = {}
    for page in search_pages:
        for t in (page.get("tweets") or []):
            conv = t.get("conversationId")
            tid = t.get("id")
            if conv and tid:
                conv_to_ids.setdefault(conv, []).append(tid)
    return conv_to_ids

# ---------------- Original (non-streaming) thread fetch ----------------
def fetch_thread_pages(tweet_id: str, max_pages: Optional[int] = None) -> List[dict]:
    """Return an array of RAW thread_context pages for this tweet_id."""
    pages: List[dict] = []
    cursor, n = "", 0
    while True:
        page = http_get("/twitter/tweet/thread_context", {"tweetId": str(tweet_id), "cursor": cursor})
        pages.append(page)
        n += 1
        if max_pages and n >= max_pages:
            break
        if not page.get("has_next_page"):
            break
        cursor = page.get("next_cursor") or ""
        if not cursor:
            break
    return pages

# ---------------- Helpers used by both paths ----------------
def extract_items(page: dict) -> Tuple[str, List[dict]]:
    """Return (key_used, items_list) where key is 'replies' or 'tweets' if present."""
    if isinstance(page.get("replies"), list):
        return "replies", page.get("replies") or []
    if isinstance(page.get("tweets"), list):
        return "tweets", page.get("tweets") or []
    return "tweets", []

def page_signature_from_ids(ids: List[str], has_next: bool, next_cursor: Optional[str], status: Optional[str], msg: Optional[str]) -> Tuple:
    """Build a lightweight signature for a page to avoid duplicates."""
    return (has_next, bool(next_cursor), status, msg, tuple(sorted([i for i in ids if i])))

def extract_grok_reply_ids_from_pages(pages_or_single, conversation_id: str, grok_username: str = "grok") -> Set[str]:
    """Extract Grok reply IDs from a page or list of pages for a given conversation."""
    it = pages_or_single if isinstance(pages_or_single, list) else [pages_or_single]
    found: Set[str] = set()
    for page in it:
        _, items = extract_items(page)
        for t in items:
            if not isinstance(t, dict):
                continue
            if t.get("conversationId") != conversation_id:
                continue
            if (t.get("author") or {}).get("userName") != grok_username:
                continue
            if not t.get("isReply"):
                continue
            tid = t.get("id")
            if tid:
                found.add(tid)
    return found

# ---------------- NEW: streaming versions ----------------
def search_grok_replies_stream(handle="grok",
                               since=None, until=None,
                               query_type="Latest",
                               include_self_threads=False,
                               include_quotes=False,
                               include_retweets=False):
    """Generator that yields advanced_search pages one at a time (streaming)."""
    query = build_query(handle, include_self_threads, include_quotes, include_retweets, since, until)
    cursor = ""
    while True:
        params = {"query": query, "queryType": query_type, "cursor": cursor}
        page = http_get("/twitter/tweet/advanced_search", params)
        yield page
        cursor = page.get("next_cursor") or ""
        if not cursor:
            break

def fetch_thread_pages_stream(tweet_id: str):
    """Generator that yields thread_context pages one at a time (streaming)."""
    cursor = ""
    while True:
        page = http_get("/twitter/tweet/thread_context", {"tweetId": str(tweet_id), "cursor": cursor})
        yield page
        if not page.get("has_next_page"):
            break
        cursor = page.get("next_cursor") or ""
        if not cursor:
            break


# ---------------- NEW: streaming runner (low-memory, DB-upserts per page) ----------------
def run_streaming(handle="grok",
                  since=None, until=None,
                  query_type="Latest",
                  include_self_threads=False,
                  include_quotes=False,
                  include_retweets=False,
                  build_final_json: bool = False,
                  out_path: str = "grok_data/data.json"):
    """
    Streaming pipeline:
      - Streams advanced_search pages.
      - Maintains per-conversation 'seen' Grok reply IDs to skip redundant thread fetches.
      - For each new Grok reply ID, streams thread pages,
        dedupes per-thread (if needed), and UPSERTs normalized tweets to SQLite immediately.
      - If build_final_json=True, rebuilds final JSON from SQLite at the end.
    """
    # Optional SQLite sink (non-fatal if missing)
    db_conn = None
    if init_db and upsert_tweets:
        try:
            db_conn = init_db()
        except Exception as e:
            logging.warning("⚠️\tSQLite storage not available (%s). Continuing without DB upserts.", e)
    else:
        logging.warning("⚠️\tstorage.py not found; DB upserts disabled.")

    seen: Dict[str, Set[str]] = {}  # per-conversation Grok reply IDs covered
    total_upserts = 0
    total_search_pages = 0

    for search_page in search_grok_replies_stream(
        handle=handle,
        since=since,
        until=until,
        query_type=query_type,
        include_self_threads=include_self_threads,
        include_quotes=include_quotes,
        include_retweets=include_retweets
    ):
        total_search_pages += 1

        # Extract conv→reply ids from THIS search page only
        conv_to_ids: Dict[str, List[str]] = {}
        _, items = extract_items(search_page)
        for t in items:
            conv = t.get("conversationId")
            tid = t.get("id")
            if conv and tid:
                conv_to_ids.setdefault(conv, []).append(tid)

        for conv_id, reply_ids in conv_to_ids.items():
            seen.setdefault(conv_id, set())

            for rid in reply_ids:
                if rid in seen[conv_id]:
                    continue
                seen[conv_id].add(rid)  # mark upfront

                # Stream thread pages for this reply id
                for page in fetch_thread_pages_stream(rid):
                    # Normalize and upsert tweets from this page immediately
                    _, page_items = extract_items(page)
                    if db_conn and page_items:
                        normalized = [save_fields(t) for t in page_items if isinstance(t, dict)]
                        if normalized:
                            total_upserts += upsert_tweets(db_conn, normalized, batch_size=500)

                    # harvest additional Grok replies surfaced by this branch to avoid extra calls later
                    new_groks = extract_grok_reply_ids_from_pages(page, conversation_id=conv_id, grok_username=handle)
                    if new_groks:
                        seen[conv_id].update(new_groks)

    logging.info("Streaming complete: %d search page(s) processed; ~%d upsert attempts.", total_search_pages, total_upserts)

    if build_final_json:
        return export_json_from_db(out_path=out_path, grok_username=handle)

    return None

# ---------------- Original non-streaming get_tweets (kept for compatibility) ----------------
def get_tweets(handle="grok",
               since=None, until=None,
               query_type="Latest",
               limit_threads: Optional[int] = None,
               include_self_threads=False,
               include_quotes=False,
               include_retweets=False,
               out_path="grok_data/data.json"):
    """
    Old path: builds everything in memory, then writes once.
    (Kept for small runs / debugging.)
    """
    search_pages = search_grok_replies(
        handle=handle, since=since, until=until, query_type=query_type,
        include_self_threads=include_self_threads, include_quotes=include_quotes, include_retweets=include_retweets
    )
    conv_to_reply_ids = collect_reply_ids_by_conversation(search_pages)
    conv_ids = list(conv_to_reply_ids.keys())
    logging.info("Search yielded %d conversations", len(conv_ids))
    if limit_threads:
        conv_ids = conv_ids[:limit_threads]

    # Per-conversation → per-thread container with dedupe state
    threads_state: Dict[str, Dict[str, dict]] = {}
    seen: Dict[str, Set[str]] = {conv_id: set() for conv_id in conv_ids}

    for conv_id in conv_ids:
        reply_ids = conv_to_reply_ids.get(conv_id, [])
        for rid in reply_ids:
            if rid in seen[conv_id]:
                continue
            seen[conv_id].add(rid)
            pages = fetch_thread_pages(rid)
            # harvest additional grok replies to skip later
            newly_found = extract_grok_reply_ids_from_pages(pages, conversation_id=conv_id, grok_username=handle)
            if newly_found:
                seen[conv_id].update(newly_found)
            # store raw pages for legacy formatter path (omitted here to keep minimal)

    # Legacy path no longer builds JSON here; prefer streaming + export_json_from_db
    logging.info("Legacy get_tweets completed. For JSON output, use run_streaming(..., build_final_json=True).")
    return None

# ---------------- Entry point ----------------
if __name__ == "__main__":
    # Streaming + DB, then export JSON from DB (matches your target schema exactly)
    run_streaming(
        handle="grok",
        since="2025-08-02 00:00:00",
        until="2025-08-07 00:00:00",
        query_type="Latest",
        include_self_threads=False,
        include_quotes=False,
        include_retweets=False,
        build_final_json=True,                 # export at the end from DB
        out_path="grok_data/data.json"
    )

    logging.info("Done.")
