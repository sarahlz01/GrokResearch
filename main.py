import os
import sys
assert sys.prefix != sys.base_prefix, "Make sure to activate the venv by calling:\tsource venv/bin/activate"

import time
import logging
import json
from typing import Dict, List, Optional
import requests
from dotenv import load_dotenv
from format_objects import build_query, save_json, build_conversation_objects

load_dotenv()
API_BASE = "https://api.twitterapi.io"
API_KEY = os.getenv("TWITTERIO_API_KEY")
HEADERS = {"X-API-Key": API_KEY}

assert API_KEY, "Set TWITTERIO_API_KEY env var."

# ---------------- Logging ----------------
logging.basicConfig(
    level=logging.INFO,  # DEBUG for more verbosity
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
            logging.info("✅ Success: %s (attempt %d/%d)", path, attempt + 1, max_retries)
            return resp.json()
        if resp.status_code in (429, 500, 502, 503, 504):
            logging.warning(
                "⚠️ HTTP %s on %s (attempt %d/%d). Backing off %.1f sec...",
                resp.status_code, path, attempt + 1, max_retries, backoff
            )
            time.sleep(backoff)
            backoff *= 2
            continue
        logging.error("❌ HTTP %s on %s. No retry for this status.", resp.status_code, path)
        resp.raise_for_status()
    logging.error("❌ Failed after %d attempts on %s", max_retries, path)
    resp.raise_for_status()

# ---------------- Search + grouping ----------------
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
    """
    Build {conversationId: [grok_reply_tweet_ids...]} from raw search pages.
    Keep *all* Grok reply ids per conversation (not just one representative).
    """
    conv_to_ids: Dict[str, List[str]] = {}
    for page in search_pages:
        for t in (page.get("tweets") or []):
            conv = t.get("conversationId")
            tid = t.get("id")
            if conv and tid:
                conv_to_ids.setdefault(conv, []).append(tid)
    return conv_to_ids

# ---------------- Thread fetch with pagination ----------------
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

        # Always obey the server's signal first
        if not page.get("has_next_page"):
            break

        cursor = page.get("next_cursor") or ""
        if not cursor:
            # Safety: stop if server says more but gives no cursor
            break
    return pages

# ---------------- Orchestration ----------------
def get_tweets(handle="grok",
               since=None, until=None,
               query_type="Latest",
               limit_threads: Optional[int] = None,
               include_self_threads=False,
               include_quotes=False,
               include_retweets=False,
               out_path="grok_data/data.json"):
    """
    In-memory 'seen' map, no persistence:
      seen: { conversationId: set(grokReplyIdsAlreadyFetched) }
    For each conversation, fetch thread_context ONLY for Grok reply ids not yet seen
    during this run. Append all fetched pages into threads[conv_id].
    """
    # 1) raw search pages
    search_pages = search_grok_replies(
        handle=handle, since=since, until=until, query_type=query_type,
        include_self_threads=include_self_threads, include_quotes=include_quotes, include_retweets=include_retweets
    )

    # 2) collect ALL grok reply ids per conversation
    conv_to_reply_ids = collect_reply_ids_by_conversation(search_pages)
    conv_ids = list(conv_to_reply_ids.keys())
    logging.info("Search yielded %d conversations", len(conv_ids))
    if limit_threads:
        conv_ids = conv_ids[:limit_threads]

    # 3) in-memory seen map for this run
    seen: Dict[str, set] = {}

    # 4) raw thread pages per conversation (only for *new* Grok reply ids in each conversation)
    threads: Dict[str, List[dict]] = {}
    for conv_id in conv_ids:
        reply_ids = conv_to_reply_ids.get(conv_id, [])
        seen_ids = seen.setdefault(conv_id, set())

        new_ids = [rid for rid in reply_ids if rid not in seen_ids]
        if not new_ids:
            logging.info("Conversation %s: no new Grok reply ids (skipping fetch).", conv_id)
            threads.setdefault(conv_id, [])
            continue

        logging.info("Conversation %s: fetching %d new branch(es).", conv_id, len(new_ids))
        for rid in new_ids:
            pages = fetch_thread_pages(rid)
            threads.setdefault(conv_id, []).extend(pages)
            seen_ids.add(rid)  # mark as seen in-memory

    # 5) save a formatted payload (conversation objects w/ your schema & ordering)
    payload = build_conversation_objects(threads)
    save_json(payload, out_path)
    return payload

if __name__ == "__main__":
    # widen the window slightly when validating counts if needed
    payload = get_tweets(
        handle="grok",
        since="2025-08-05 00:00:00",
        until="2025-08-05 00:00:01",
        query_type="Latest",
        include_self_threads=False,
        include_quotes=False,
        include_retweets=False,
        out_path="grok_data/data.json"
    )
    logging.info("Done.")
