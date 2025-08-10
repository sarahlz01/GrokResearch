import os
import sys
assert sys.prefix != sys.base_prefix, "Make sure you have setup the venv and activated it by calling:\tsource venv/bin/activate"

import time
import logging
from typing import Dict, List, Optional, Tuple
import requests
from dotenv import load_dotenv

from format_objects import build_query, save_json, build_conversation_objects_by_threads

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
    Keep *all* Grok reply ids per conversation (order of discovery preserved).
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

# ---------------- Helpers for per-thread dedupe ----------------
def extract_items(page: dict) -> Tuple[str, List[dict]]:
    """Return (key_used, items_list) where key is 'replies' or 'tweets' if present."""
    if isinstance(page.get("replies"), list):
        return "replies", page.get("replies") or []
    if isinstance(page.get("tweets"), list):
        return "tweets", page.get("tweets") or []
    return "tweets", []

def page_signature_from_ids(ids: List[str], has_next: bool, next_cursor: Optional[str], status: Optional[str], msg: Optional[str]) -> Tuple:
    """Build a lightweight signature for a page to avoid duplicates."""
    return (
        has_next,
        bool(next_cursor),
        status,
        msg,
        tuple(sorted(ids))  # stable regardless of incoming order
    )

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
    Loose structure, PLUS per-thread dedupe while merging:
      - seen_tweet_ids per (conversationId, replyId)
      - page_signatures per (conversationId, replyId)
      - we filter each incoming page down to unseen tweet IDs before appending
      - we skip pages whose signature we've already seen
    """
    # 1) raw search pages
    search_pages = search_grok_replies(
        handle=handle, since=since, until=until, query_type=query_type,
        include_self_threads=include_self_threads, include_quotes=include_quotes, include_retweets=include_retweets
    )

    # 2) collect ALL grok reply ids per conversation (ordered)
    conv_to_reply_ids = collect_reply_ids_by_conversation(search_pages)
    conv_ids = list(conv_to_reply_ids.keys())
    logging.info("Search yielded %d conversations", len(conv_ids))
    if limit_threads:
        conv_ids = conv_ids[:limit_threads]

    # 3) In-memory "seen" per run to avoid refetching same replyId twice in this run
    run_seen_reply_ids: Dict[str, set] = {}

    # 4) Per-conversation → per-thread container with dedupe state
    #    threads_state[conv_id][rid] = { "pages": [], "seen_tweet_ids": set(), "page_signatures": set() }
    threads_state: Dict[str, Dict[str, dict]] = {}

    for conv_id in conv_ids:
        reply_ids = conv_to_reply_ids.get(conv_id, [])
        seen_rids = run_seen_reply_ids.setdefault(conv_id, set())

        new_ids = [rid for rid in reply_ids if rid not in seen_rids]
        if not new_ids:
            logging.info("Conversation %s: no new Grok reply ids (skipping fetch).", conv_id)
            continue

        logging.info("Conversation %s: fetching %d thread(s).", conv_id, len(new_ids))
        for rid in new_ids:
            # Fetch raw pages for this replyId
            pages = fetch_thread_pages(rid)

            # Ensure state object exists
            state = threads_state.setdefault(conv_id, {}).setdefault(rid, {
                "pages": [],
                "seen_tweet_ids": set(),
                "page_signatures": set(),
            })

            # Merge with per-thread dedupe
            for page in pages or []:
                key, items = extract_items(page)
                ids_in_page = [t.get("id") for t in items if t.get("id")]
                sig = page_signature_from_ids(
                    ids=ids_in_page,
                    has_next=bool(page.get("has_next_page")),
                    next_cursor=page.get("next_cursor"),
                    status=page.get("status"),
                    msg=page.get("msg"),
                )
                if sig in state["page_signatures"]:
                    # Duplicate page; skip
                    continue

                # Filter items down to unseen tweet IDs for this thread
                filtered_items = []
                for t in items:
                    tid = t.get("id")
                    if not tid:
                        continue
                    if tid in state["seen_tweet_ids"]:
                        continue
                    state["seen_tweet_ids"].add(tid)
                    filtered_items.append(t)

                # Build a filtered page object preserving the same key ('replies' or 'tweets')
                filtered_page = dict(page)  # shallow copy

                # Remove both keys first to avoid empty-list traps in the formatter
                filtered_page.pop("replies", None)
                filtered_page.pop("tweets", None)

                # Set only the array that the page actually uses
                if key == "replies":
                    filtered_page["replies"] = filtered_items
                else:
                    filtered_page["tweets"] = filtered_items
                
                # Record page signature and append
                state["page_signatures"].add(sig)
                state["pages"].append(filtered_page)

            # mark reply id as fetched in this run
            seen_rids.add(rid)

    # 5) Convert to the formatter's expected shape: {conv: {rid: [pages...]}}
    threads_by_conv: Dict[str, Dict[str, List[dict]]] = {}
    for conv_id, threads in threads_state.items():
        for rid, state in threads.items():
            threads_by_conv.setdefault(conv_id, {})[rid] = state["pages"]

    # 6) Build conversation objects grouped by threads and save
    payload = build_conversation_objects_by_threads(threads_by_conv)
    save_json(payload, out_path)
    return payload

if __name__ == "__main__":
    payload = get_tweets(
        handle="grok",
        since="2025-08-05 00:00:00",  # CHANGE THESE FIELDS!
        until="2025-08-05 00:00:01",
        query_type="Latest",
        include_self_threads=False,
        include_quotes=False,
        include_retweets=False,
        out_path="grok_data/data.json"
    )
    logging.info("Done.")
