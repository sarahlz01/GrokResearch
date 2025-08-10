import os
import time
import logging
import json
from typing import Dict, List, Optional
import requests
from dotenv import load_dotenv
import sys
from format_objects import build_query, save_json, build_conversation_objects
load_dotenv()
API_BASE = "https://api.twitterapi.io"
API_KEY = os.getenv("TWITTERIO_API_KEY")
HEADERS = {"X-API-Key": API_KEY}

assert API_KEY, "Set TWITTERIO_API_KEY env var."
assert sys.prefix != sys.base_prefix, "Make sure to activate the venv by calling: source venv/bin/activate"

# logging config
logging.basicConfig(
    level=logging.INFO,  # Minimum level to log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# make the API call with timeouts if necessary
def http_get(path: str, params: Optional[dict] = None, max_retries: int = 4, timeout: int = 30) -> dict:
    url = f"{API_BASE}{path}"
    backoff = 1.0
    for attempt in range(max_retries):
        resp = requests.get(url, headers=HEADERS, params=params, timeout=timeout)
        if resp.status_code == 200:
            logging.info("✅\tSuccess: %s \t(attempt %d/%d)", path, attempt + 1, max_retries)
            return resp.json()
        if resp.status_code in (429, 500, 502, 503, 504):
            logging.warning(
                "⚠️\tHTTP %s on %s \t(attempt %d/%d). Backing off %.1f sec...",
                resp.status_code, path, attempt + 1, max_retries, backoff
            )
            time.sleep(backoff); backoff *= 2; continue
        logging.error("❌\tHTTP %s on %s. No retry for this status.", resp.status_code, path)
        resp.raise_for_status()
    logging.error("❌\tFailed after %d attempts on %s", max_retries, path)
    resp.raise_for_status()


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

def dedupe_conversations_from_pages(search_pages: List[dict]) -> Dict[str, str]:
    """Build {conversationId: representative_tweet_id} from raw search pages."""
    conv_to_tweet: Dict[str, str] = {}
    for page in search_pages:
        for t in page.get("tweets", []) or []:
            conv = t.get("conversationId")
            tid = t.get("id")
            if conv and tid and conv not in conv_to_tweet:
                conv_to_tweet[conv] = tid
    return conv_to_tweet

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

def get_tweets(handle="grok",
                      since=None, until=None,
                      query_type="Latest",
                      limit_threads: Optional[int] = None,
                      include_self_threads=False,
                      include_quotes=False,
                      include_retweets=False,
                      out_path="grok_raw_dump.json"):

    # 1) raw search pages
    search_pages = search_grok_replies(
        handle=handle, since=since, until=until, query_type=query_type,
        include_self_threads=include_self_threads, include_quotes=include_quotes, include_retweets=include_retweets
    )

    # 2) dedupe conversations
    conv_map = dedupe_conversations_from_pages(search_pages)
    conv_ids = list(conv_map.keys())
    logging.info("Search yielded %d conversations", len(conv_ids))
    if limit_threads: conv_ids = conv_ids[:limit_threads]

    # 3) raw thread pages per conversation
    threads: Dict[str, List[dict]] = {}
    for conv_id in conv_ids:
        rep_tweet_id = conv_map[conv_id]
        threads[conv_id] = fetch_thread_pages(rep_tweet_id)

    # 4) save a formatted payload
    payload = build_conversation_objects(threads)
    save_json(payload, out_path)
    return payload
    
if __name__ == "__main__":
    # widen the window slightly when validating counts
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
