# set up logging
from setuplog import setup_logging
LOG_PATH = setup_logging(run_name="run", log_dir="logs", to_stdout=False)

import os
import sys
assert sys.prefix != sys.base_prefix, "Make sure you have setup the venv and activated it by calling:\tsource venv/bin/activate.\nCheck README for more information"

import time
import logging
from typing import Dict, List, Optional, Tuple, Set
import requests
from dotenv import load_dotenv

from format_objects import build_query, export_json_from_db, save_fields 
from storage import init_db, upsert_tweets

# load env variables
load_dotenv()
API_BASE = "https://api.twitterapi.io"
API_KEY = os.getenv("TWITTERIO_API_KEY")
HEADERS = {"X-API-Key": API_KEY}
assert API_KEY, "Set TWITTERIO_API_KEY env var."

# Makes ONE http request
def http_get(path: str, params: Optional[dict] = None, max_retries: int = 4, timeout: int = 30) -> dict:
    url = f"{API_BASE}{path}"
    backoff = 1.0
    last_exc = None

    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=timeout)
            if resp.status_code == 200:
                try:
                    logging.info("✅ Success: %s (attempt %d/%d)", path, attempt + 1, max_retries)
                    return resp.json()
                except ValueError as e:
                    logging.error("🚫\tInvalid JSON from %s: %s", url, e)
                    last_exc = e
                    time.sleep(backoff)
                    backoff *= 2
                    continue

            if resp.status_code in (429, 500, 502, 503, 504):
                logging.warning(
                    "⚠️\tHTTP %s on %s (%d/%d). Backing off %.1f s... | VERBOSE : %s",
                    resp.status_code, path, attempt + 1, max_retries, backoff, resp.text
                )
                time.sleep(5)  #!! change to backoff when we have the paid version
                backoff *= 2
                continue

            logging.error("🚫\tHTTP %s on %s. No retry.", resp.status_code, path)
            resp.raise_for_status()

        except requests.RequestException as e:
            logging.warning("⚠️\tRequest error on %s (%d/%d): %s. Backing off %.1f s...",
                path, attempt + 1, max_retries, e, backoff
            )
            last_exc = e
            time.sleep(backoff)
            backoff *= 2
            continue

        except Exception as e:
            logging.error("🚫\tUnexpected error on %s: %s", path, e)
            last_exc = e

    logging.error("🚫\tFailed after %d attempts on %s", max_retries, path)
    if last_exc:
        raise last_exc
    else:
        raise RuntimeError(f"Failed to fetch {url}")

def extract_items(page: dict) -> Tuple[str, List[dict]]:
    if isinstance(page.get("replies"), list):
        return "replies", page.get("replies") or []
    if isinstance(page.get("tweets"), list):
        return "tweets", page.get("tweets") or []
    return "tweets", []

def search_grok_replies_stream(handle="grok", since=None, until=None, query_type="Latest",
                               include_self_threads=False, include_quotes=False, include_retweets=False):
    query = build_query(handle, include_self_threads, include_quotes, include_retweets, since, until)
    cursor = ""
    while True:
        params = {"query": query, "queryType": query_type, "cursor": cursor}
        page = http_get("/twitter/tweet/advanced_search", params)
        yield page # we YIELD pages instead of returning them. This makes it so that every time we get a new page, its instantly processed before we move on to the next page
        cursor = page.get("next_cursor") or ""
        if not cursor:
            break

def fetch_thread_pages_stream(tweet_id: str):
    cursor = ""
    while True:
        page = http_get("/twitter/tweet/thread_context", {"tweetId": str(tweet_id), "cursor": cursor})
        yield page # same thing here, we YIELD pages
        if not page.get("has_next_page"):
            break
        cursor = page.get("next_cursor") or ""
        if not cursor:
            break

def extract_grok_reply_ids_from_pages(pages_or_single, conversation_id: str, grok_username: str = "grok") -> Set[str]:
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

# -------- Streaming runner (unchanged logic, now passes grok_username to upserts) --------
def run_streaming(handle="grok",
                  since=None, until=None,
                  query_type="Latest",
                  include_self_threads=False,
                  include_quotes=False,
                  include_retweets=False,
                  build_final_json: bool = False,
                  out_path: str = "grok_data/data.json",
                  number_conversations: int= 0):
    db_conn = None
    if init_db and upsert_tweets:
        try:
            db_conn = init_db()
        except Exception as e:
            logging.warning("⚠️\tSQLite storage not available (%s). Continuing without DB upserts.", e)
    else:
        logging.warning("⚠️\tstorage.py not found; DB upserts disabled.")

    seen: Dict[str, Set[str]] = {}
    total_upserts = 0
    total_search_pages = 0
    try:
        for search_page in search_grok_replies_stream(
            handle=handle, since=since, until=until, query_type=query_type,
            include_self_threads=include_self_threads, include_quotes=include_quotes, include_retweets=include_retweets
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
                # logic to handle # conversations
                print(len(seen))
                if number_conversations <= 0 or len(seen) >= number_conversations:
                    if build_final_json:
                        try:
                            return export_json_from_db(out_path=out_path, grok_username=handle)
                        except Exception as e:
                            logging.error("Couldn't export as JSON due to error: %s", e)
                            raise
                    return None
                seen.setdefault(conv_id, set())

                for rid in reply_ids:
                    if rid in seen[conv_id]:
                        continue
                    seen[conv_id].add(rid)

                    for page in fetch_thread_pages_stream(rid):
                        _, page_items = extract_items(page)
                        if db_conn and page_items:
                            normalized = [save_fields(t) for t in page_items if isinstance(t, dict)]
                            if normalized:
                                total_upserts += upsert_tweets(db_conn, normalized, batch_size=500, grok_username=handle)

                        new_groks = extract_grok_reply_ids_from_pages(page, conversation_id=conv_id, grok_username=handle)
                        if new_groks:
                            seen[conv_id].update(new_groks)

        logging.info("Streaming complete: %d search page(s); ~%d upsert attempts.", total_search_pages, total_upserts)

        if build_final_json:
            return export_json_from_db(out_path=out_path, grok_username=handle)
        return None
    except Exception as e:
        logging.error("Dumping partial DB to JSON due to error: %s", e)
        try:
            export_json_from_db(out_path=out_path, grok_username=handle)
            logging.info("💾 Partial dump complete: %s", out_path)
            logging.info("Done.")
        except Exception as dump_err:
            logging.error("🚫 Failed to dump partial JSON after error: %s", dump_err)
            logging.info("Done.")
        raise # re-raise so callers know the run failed (remove if you prefer to swallow)

if __name__ == "__main__":
    run_streaming(
        handle="grok",
        since="2025-08-01 00:00:00",
        until="2025-08-01 23:59:59",
        query_type="Latest",
        include_self_threads=False,
        include_quotes=False,
        include_retweets=False,
        build_final_json=True,
        out_path="grok_data/data.json",
        number_conversations=25 # !! default value is 0, must set it here!
    )
    logging.info("Done.")