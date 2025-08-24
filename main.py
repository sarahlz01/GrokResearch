# set up logging
import logging

logging.getLogger(__name__)

import os
import sys

assert (
    sys.prefix != sys.base_prefix
), "Make sure you have setup the venv and activated it by calling:\tsource venv/bin/activate.\nCheck README for more information"

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

# global variables for tracking purposes
TOTAL_API_CALLS = 0
SUCCESSFUL_API_CALLS = 0


# Makes ONE http request
def http_get(
    path: str, params: Optional[dict] = None, max_retries: int = 3, timeout: int = 30
) -> dict:
    global TOTAL_API_CALLS, SUCCESSFUL_API_CALLS

    url = f"{API_BASE}{path}"
    backoff = 5
    last_exc = None

    for attempt in range(max_retries):
        try:
            TOTAL_API_CALLS += 1
            resp = requests.get(url, headers=HEADERS, params=params, timeout=timeout)
            # if the call was a advanced_search, sometimes it returns 0 tweets. if so, then recall it just in case
            if (path == "/twitter/tweet/advanced_search"):
                _, res_ = extract_items(resp.json())
                if len(res_) == 0:
                    p = params or {}
                    if p.get("tweetId"):  
                        logging.warning("⚠️\tReturned 0 tweets for %s conversation: %s, retrying (%d/%d). Backing off %.1f s... | VERBOSE : %s", path, p.get("tweetId"), attempt + 1, max_retries, backoff, resp.text,)
                    else:  
                        logging.warning("⚠️\tReturned 0 tweets for %s, retrying (%d/%d). Backing off %.1f s... | VERBOSE : %s", path, attempt + 1, max_retries, backoff, resp.text,)

                    time.sleep(backoff)  #!! change to backoff when we have the paid version
                    backoff *= 2
                    continue
            if resp.status_code == 200:
                try:
                    SUCCESSFUL_API_CALLS += 1
                    p = params or {}
                    if p.get("tweetId"):
                        logging.info(
                            "✅\tSuccess: %s for conversation %s (attempt %d/%d)",
                            path,
                            p.get("tweetId"),
                            attempt + 1,
                            max_retries,
                        )
                    else:
                        logging.info(
                            "✅\tSuccess: %s (attempt %d/%d)",
                            path,
                            attempt + 1,
                            max_retries,
                        )

                    return resp.json()
                except ValueError as e:
                    logging.error("🚫\tInvalid JSON from %s: %s", url, e)
                    last_exc = e
                    time.sleep(
                        backoff
                    )  #!! change to backoff when we have the paid version
                    backoff *= 2
            else:
                logging.warning(
                    "⚠️\tHTTP %s on %s (%d/%d). Backing off %.1f s... | VERBOSE : %s",
                    resp.status_code,
                    path,
                    attempt + 1,
                    max_retries,
                    backoff,
                    resp.text,
                )
                time.sleep(backoff)  #!! change to backoff when we have the paid version
                backoff *= 2
            #logging.error("🚫\tHTTP %s on %s. No retry.", resp.status_code, path)

        except requests.RequestException as e:
            logging.warning(
                "⚠️\tRequest error on %s (%d/%d): %s. Backing off %.1f s...",
                path,
                attempt + 1,
                max_retries,
                e,
                backoff,
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


def search_grok_replies_stream(
    handle="grok",
    since=None,
    until=None,
    query_type="Latest",
    include_self_threads=False,
    include_quotes=False,
    include_retweets=False,
):
    query = build_query(
        handle, include_self_threads, include_quotes, include_retweets, since, until
    )
    cursor = ""
    while True:
        params = {"query": query, "queryType": query_type, "cursor": cursor}
        page = http_get("/twitter/tweet/advanced_search", params)
        yield page  # we YIELD pages instead of returning them. This makes it so that every time we get a new page, its instantly processed before we move on to the next page
        cursor = page.get("next_cursor") or ""
        if not cursor:
            break


# we only do ONE call because the pagination system is broken
def fetch_thread_pages_stream(tweet_id: str):
    cursor = ""
    page = http_get(
        "/twitter/tweet/thread_context",
        {"tweetId": str(tweet_id), "cursor": cursor},
    )
    yield page  # same thing here, we YIELD pages (which is an array) so we get them one at a time

def extract_grok_reply_ids_from_pages(
    pages_or_single, conversation_id: str, grok_username: str = "grok"
) -> Set[str]:
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
def run_streaming(
    handle="grok",
    since=None,
    until=None,
    query_type="Latest",
    include_self_threads=False,
    include_quotes=False,
    include_retweets=False,
    build_final_json: bool = False,
    out_path: str = "grok_data/data.json",
    number_conversations: int = 0,
):
    global TOTAL_API_CALLS, SUCCESSFUL_API_CALLS
    db_conn = None
    stop = False
    t0 = time.time()

    if init_db and upsert_tweets:
        try:
            db_conn = init_db()
        except Exception as e:
            logging.warning(
                "⚠️\tSQLite storage not available (%s). Continuing without DB upserts.",
                e,
            )
    else:
        logging.warning("⚠️\tstorage.py not found; DB upserts disabled.")

    seen: Dict[str, Set[str]] = {}
    total_upserts = 0
    total_search_pages = 0
    try:
        for search_page in search_grok_replies_stream(
            handle=handle,
            since=since,
            until=until,
            query_type=query_type,
            include_self_threads=include_self_threads,
            include_quotes=include_quotes,
            include_retweets=include_retweets,
        ):
            total_search_pages += 1

            # Extract conv→reply ids from THIS search page only
            conv_to_ids: Dict[str, Set] = {}
            _, items = extract_items(search_page)
            for t in items:
                conv = t.get("conversationId")
                tid = t.get("id")
                if conv and tid:
                    conv_to_ids.setdefault(conv, set()).add(tid)

            for conv_id, reply_ids in conv_to_ids.items():
                # logic to handle # conversations
                if number_conversations <= 0 or len(seen) >= number_conversations:
                    stop = True
                    break
                seen.setdefault(conv_id, set())

                for rid in reply_ids:
                    if rid in seen[conv_id]:
                        continue
                    seen[conv_id].add(rid)

                    for page in fetch_thread_pages_stream(rid):
                        _, page_items = extract_items(page)
                        if db_conn and page_items:
                            normalized = [
                                save_fields(t)
                                for t in page_items
                                if isinstance(t, dict)
                            ]
                            if normalized:
                                total_upserts += upsert_tweets(
                                    db_conn,
                                    normalized,
                                    batch_size=500,
                                    grok_username=handle,
                                )

                        new_groks = extract_grok_reply_ids_from_pages(
                            page, conversation_id=conv_id, grok_username=handle
                        )
                        if new_groks:
                            seen[conv_id].update(new_groks)

            if stop:
                break

        logging.info(
            "Streaming complete: %d search page(s); ~%d upsert attempts.",
            total_search_pages,
            total_upserts,
        )

        if build_final_json:
            return export_json_from_db(out_path=out_path, grok_username=handle)
        return None
    except Exception as e:
        logging.error("🚫\tDumping partial DB to JSON due to error: %s", e)
        try:
            export_json_from_db(out_path=out_path, grok_username=handle)
            logging.info("💾\tPartial dump complete: %s", out_path)
        except Exception as dump_err:
            logging.error("🚫\tFailed to dump partial JSON after error: %s", dump_err)
        raise  # re-raise so callers know the run failed (remove if you prefer to swallow)
    finally:
        elapsed = time.time() - t0
        logging.info(
            "Done! Run summary — elapsed=%.1fs | conversations=%d | search_pages=%d | upserts≈%d | api_success=%d / attempts=%d",
            elapsed,
            len(seen),
            total_search_pages,
            total_upserts,
            SUCCESSFUL_API_CALLS,
            TOTAL_API_CALLS,
        )


# -------- Direct execution (no hydra dependency) --------
if __name__ == "__main__":
    import logging

    # Set up logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # Try without time restrictions first to see if we can get any tweets
    since = None
    until = None

    logging.info("Starting tweet collection...")
    if since and until:
        logging.info(f"Time range: {since} to {until}")
    else:
        logging.info("No time restrictions - collecting all available tweets")
    logging.info("Target: 100+ tweets")

    try:
        result = run_streaming(
            handle="grok",
            since=since,
            until=until,
            query_type="Latest",
            include_self_threads=False,  # Try without self threads first
            include_quotes=False,  # Try without quotes first
            include_retweets=False,
            build_final_json=True,
            out_path="grok_data/data2.json",
            number_conversations=50,  # Increase to get more conversations
        )

        logging.info("Collection completed successfully!")
        if result:
            logging.info(f"Generated file: {result}")

    except Exception as e:
        logging.error(f"Collection failed: {e}")
        raise
