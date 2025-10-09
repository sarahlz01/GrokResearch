# set up logging
import logging
import network.http as http

from network.http import http_get
logging.getLogger(__name__)

import sys

assert (sys.prefix != sys.base_prefix), "Make sure you have setup the venv and activated it by calling:\tsource venv/bin/activate.\nCheck README for more information"

import time
import logging
from typing import Dict, Set

from cleaning.clean_objects_while_scraping import build_query, extract_items
from cleaning.convert_db_to_json import export_json_from_db
from db.storage import init_db, tweet_exists, upsert_tweets

def search_grok_replies_stream(handle="grok", since=None, until=None, query_type="Latest", include_self_threads=False, include_quotes=False, include_retweets=False):
    query = build_query(handle, include_self_threads, include_quotes, include_retweets, since, until)
    cursor = ""
    while True:
        params = {"query": query, "queryType": query_type, "cursor": cursor}
        page = http_get("/twitter/tweet/advanced_search", params)
        yield page  # we YIELD pages instead of returning them. This makes it so that every time we get a new page, its instantly processed before we move on to the next page
        cursor = page.get("next_cursor") or ""
        if not cursor:
            break

def fetch_thread_pages_stream_thread_context(tweet_id: str, conversation_id: str):
    """
    Call /twitter/tweet/thread_context up to (1 + tries) times.
    After each call, if items are sorted oldest→newest, update tweet_id to the FIRST tweet's id
    to climb further up the context. Stop early when we detect the root or no new items arrive.
    Yields a single synthesized page with merged unique tweets.
    """
    seen_ids = set()
    merged = []
    found_root = False
    attempts = 0
    current_id = str(tweet_id)

    while attempts < 3:
        attempts += 1

        page = http_get(
            "/twitter/tweet/thread_context",
            {"tweetId": current_id, "cursor": ""},  # cursor ignored; endpoint is flaky
            conversation_id=conversation_id
        )

        _, items = extract_items(page)  # expects oldest→newest already
        items = items or []

        new_added = 0
        for t in items:
            if not isinstance(t, dict):
                continue
            tid = t.get("id")
            if not tid:
                continue
            tid = str(tid)
            if tid in seen_ids:
                continue
            seen_ids.add(tid)
            merged.append(t)
            new_added += 1
            # Full-context stop condition
            if (t.get("conversationId") and t.get("id") == t.get("conversationId")) or (t.get("inReplyToId") in (None, "")):
                found_root = True

        if found_root or new_added == 0:
            break

        # Walk further up by switching to the oldest tweet returned this round
        # (items are oldest→newest, so items[0] is the oldest)
        if items and isinstance(items[0], dict) and items[0].get("id"):
            oldest_id = str(items[0]["id"])
            # If we didn't move upward, bail to avoid a loop
            if oldest_id == current_id:
                break
            current_id = oldest_id
        else:
            break
            
    # Yield a single normalized "page" so downstream code stays unchanged
    yield {"tweets": merged}
    
def fetch_thread_pages_stream_by_tweet_id(tweet_id: str, conversation_id: str, db_conn):
    """
    Call /twitter/tweets and climb to each tweet using inReplyToId
    """
    seen_ids = set()
    res = []
    current_id = str(tweet_id)
    MAX_NON_ASSISTANT = 15
    consecutive_non_assistant = 0
    tweet_counter = 0

    while current_id:   
        if current_id in seen_ids: # avoid duplicates
            break
        
        if tweet_exists(db_conn, current_id): # stop crawling up if this already exists in the DB
            break
        
        seen_ids.add(current_id) 
        
        
        page = http_get("/twitter/tweets", {"tweet_ids": current_id}, conversation_id=conversation_id)

        _, items = extract_items(page)
        if not items:
            break
        
        pg = items[0]
        if not pg.get('id') or not pg.get("type"):
            break
        
        res.append(pg)
        current_id = pg.get("inReplyToId")
        
        author_un = ((pg.get("author") or {}).get("userName")) or pg.get("authorName")
        is_assistant = (author_un == "grok")
        if is_assistant:
            consecutive_non_assistant = 0
        else:
            consecutive_non_assistant += 1
            if consecutive_non_assistant >= MAX_NON_ASSISTANT:
                try:
                    pg["_stop_reason"] = "non_assistant_limit"
                    pg["_incomplete_thread"] = True
                except Exception:
                    pass
                finally:
                    break
        current_id = pg.get("inReplyToId")
        tweet_counter += 1
        if tweet_counter > 150:
            try:
                    pg["_stop_reason"] = "tweet_counter_reached_150"
                    pg["_incomplete_thread"] = True
            except Exception:
                    pass
            finally:
                break
            
    # Yield a single normalized "page" so downstream code stays unchanged
    yield {"tweets": res}

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


# -------- Streaming runner --------
def run_streaming(handle="grok", since=None, until=None, query_type="Latest", include_self_threads=False, include_quotes=False, include_retweets=False, build_final_json: bool = False, out_path: str = "grok_data/data.json", number_conversations: int = 0, grok_db_outpath: str="grok_data/grok.sqlite3"):
    db_conn = None
    stop = False
    t0 = time.time()

    try:
        db_conn = init_db(grok_db_outpath)
    except Exception as e:
        logging.error("🚫\tSQLite storage not available (%s). Aborting.", e)
        raise # raise this error as there's no point in continuing the API calls if we can't store the results

    seen: Dict[str, Set[str]] = {}
    total_upserts = 0
    total_search_pages = 0
    
    try:
        for search_page in search_grok_replies_stream(handle=handle, since=since, until=until, query_type=query_type, include_self_threads=include_self_threads, include_quotes=include_quotes, include_retweets=include_retweets):
            total_search_pages += 1

            # Extract the conversationId from the search page(s)
            conv_to_ids: Dict[str, Set] = {}
            _, items = extract_items(search_page)
            for t in items:
                conv = t.get("conversationId")
                tid = t.get("id")
                if conv and tid:
                    conv_to_ids.setdefault(conv, set()).add(tid)

            # for a conversation(s), search through their replies and scrape them if they aren't duplicates
            for conv_id, reply_ids in conv_to_ids.items():
                # logic to handle # conversations
                if number_conversations <= 0 or len(seen) >= number_conversations:
                    stop = True
                    break
                seen.setdefault(conv_id, set())

                # scrape through each reply in a given conversation
                for rid in reply_ids:
                    if rid in seen[conv_id]:
                        continue
                    seen[conv_id].add(rid)

                    # add/upsert each page result from the stream 
                    for page in fetch_thread_pages_stream_by_tweet_id(tweet_id=rid, conversation_id=conv_id, db_conn=db_conn):
                        _, page_items = extract_items(page)
                        if db_conn and page_items:
                            normalized = [t for t in page_items if isinstance(t, dict)]
                            if normalized:
                                total_upserts += upsert_tweets(db_conn, normalized, batch_size=500, grok_username=handle)
                        
                        new_groks = extract_grok_reply_ids_from_pages(page, conversation_id=conv_id, grok_username=handle)
                        if new_groks:
                            seen[conv_id].update(new_groks)
            if stop: # we met the conversation limit, stop early
                break

        logging.info(
            "Streaming complete: %d search page(s); ~%d upsert attempts.",
            total_search_pages,
            total_upserts,
        )
        if build_final_json:
            logging.info("💾\tExporting to json from db")
            return export_json_from_db(out_path=out_path, grok_db_outpath=grok_db_outpath)
        return None
    except Exception as e:
        logging.error("🚫\tDumping partial DB to JSON due to error: %s", e)
        raise  # re-raise so callers know the run failed (remove if you prefer to swallow) -- swallow this
    finally:
        elapsed = time.time() - t0
        logging.info("Done! Run summary — elapsed=%.1fs | conversations=%d | search_pages=%d | upserts≈%d | api_success=%d / attempts=%d", elapsed,len(seen), total_search_pages, total_upserts, http.SUCCESSFUL_API_CALLS, http.TOTAL_API_CALLS,)


# -------- Direct execution (no hydra dependency) --------
if __name__ == "__main__":
    import logging

    # Set up logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

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
            since="2025-08-05 00:00:00",
            until="2025-08-05 00:00:01",
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
