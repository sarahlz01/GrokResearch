# format the time into UTC
import logging
from typing import Dict, List, Optional
import json
import os

# logging config
logging.basicConfig(
    level=logging.INFO,  # Minimum level to log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def format_time_utc(ts: str) -> str:
    ts = ts.strip()
    if "_UTC" in ts: return ts
    if " " in ts: date, hms = ts.split(" ", 1)
    else: date, hms = ts, "00:00:00"
    return f"{date}_{hms}_UTC"

def build_query(handle: str,
                include_self_threads: bool = False,
                include_quotes: bool = False,
                include_retweets: bool = False,
                since: Optional[str] = None,
                until: Optional[str] = None) -> str:
    parts = [f"from:{handle}", "filter:replies"]
    parts.append("filter:retweets" if include_retweets else "-filter:retweets")
    parts.append("filter:quote" if include_quotes else "-filter:quote")
    parts.append("filter:self_threads" if include_self_threads else "-filter:self_threads")
    if since: parts.append(f"since:{format_time_utc(since)}")
    if until: parts.append(f"until:{format_time_utc(until)}")
    query = " ".join(parts)
    logging.info("Built query:\t%s", query)
    return query
# ---------- Field selection ----------
# --- Replace your ALLOWED_* with ordered lists ---

TWEET_KEY_ORDER = [
    "type", "id", "url", "twitterUrl", "text",
    "retweetCount", "replyCount", "quoteCount",
    "createdAt", "lang", "bookmarkCount", "isReply",
    "inReplyToId", "conversationId", "inReplyToUserId", "inReplyToUsername",
    "possiblySensitive"
]

AUTHOR_KEY_ORDER = [
    "type", "userName", "url", "twitterUrl", "id",
    "followers", "following", "createdAt", "protected"
]   

def trim_author(a: Optional[dict]) -> Optional[dict]:
    if not isinstance(a, dict):
        return None
    out = {}
    for k in AUTHOR_KEY_ORDER:
        out[k] = a.get(k)
    return out

def trim_tweet_core(t: dict) -> dict:
    """Build tweet dict in the exact key order, then append author."""
    out = {}
    for k in TWEET_KEY_ORDER:
        out[k] = t.get(k)
    out["author"] = trim_author(t.get("author"))
    return out

def format_nested_tweet(t: Optional[dict], depth: int = 1) -> Optional[dict]:
    """
    Normalize quoted/retweeted tweets to the same schema and key order.
    depth=1 => allow one more level of nested quoted/retweeted; 0 => stop.
    """
    if not isinstance(t, dict):
        return None
    base = trim_tweet_core(t)
    if depth > 0:
        base["quoted_tweet"]    = format_nested_tweet(t.get("quoted_tweet"), depth=0)
        base["retweeted_tweet"] = format_nested_tweet(t.get("retweeted_tweet"), depth=0)
    else:
        base["quoted_tweet"]    = None
        base["retweeted_tweet"] = None
    return base

def save_fields(t: dict) -> dict:
    """Top-level tweet formatter (ordered), then normalized nested tweets."""
    out = trim_tweet_core(t)                  # ordered core + author
    out["quoted_tweet"]    = format_nested_tweet(t.get("quoted_tweet"), depth=1)
    out["retweeted_tweet"] = format_nested_tweet(t.get("retweeted_tweet"), depth=1)
    return out

# ---------- Thread page helpers ----------
def items_from_thread_page(page: dict) -> List[dict]:
    """Thread pages sometimes use 'replies' or 'tweets'."""
    if isinstance(page.get("replies"), list):
        return page.get("replies") or []
    if isinstance(page.get("tweets"), list):
        return page.get("tweets") or []
    return []

def build_conversation_objects(
    conv_to_thread_pages: Dict[str, List[dict]],
) -> List[dict]:
    """
    Convert raw thread_context pages into:
      [{ "conversationId": <id>, "tweets": [<selected fields>...] }, ...]
    Dedup tweets by id within each conversation.
    """
    conversations = []

    for conv_id, pages in (conv_to_thread_pages or {}).items():
        # Flattened, deduped tweets across all pages (as before)
        seen = set()
        all_tweets = []

        # Page-by-page view with metadata
        page_objs = []
        for page in pages or []:
            raw_items = items_from_thread_page(page)

            # Trim/order tweets for this page
            pagination_objs = []
            for t in raw_items:
                tid = t.get("id")
                if tid and tid not in seen:
                    seen.add(tid)
                    formatted = save_fields(t)
                    all_tweets.append(formatted)
               # else:
                    # Even if duplicate globally, keep it in pagination_objs
                    # ONLY if you want page fidelity. If not, skip this branch.
                    #if tid:
                    #    pagination_objs.append(save_fields(t))

            pagination_obj = {
                "has_next_page": page.get("has_next_page"),
                "next_cursor": page.get("next_cursor"),
                "status": page.get("status"),
                "msg": page.get("msg"),
            }
            pagination_objs.append(pagination_obj)

        conversations.append({
            "conversationId": conv_id,
            "tweets": all_tweets,   # fully flattened, deduped per conversation
            "pagination": pagination_objs      # page-wise view incl. the 4 fields you asked for
        })

    return conversations

# ---------- Save helper ----------
def save_json(conversations: List[dict], path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(conversations, f, ensure_ascii=False, indent=2)
    logging.info("Saved %d conversations to %s", len(conversations), path)