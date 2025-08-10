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

# build the query
def build_query(handle: str,
                 include_self_threads: bool = False,
                 include_quotes: bool = False,
                 include_retweets: bool = False,
                 since: Optional[str] = None,
                 until: Optional[str] = None) -> str:
    parts = [f"from:{handle}", "filter:replies"]
    if not include_retweets: 
        parts.append("-filter:retweets") 
    else: 
        parts.append("filter:retweets")
    
    if not include_quotes: 
        parts.append("-filter:quote")
    else: 
        parts.append("filter:quote")
        
    if not include_self_threads: 
        parts.append("-filter:self_threads")
    else:
        parts.append("filter:self_threads")
    
    if since: parts.append(f"since:{format_time_utc(since)}")
    if until: parts.append(f"until:{format_time_utc(until)}")
    query = " ".join(parts)
    logging.info("Built query:\t%s ", query)
    return query

def save_author(a: Optional[dict]) -> Optional[dict]:
    if not isinstance(a, dict):
        return None
    return {
        "type": a.get("type"),
        "userName": a.get("userName"),
        "url": a.get("url"),
        "twitterUrl": a.get("twitterUrl"),
        "id": a.get("id"),
        "followers": a.get("followers"),
        "following": a.get("following"),
        "createdAt": a.get("createdAt"),
        "protected": a.get("protected"),
    }
    
def save_fields(t: dict) -> dict:
    return {
        "type": t.get("type"),
        "id": t.get("id"),
        "url": t.get("url"),
        "twitterUrl": t.get("twitterUrl"),
        "text": t.get("text"),
        "retweetCount": t.get("retweetCount"),
        "replyCount": t.get("replyCount"),
        "quoteCount": t.get("quoteCount"),
        "createdAt": t.get("createdAt"),
        "lang": t.get("lang"),
        "bookmarkCount": t.get("bookmarkCount"),
        "isReply": t.get("isReply"),
        "inReplyToId": t.get("inReplyToId"),
        "conversationId": t.get("conversationId"),
        "inReplyToUserId": t.get("inReplyToUserId"),
        "inReplyToUsername": t.get("inReplyToUsername"),
        "author": save_author(t.get("author")),
        "possiblySensitive": t.get("possiblySensitive"),
        "quoted_tweet": t.get("quoted_tweet"),
        "retweeted_tweet": t.get("retweeted_tweet"),
    }

import json
import logging
from typing import Dict, List, Optional

# -------- Tweet field extraction (only keep what you asked for) --------

def _safe_author(a: Optional[dict]) -> Optional[dict]:
    if not isinstance(a, dict):
        return None
    return {
        "type": a.get("type"),
        "userName": a.get("userName"),
        "url": a.get("url"),
        "twitterUrl": a.get("twitterUrl"),
        "id": a.get("id"),
        "followers": a.get("followers"),
        "following": a.get("following"),
        "createdAt": a.get("createdAt"),
        "protected": a.get("protected"),
    }

def _select_fields(t: dict) -> dict:
    # Note: you asked for exact keys like "inreplyToId" (lowercase r) and "conversationID" (capital D)
    # so we map the source fields to those exact output keys.
    return {
        "type": t.get("type"),
        "id": t.get("id"),
        "url": t.get("url"),
        "twitterUrl": t.get("twitterUrl"),
        "text": t.get("text"),
        "retweetCount": t.get("retweetCount"),
        "replyCount": t.get("replyCount"),
        "quoteCount": t.get("quoteCount"),
        "createdAt": t.get("createdAt"),
        "lang": t.get("lang"),
        "bookmarkCount": t.get("bookmarkCount"),
        "isReply": t.get("isReply"),
        "inreplyToId": t.get("inReplyToId"),               # map to your requested key
        "conversationID": t.get("conversationId"),         # map to your requested key
        "inReplyToUserID": t.get("inReplyToUserId"),       # map to your requested key
        "inReplyToUsername": t.get("inReplyToUsername"),
        "author": _safe_author(t.get("author")),
        "possiblySensitive": t.get("possiblySensitive"),
        "quoted_tweet": t.get("quoted_tweet"),
        "retweeted_tweet": t.get("retweeted_tweet"),
    }

# -------- Build conversation -> tweets from RAW thread_context pages --------

def build_conversation_objects(
    conv_to_rep_tweet: Dict[str, str],
    conv_to_thread_pages: Dict[str, List[dict]],
) -> List[dict]:
    """
    Convert raw thread_context pages into:
      [{ "conversationId": <id>, "tweets": [<selected fields>...] }, ...]
    Dedup tweets by id within each conversation.
    """
    output = []
    for conv_id, pages in conv_to_thread_pages.items():
        seen = set()
        tweets_out = []
        for page in pages or []:
            # API sometimes returns "replies" or "tweets"
            items = page.get("replies") if isinstance(page.get("replies"), list) else page.get("tweets")
            if not isinstance(items, list):
                items = []
            for t in items:
                tid = t.get("id")
                if tid and tid not in seen:
                    seen.add(tid)
                    tweets_out.append(_select_fields(t))

            # Follow has_next_page even if the current items list is empty (handled in your fetch loop)

        # Sort optional: newest first by createdAt (string sort may be fine, but you can parse if needed)
        # tweets_out.sort(key=lambda x: x.get("createdAt") or "", reverse=False)

        output.append({
            "conversationId": conv_id,
            "tweets": tweets_out
        })
    return output

def save_json(conversations: List[dict], path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(conversations, f, ensure_ascii=False, indent=2)
    logging.info("Saved %d conversations to %s", len(conversations), path)
