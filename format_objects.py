# setup logging
import logging
logging.getLogger(__name__)

from typing import Dict, List, Optional, Set
import json, os
from storage import init_db

from translate_db import translate

def format_time_utc(ts: str) -> str:
    ts = ts.strip()
    if "_UTC" in ts: return ts
    if " " in ts: date, hms = ts.split(" ", 1)
    else: date, hms = ts, "00:00:00"
    return f"{date}_{hms}_UTC"

def build_query(handle: str,include_self_threads: bool = False, include_quotes: bool = False, include_retweets: bool = False, since: Optional[str] = None, until: Optional[str] = None) -> str:
    parts = [f"from:{handle}", "filter:replies"]
    parts.append("filter:retweets" if include_retweets else "-filter:retweets")
    parts.append("filter:quote" if include_quotes else "-filter:quote")
    parts.append("filter:self_threads" if include_self_threads else "-filter:self_threads")
    if since: parts.append(f"since:{format_time_utc(since)}")
    if until: parts.append(f"until:{format_time_utc(until)}")
    query = " ".join(parts)
    logging.info("Built query:\t%s", query)
    return query

def export_json_from_db(out_path: str, grok_username: str ="grok"):
    raw_path = out_path[0:out_path.find(".json")]+"_RAW"+".json"
    dump_conversations_raw(raw_path)
    translate(raw_path, out_path)

def dump_conversations_raw(out_path: str) -> List[dict]:
    """
    Dump *all* tweets from SQLite grouped by conversationId to a simple JSON:
      [
        {"conversationId": "<id>", "tweets": [<tweet_json>, ...]},
        ...
      ]
    Guarantees:
      - Every tweet row for a conversation is included exactly once
      - Tweets are sorted by (created_at_ts, id)
    """
    conn = init_db()
    rows = conn.execute(
        "SELECT conversation_id, id, created_at_ts, json FROM tweets "
        "WHERE conversation_id IS NOT NULL "
        "ORDER BY conversation_id, created_at_ts, id"
    ).fetchall()

    conv_to_tweets: Dict[str, List[dict]] = {}
    seen_in_conv: Dict[str, Set[str]] = {}

    for conv_id, tid, ts, j in rows:
        if not conv_id or not tid or not j:
            continue
        try:
            t = json.loads(j)
        except Exception:
            continue
        s = seen_in_conv.setdefault(conv_id, set())
        if tid in s:
            continue
        s.add(tid)
        conv_to_tweets.setdefault(conv_id, []).append(t)

    out_list = [{"conversationId": cid, "tweets": conv_to_tweets[cid]} for cid in sorted(conv_to_tweets.keys())]

    # simple write (no atomic swap)
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out_list, f, ensure_ascii=False, indent=2)

    logging.info("Raw dump: %d conversation(s) → %s", len(out_list), out_path)
    return out_list