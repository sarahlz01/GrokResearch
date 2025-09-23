import json
from collections import defaultdict
import os
from typing import Dict, List, Set
import logging
logging.getLogger(__name__)

from cleaning.clean_threads import clean_conversations_minimal
from db.storage import init_db


def export_json_from_db(out_path: str, grok_db_outpath):
    raw_path = out_path[0:out_path.find(".json")]+"_RAW"+".json"
    dump_conversations_raw(raw_path, grok_db_outpath)
    translate(raw_path, out_path)

def translate(raw_path, out_path):
    os.makedirs(os.path.dirname(raw_path) or ".", exist_ok=True)
    with open(raw_path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    
    # build the output with all metadata
    out = build_threads_for_raw(raw)
    out = [c for c in out if c.get("threads")] #drop conversations with 0 threads
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    
    # build the cleaned output    
    cleaned = clean_conversations_minimal(out)
    cleaned_path = out_path.replace(".json", "_CLEANED.json")
    with open(cleaned_path, "w", encoding="utf-8") as f2:
        json.dump(cleaned, f2, ensure_ascii=False, indent=2)

# dump the db into json without organizing
def dump_conversations_raw(out_path: str, grok_db_outpath:str) -> List[dict]:
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
    conn = init_db(grok_db_outpath)
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

def build_threads_for_raw(raw_conversations):
    out = []
    for conv in raw_conversations:
        threads = threads_for_conversation(conv)  # from above
        out.append({
            "conversationId": conv["conversationId"],
            "threads": threads
        })
    return out

# organize the raw JSON into threads
def threads_for_conversation(conv):
    tweets = conv["tweets"]
    id_map = {t["id"]: t for t in tweets}
    
    # Build parent->children index to find leaves
    children = defaultdict(list)
    for t in tweets:
        parent = t.get("inReplyToId")
        if parent:
            children[parent].append(t["id"])

    all_ids = set(id_map.keys())
    leaves = [tid for tid in all_ids if tid not in children]  # no children → leaf

    threads = []
    for leaf_id in leaves:
        chain_ids = []
        has_missing_parent = False
        cur = id_map[leaf_id]
        while True:
            chain_ids.append(cur["id"])
            parent_id = cur.get("inReplyToId")
            if parent_id is None or parent_id == '':
                break
            parent = id_map.get(parent_id)
            if parent is None:
                has_missing_parent = True
                break
            cur = parent

        chain_ids.reverse()  # oldest → newest
        threads.append({
            "threadId": leaf_id,  # or last id, or hash(tuple(chain_ids))
            "has_missing_parent": has_missing_parent,
            "tweets": [id_map[tid] for tid in chain_ids],
        })

    # Optional: sort threads by (length desc, last createdAt), etc.
    threads = prune_threads_without_grok(threads) # get rid of threads that dont have grok
    threads = prune_single_tweet_threads(threads) # get rid of single length threads
    threads = ensure_root_in_threads(threads, conv) # add in the initial post to each thread
    return threads

# these 3 functions organize the threads further by removing unnecessary threads
def prune_threads_without_grok(threads):
    """
    Keep only threads that include at least one Grok reply.
    We identify Grok tweets by author.userName == 'grok' (case-insensitive).
    """
    def is_grok_tweet(t):
        author = t.get("author") or {}
        return str(author.get("id")) == "1720665183188922368" # groks user ID

    return [th for th in threads if any(is_grok_tweet(tw) for tw in th.get("tweets", []))]

def ensure_root_in_threads(threads, conv):
    """
    Make sure each thread contains the root tweet (id == conversationId).
    If it's already there, do nothing; otherwise, prepend it.
    """
    conv_id = str(conv["conversationId"])
    id_map = {str(t["id"]): t for t in conv["tweets"]}

    root_tweet = id_map.get(conv_id)
    if not root_tweet:
        return threads  # nothing to do if root isn't in the conversation

    for th in threads:
        ids = {str(tw["id"]) for tw in th.get("tweets", [])}
        if conv_id not in ids:
            th["tweets"].insert(0, root_tweet)
    return threads

def prune_single_tweet_threads(threads):
    """Remove threads that only contain a single tweet."""
    return [th for th in threads if len(th.get("tweets", [])) > 1]

