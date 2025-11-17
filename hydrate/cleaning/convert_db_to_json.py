import json
import ijson
from collections import defaultdict
import os
from typing import Dict, List, Set
from decimal import Decimal
import logging
logging.getLogger(__name__)

from cleaning.clean_threads import clean_conversations_minimal
from db.storage import init_db
def _json_default(o):
    if isinstance(o, Decimal):
        # keep integers as ints when possible
        try:
            return int(o) if o == o.to_integral_value() else float(o)
        except Exception: return float(o)
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")

def export_json_from_db(out_path: str, grok_db_outpath):
    raw_path = out_path[0:out_path.find(".json")]+"_RAW"+".json"
    dump_conversations_raw(raw_path, grok_db_outpath)
    translate(raw_path, out_path)

def translate(raw_path, out_path):
    """
    Stream-read RAW.json (array of conversations), and stream-write both:
      - out_path                (thread-organized full JSON)
      - out_path.replace(... )  (cleaned minimal JSON)
    Nothing is fully loaded into memory.
    """

    cleaned_path = out_path.replace(".json", "_CLEANED.json")
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

    # Prepare writers for the two output JSON arrays
    out_f = open(out_path, "w", encoding="utf-8")
    out_f.write("[\n")
    first_full = True

    clean_f = open(cleaned_path, "w", encoding="utf-8")
    clean_f.write("[\n")
    first_clean = True

    try:
        with open(raw_path, "r", encoding="utf-8") as f:
            # stream each conversation object from the top-level array
            for conv in ijson.items(f, "item", use_float=True):
                threads = threads_for_conversation(conv)
                if not threads:
                    continue

                full_conv = {
                    "conversationId": conv["conversationId"],
                    "threads": threads
                }

                # write full (thread-organized) object
                if not first_full:
                    out_f.write(",\n")
                json.dump(full_conv, out_f, ensure_ascii=False, indent=2, default=_json_default)
                first_full = False

                # write cleaned object (re-use your existing cleaner over a single-item list)
                cleaned_list = clean_conversations_minimal([full_conv])
                cleaned_conv = cleaned_list[0] if cleaned_list else None
                if cleaned_conv:
                    if not first_clean:
                        clean_f.write(",\n")
                    json.dump(cleaned_conv, clean_f, ensure_ascii=False, indent=2, default=_json_default)
                    first_clean = False
    finally:
        out_f.write("\n]\n")
        out_f.close()
        clean_f.write("\n]\n")
        clean_f.close()


# dump the db into json without organizing
def dump_conversations_raw(out_path: str, grok_db_outpath: str) -> None:
    """
    Stream all tweets grouped by conversationId into a single JSON array, but
    only keep one conversation in memory at a time.
    Output format is unchanged:
      [
        {"conversationId": "<id>", "tweets": [<tweet_json>, ...]},
        ...
      ]
    """
    conn = init_db(grok_db_outpath)
    cur = conn.execute(
        "SELECT conversation_id, id, created_at_ts, json FROM tweets "
        "WHERE conversation_id IS NOT NULL "
        "ORDER BY conversation_id, created_at_ts, id"
    )

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("[\n")

        current_conv = None
        current_tweets = []
        current_seen = set()
        first_obj = True

        for conv_id, tid, ts, j in cur:
            if not conv_id or not tid or not j:
                continue

            # when conversation changes, flush previous
            if current_conv is not None and conv_id != current_conv:
                obj = {"conversationId": current_conv, "tweets": current_tweets}
                if not first_obj:
                    f.write(",\n")
                json.dump(obj, f, ensure_ascii=False)
                first_obj = False
                current_tweets = []
                current_seen = set()

            current_conv = conv_id
            # dedupe per conversation
            if tid in current_seen:
                continue
            current_seen.add(tid)

            try:
                t = json.loads(j, parse_float=float, parse_int=int)
            except Exception:
                continue
            current_tweets.append(t)

        # flush last conversation
        if current_conv is not None:
            obj = {"conversationId": current_conv, "tweets": current_tweets}
            if not first_obj:
                f.write(",\n")
            json.dump(obj, f, ensure_ascii=False)

        f.write("\n]\n")


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
        
        chain_tweets = [id_map[tid] for tid in chain_ids]
        stopped_flag = any((t.get("_incomplete_thread") is True) or (t.get("_stop_reason") == "non_assistant_limit")
                           for t in chain_tweets)
        threads.append({
            "threadId": leaf_id,  # or last id, or hash(tuple(chain_ids))
            "incomplete_thread": bool(stopped_flag),
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

