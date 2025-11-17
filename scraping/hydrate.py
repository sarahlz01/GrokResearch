#!/usr/bin/env python3
"""
Hydrate a dehydrated dataset back to the full (released) schema.

- Streams the dehydrated JSON (no SQLite).
- For each thread, batches tweet IDs into a single /twitter/tweets call.
- Reconstructs % fields:
    original_text  <- API 'text'
    text           <- clean_threads.clean_text_with_map(...)
    author fields  <- username/name/description
    entities.user_mentions
- Preserves computed flags and any existing (non-text) annotations.

Run:
    python hydrate.py --in ./dehydrated.json --out ./hydrated.json
"""

import argparse
import os
import json
import logging
from typing import Any, Dict, List
from setuplog import setup_logging

import ijson

# ---------------- Project-local imports (minimal reuse) ----------------

from network.http import http_get                      # uploaded by you
from cleaning.clean_objects_while_scraping import extract_items
from cleaning.clean_threads import clean_text_with_map, GROK_USER_ID  # uploaded by you

# ---------------- Helpers ----------------

def is_media_only_from_api(tweet: Dict[str, Any], alias_map: dict) -> bool:
    """
    Re-check media-only with hydrated text + entities:
      - If cleaned body is empty, but there is media or urls -> media-only
    """
    original = tweet.get("text", "") or ""
    cleaned_body = clean_text_with_map(original, alias_map)  # use your canonical cleaner
    has_text = len(cleaned_body) > 0

    ent = tweet.get("entities") or {}
    urls_list = ent.get("urls") or []
    has_urls = isinstance(urls_list, list) and len(urls_list) > 0

    ext = tweet.get("extendedEntities") or {}
    media = ext.get("media") or []
    has_media = isinstance(media, list) and len(media) > 0

    return (not has_text) and (has_media or has_urls)

def pick_user_mentions(ent: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Normalize user_mentions into a consistent list of dicts.
    """
    out: List[Dict[str, Any]] = []
    for m in (ent or {}).get("user_mentions") or []:
        if not isinstance(m, dict):
            continue
        out.append({
            "id_str": m.get("id_str"),
            "indices": m.get("indices"),
            "name": m.get("name"),
            "screen_name": m.get("screen_name"),
        })
    return out

def pick_simple_entities(ent: Dict[str, Any]) -> Dict[str, List[str]]:
    """
    Keep hashtags/symbols/urls as simple string lists.
    """
    out = {"hashtags": [], "symbols": [], "urls": []}
    for h in (ent or {}).get("hashtags") or []:
        if isinstance(h, dict):
            tag = h.get("text") or h.get("tag")
            if isinstance(tag, str):
                out["hashtags"].append(tag)
        elif isinstance(h, str):
            out["hashtags"].append(h)
    for s in (ent or {}).get("symbols") or []:
        if isinstance(s, dict):
            sym = s.get("text") or s.get("symbol")
            if isinstance(sym, str):
                out["symbols"].append(sym)
        elif isinstance(s, str):
            out["symbols"].append(s)
    for u in (ent or {}).get("urls") or []:
        if isinstance(u, dict):
            url = u.get("expanded_url") or u.get("url")
            if isinstance(url, str):
                out["urls"].append(url)
        elif isinstance(u, str):
            out["urls"].append(u)
    return out

def build_hydrated_tweet(api_t: Dict[str, Any], dehydrated_t: Dict[str, Any], alias_map: dict) -> Dict[str, Any]:
    """
    Merge the API tweet into the dehydrated shell to reconstruct the hydrated schema.
    Dehydrated carries counts/ids/lang/url; API supplies % fields + full entities/author.
    Assumes api_t is a valid tweet dict (we skip if missing).
    """
    author = api_t.get("author") or {}
    entities = api_t.get("entities") or {}

    # author section (fields restored)
    # Use GROK_USER_ID for isAssistant when possible; fallback to username check if missing id.
    is_assistant = False
    if str(author.get("id")) == str(GROK_USER_ID):
        is_assistant = True
    else:
        uname = (author.get("userName") or "").lower()
        if uname == "grok":
            is_assistant = True

    author_out = {
        "userName": author.get("userName"),
        "name": author.get("name"),
        "isVerified": bool(author.get("isVerified")),
        "isBlueVerified": bool(author.get("isBlueVerified")),
        "followers": author.get("followers"),
        "following": author.get("following"),
        "description": author.get("description"),
        "isAssistant": is_assistant,
    }

    # entity section (simple lists + user_mentions)
    ents_out = pick_simple_entities(entities)
    ents_out["user_mentions"] = pick_user_mentions(entities)

    # text fields
    original_text = api_t.get("text") or ""
    cleaned_text  = clean_text_with_map(original_text, alias_map)  # canonical cleaner

    # recompute media-only with hydrated content
    media_only = is_media_only_from_api(api_t, alias_map)

    # stitch final hydrated tweet
    hydrated = {
        "id": str(dehydrated_t.get("id")),
        "inReplyToId": str(dehydrated_t.get("inReplyToId")),
        "url": dehydrated_t.get("url") or api_t.get("url"),
        "original_text": original_text,
        "text": cleaned_text,
        "retweetCount": dehydrated_t.get("retweetCount"),
        "replyCount": dehydrated_t.get("replyCount"),
        "likeCount": dehydrated_t.get("likeCount"),
        "quoteCount": dehydrated_t.get("quoteCount"),
        "viewCount": dehydrated_t.get("viewCount"),
        "createdAt": dehydrated_t.get("createdAt") or api_t.get("createdAt"),
        "lang": dehydrated_t.get("lang") or api_t.get("lang"),
        "bookmarkCount": dehydrated_t.get("bookmarkCount"),
        "isMediaOnly": media_only,
        "author": author_out,
        "entities": ents_out,
    }

    # pass through any existing annotations from dehydrated
    if "annotations" in dehydrated_t and isinstance(dehydrated_t["annotations"], dict):
        hydrated["annotations"] = dehydrated_t["annotations"]

    return hydrated

# ---------------- Streaming & API ----------------

def iter_conversations(path: str):
    """
    Stream top-level array: yield conversation objects one-by-one.
    Each object expected to have: {"threads": [...]}
    """
    with open(path, "r", encoding="utf-8") as f:
        for conv in ijson.items(f, "item", use_float=True):
            if isinstance(conv, dict):
                yield conv

def hydrate_thread(thread_obj: Dict[str, Any], alias_map: dict) -> Dict[str, Any]:
    """
    For a single dehydrated thread:
      - batch GET /twitter/tweets with all tweet ids
      - rebuild all tweets in original order using api_map
      - SKIP any tweets whose IDs are missing from the API response
        (e.g., deleted, not found, or otherwise absent).
    """
    dehy_tweets: List[Dict[str, Any]] = thread_obj.get("tweets") or []
    ids_in_order = [str(t.get("id")) for t in dehy_tweets if t.get("id")]
    if not ids_in_order:
        return {
            "threadId": str(thread_obj.get("threadId") or ""),
            "conversationId": thread_obj.get("conversationId"),
            "hasMissingTweets": bool(thread_obj.get("hasMissingTweets")),
            "headlessThread": bool(thread_obj.get("headlessThread")),
            "validTweetCount": thread_obj.get("validTweetCount"),
            "deletedTweetCount": thread_obj.get("deletedTweetCount"),
            "tweets": []
        }

    # One API call per thread with all tweet IDs
    try:
        params = {"tweet_ids": ",".join(ids_in_order)}
        page = http_get("/twitter/tweets", params=params, conversation_id=thread_obj.get("conversationId"))
        _kind, api_items = extract_items(page)   # returns list from {"tweets": [...]}
    except Exception as e:
        logging.exception(
            "Error hydrating thread %s (conversation %s) for ids=%s: %s",
            thread_obj.get("threadId"),
            thread_obj.get("conversationId"),
            ",".join(ids_in_order),
            e,
        )
        api_items = []

    api_map = {str(t.get("id")): t for t in (api_items or []) if isinstance(t, dict) and t.get("id")}

    hydrated_tweets: List[Dict[str, Any]] = []
    missing_ids: List[str] = []

    for dt in dehy_tweets:
        tid = str(dt.get("id"))
        api_t = api_map.get(tid)
        if not api_t:
            # Tweet not returned by API (likely deleted or not found) → skip
            missing_ids.append(tid)
            continue
        hydrated_tweets.append(build_hydrated_tweet(api_t, dt, alias_map))

    if missing_ids:
        logging.warning(
            "Skipped %d tweets in thread %s (conversation %s) because API returned no data for ids (showing up to 10): %s",
            len(missing_ids),
            thread_obj.get("threadId"),
            thread_obj.get("conversationId"),
            ",".join(missing_ids[:10]),
        )

    return {
        "threadId": str(thread_obj.get("threadId") or ""),
        "conversationId": thread_obj.get("conversationId"),
        "hasMissingTweets": bool(thread_obj.get("hasMissingTweets")),
        "headlessThread": bool(thread_obj.get("headlessThread")),
        "validTweetCount": thread_obj.get("validTweetCount"),
        "deletedTweetCount": thread_obj.get("deletedTweetCount"),
        "tweets": hydrated_tweets
    }

def write_hydrated(in_path: str, out_path: str) -> None:
    """
    Stream conversations; create a stable alias_map per conversation so that <USER_n>
    tokens are consistent across all its threads, then write hydrated output.
    """
    with open(out_path, "w", encoding="utf-8") as out_f:
        out_f.write("[\n")
        first = True
        for conv in iter_conversations(in_path):
            threads = conv.get("threads") or []

            # Stable per-conversation alias map (ensures consistent <USER_n> across threads)
            alias_map: dict = {}

            hydrated_threads = []
            for th in threads:
                hydrated_threads.append(hydrate_thread(th, alias_map))

            out_obj = {"threads": hydrated_threads}
            if not first:
                out_f.write(",\n")
            json.dump(out_obj, out_f, ensure_ascii=False, indent=2)
            first = False
        out_f.write("\n]\n")

# ---------------- CLI ----------------

def main():
    ap = argparse.ArgumentParser(description="Hydrate a dehydrated dataset to the full release schema.")
    ap.add_argument("--in",  dest="in_path",  required=True, help="dehydrated JSON path")
    ap.add_argument("--out", dest="out_path", required=True, help="hydrated JSON output path")
    args = ap.parse_args()

    # Set up logger with timestamped file under hydration/logs/
    os.makedirs("hydration/logs", exist_ok=True)
    setup_logging(run_name="hydration", log_dir="hydration/logs", to_stdout=False)
    logging.info("🪣 Hydration starting...")
    logging.info("Input: %s", args.in_path)
    logging.info("Output: %s", args.out_path)

    try:
        write_hydrated(args.in_path, args.out_path)
        logging.info("✅ Hydration complete → %s", args.out_path)
    except Exception as e:
        logging.exception("💥 Hydration failed: %s", e)
        raise

if __name__ == "__main__":
    main()
