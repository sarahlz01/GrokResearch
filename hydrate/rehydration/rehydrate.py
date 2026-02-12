#!/usr/bin/env python3
"""
Rehydrate dehydrated GrokSet JSON using twitterapi.io.

Goals (current project rules):
- By default, ALL fields already present in dehydrated.json remain in hydrated.json.
  (conversation, thread, tweet level)
- Rehydrate "live / changing" fields from API (optional), like engagement counts.
- Preserve annotations exactly as in dehydrated.json (conversation-level and tweet-level).
- Include extra fields returned by twitterapi.io (tweet-level), without deleting existing dehydrated fields.
- Stream large JSON using ijson.
- Always write valid JSON even if interrupted (Ctrl+C).

Requested constraints:
- No thread-level annotations in output.
- Avoid duplicate ID fields:
    - Conversations keep only 'conversationId'
    - Threads keep only 'threadId' (remove 'conversation_id' if present)
    - Tweets keep only 'id' (string)
- Add a flag to refresh everything from the API (except annotated/computed fields),
  including text fields.

Missing tweets:
- If API returns no data for a tweet id, emit {"id": "...", "missing": true}
- Update thread.hasMissingTweets to missing count (int).
- Log missing tweet lines with a 🔴 emoji.

Thread-id mode:
- If --thread-ids or --thread-ids-file is provided, only hydrate those threads.
  Conversations with zero selected threads are skipped.
"""

import argparse
import json
import logging
import os
from typing import Dict, Any, List, Optional, Set

import ijson

from decimal import Decimal

from setuplog import setup_logging
from network.http import http_get
from cleaning.clean_objects_while_scraping import extract_items
from cleaning.clean_threads import clean_text_with_map, GROK_USER_ID


# ------------------------------------------------------------
# JSON serialization helpers
# ------------------------------------------------------------

def json_default(o: Any):
    """
    Allow json.dump to serialize Decimal values (e.g., from network_metrics merges).
    """
    if isinstance(o, Decimal):
        # Preserve integers as ints if possible; otherwise float.
        if o == o.to_integral_value():
            return int(o)
        return float(o)
    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")


# ------------------------------------------------------------
# Utilities
# ------------------------------------------------------------
TWEET_KEY_ORDER = [
    "id",
    "inReplyToId",
    "text",
    "cleaned_text",     # only if you decide to include it
    "original_text",
    "toxicity",
    "likeCount",
    "retweetCount",
    "replyCount",
    "quoteCount",
    "viewCount",
    "bookmarkCount",
    "createdAt",
    "lang",
]

def order_keys(obj: Dict[str, Any], priority: List[str]) -> Dict[str, Any]:
    """
    Return a new dict with keys in `priority` first (if present),
    then all remaining keys in their existing order.
    """
    out: Dict[str, Any] = {}
    for k in priority:
        if k in obj:
            out[k] = obj[k]
    for k, v in obj.items():
        if k not in out:
            out[k] = v
    return out


def _as_str(x: Any) -> str:
    if x is None:
        return ""
    return str(x)


def _load_thread_id_set(thread_ids: Optional[str], thread_ids_file: Optional[str]) -> Optional[Set[str]]:
    """
    Returns:
      - None if no filtering requested
      - set of threadIds otherwise
    Accepts:
      --thread-ids "a,b,c"
      --thread-ids-file JSON file containing ["a","b",...]
        or [{"threadId":"..."}, ...]
    """
    if not thread_ids and not thread_ids_file:
        return None

    out: Set[str] = set()

    if thread_ids:
        for part in thread_ids.split(","):
            t = part.strip()
            if t:
                out.add(t)

    if thread_ids_file:
        with open(thread_ids_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            for item in data:
                if isinstance(item, str):
                    out.add(item)
                elif isinstance(item, dict):
                    tid = item.get("threadId") or item.get("thread_id") or item.get("id")
                    if tid:
                        out.add(str(tid))

    return out


def _is_assistant_from_api(author: Dict[str, Any]) -> bool:
    # Prefer id match, fallback to username
    if str(author.get("id")) == str(GROK_USER_ID):
        return True
    uname = (author.get("userName") or "").lower()
    return uname == "grok"


# ------------------------------------------------------------
# Hydration builders
# ------------------------------------------------------------

def build_hydrated_tweet(
    api_t: Dict[str, Any],
    dehydrated_t: Dict[str, Any],
    alias_map: dict,
    update_engagement: bool,
    refresh_all: bool,
) -> Dict[str, Any]:
    """
    Return a tweet object that:
    - Preserves annotated/computed fields from dehydrated_t (e.g., tweet-level toxicity).
    - Adds hydrated text fields: original_text + cleaned text.
    - Adds API fields (optionally overwriting) without deleting annotations.
    - Optionally updates engagement counts from API (default ON unless --no-update-engagement).
    - When refresh_all=True, overwrite all non-annotated fields from the API, including text,
      while preserving:
        - toxicity (tweet-level annotation)
        - isMediaOnly (computed in dehydration)
    """
    author = api_t.get("author") or {}
    entities = api_t.get("entities") or {}

    # Preserve tweet-level annotation + computed fields.
    preserved_toxicity = dehydrated_t.get("toxicity", None)
    preserved_is_media_only = dehydrated_t.get("isMediaOnly", None)

    # Start from dehydrated fields (preserve insertion order).
    hydrated: Dict[str, Any] = dict(dehydrated_t)

    # Ensure preserved fields survive any overwrite.
    if "toxicity" in hydrated:
        hydrated["toxicity"] = preserved_toxicity
    if "isMediaOnly" in hydrated and preserved_is_media_only is not None:
        hydrated["isMediaOnly"] = preserved_is_media_only

    # Bring in API fields.
    if refresh_all:
        # Overwrite everything from API, except our preserved fields.
        for k, v in api_t.items():
            if k in {"toxicity"}:
                continue
            hydrated[k] = v
    else:
        # Only add missing keys (do not stomp dehydrated schema).
        for k, v in api_t.items():
            if k not in hydrated:
                hydrated[k] = v

    # Always keep id/inReplyToId as strings from dehydrated (fallback to API).
    hydrated["id"] = _as_str(dehydrated_t.get("id") or api_t.get("id"))
    hydrated["inReplyToId"] = _as_str(dehydrated_t.get("inReplyToId") or api_t.get("inReplyToId"))

    # Hydrated text fields
    original_text = api_t.get("text", "") or ""
    cleaned = clean_text_with_map(original_text, alias_map)

    hydrated["original_text"] = original_text
    hydrated["text"] = cleaned  # cleaned text becomes the public 'text' on hydration

    # createdAt/lang should exist already in dehydrated; overwrite on refresh_all.
    if refresh_all or not hydrated.get("createdAt"):
        hydrated["createdAt"] = api_t.get("createdAt") or hydrated.get("createdAt") or ""
    if refresh_all or not hydrated.get("lang"):
        hydrated["lang"] = api_t.get("lang") or hydrated.get("lang") or ""

    # Engagement counts:
    # - refresh_all: always overwrite if present
    # - otherwise: overwrite only if update_engagement=True
    if refresh_all or update_engagement:
        for fld in ["likeCount", "retweetCount", "replyCount", "quoteCount", "viewCount", "bookmarkCount"]:
            if fld in api_t:
                hydrated[fld] = api_t.get(fld)

    # Rehydrate author/entities richer objects, but preserve isAssistant computed value.
    is_assistant = _is_assistant_from_api(author)

    dehy_author = dehydrated_t.get("author") or {}
    dehy_entities = dehydrated_t.get("entities") or {}

    if refresh_all:
        merged_author = dict(author) if isinstance(author, dict) else {}
        # Keep any dehydrated-only author keys
        if isinstance(dehy_author, dict):
            for k, v in dehy_author.items():
                if k not in merged_author:
                    merged_author[k] = v
        merged_author["isAssistant"] = is_assistant
        hydrated["author"] = merged_author

        merged_entities = dict(entities) if isinstance(entities, dict) else {}
        if isinstance(dehy_entities, dict):
            for k, v in dehy_entities.items():
                if k not in merged_entities:
                    merged_entities[k] = v
        hydrated["entities"] = merged_entities
    else:
        # Merge author: keep dehydrated first, then add API fields
        merged_author = dict(dehy_author) if isinstance(dehy_author, dict) else {}
        for k, v in author.items():
            if k not in merged_author:
                merged_author[k] = v
        merged_author["isAssistant"] = is_assistant
        hydrated["author"] = merged_author

        # Merge entities similarly
        merged_entities = dict(dehy_entities) if isinstance(dehy_entities, dict) else {}
        for k, v in entities.items():
            if k not in merged_entities:
                merged_entities[k] = v
        hydrated["entities"] = merged_entities

    # Re-assert preserved fields (in case refresh_all overwrote indirectly)
    hydrated["toxicity"] = preserved_toxicity
    if preserved_is_media_only is not None:
        hydrated["isMediaOnly"] = preserved_is_media_only

    hydrated = order_keys(hydrated, TWEET_KEY_ORDER)
    return hydrated


def hydrate_thread(
    thread: Dict[str, Any],
    conversation_id: str,
    alias_map: dict,
    update_engagement: bool,
    refresh_all: bool,
) -> Dict[str, Any]:
    """
    Returns a thread object that:
    - Preserves all existing dehydrated thread fields
    - Rehydrates tweets (keeping tweet fields + adding hydrated text/API extras)
    - Updates hasMissingTweets to missing count (int)
    - Emits explicit placeholders for missing tweets
    - Removes thread-level annotations entirely
    - Removes duplicate conversation_id field if present
    """
    tweet_shells: List[Dict[str, Any]] = thread.get("tweets") or []
    tweet_ids = [_as_str(t.get("id")) for t in tweet_shells if t.get("id")]

    api_map: Dict[str, Dict[str, Any]] = {}
    if tweet_ids:
        try:
            page = http_get(
                "/twitter/tweets",
                params={"tweet_ids": ",".join(tweet_ids)},
                conversation_id=conversation_id
            )
            _, api_items = extract_items(page)
            api_map = {
                _as_str(t.get("id")): t
                for t in (api_items or [])
                if isinstance(t, dict) and t.get("id") is not None
            }
        except Exception:
            logging.exception(
                "❌ Failed API call for thread=%s (conversation=%s)",
                thread.get("threadId"), conversation_id
            )

    hydrated_tweets: List[Dict[str, Any]] = []
    missing_count = 0

    for dt in tweet_shells:
        tid = _as_str(dt.get("id"))
        api_t = api_map.get(tid)

        if not api_t:
            missing_count += 1
            logging.warning("🔴 Missing tweet %s | thread=%s | conversation=%s",
                            tid, thread.get("threadId"), conversation_id)
            hydrated_tweets.append({"id": tid, "missing": True})
            continue

        hydrated_tweets.append(build_hydrated_tweet(api_t, dt, alias_map, update_engagement, refresh_all))

    if missing_count:
        logging.warning("🔴 Thread %s had %d missing tweets (conversation=%s)",
                        thread.get("threadId"), missing_count, conversation_id)

    hydrated_thread: Dict[str, Any] = dict(thread)
    hydrated_thread["hasMissingTweets"] = missing_count
    hydrated_thread["tweets"] = hydrated_tweets

    # Remove thread-level annotations entirely (requested).
    hydrated_thread.pop("annotations", None)

    # Remove duplicate conversation_id (requested).
    hydrated_thread.pop("conversation_id", None)

    return hydrated_thread


# ------------------------------------------------------------
# Streaming driver
# ------------------------------------------------------------

def write_hydrated(
    in_path: str,
    out_path: str,
    update_engagement: bool,
    refresh_all: bool,
    thread_id_filter: Optional[Set[str]],
    log_every: int,
) -> None:
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

    # We always close the JSON array, even on Ctrl+C.
    fout = open(out_path, "w", encoding="utf-8")
    fout.write("[\n")
    first_out = True
    processed = 0
    kept = 0

    try:
        with open(in_path, "rb") as fin:
            for conv in ijson.items(fin, "item"):
                if not isinstance(conv, dict):
                    continue

                processed += 1
                cid = _as_str(conv.get("conversationId"))

                # Per-conversation alias map so the same <USER_n> mapping stays stable
                alias_map: dict = {}

                threads = conv.get("threads") or []
                if not isinstance(threads, list):
                    threads = []

                # Filter threads if requested
                if thread_id_filter is not None:
                    threads = [t for t in threads if _as_str(t.get("threadId")) in thread_id_filter]
                    if not threads:
                        continue

                hydrated_threads = [
                    hydrate_thread(t, cid, alias_map, update_engagement, refresh_all)
                    for t in threads
                    if isinstance(t, dict)
                ]

                hydrated_conv: Dict[str, Any] = dict(conv)
                hydrated_conv["conversationId"] = cid
                hydrated_conv["threads"] = hydrated_threads

                if not first_out:
                    fout.write(",\n")
                json.dump(hydrated_conv, fout, ensure_ascii=False, indent=2, default=json_default)
                first_out = False
                kept += 1

                if log_every > 0 and processed % log_every == 0:
                    logging.info("[progress] processed=%d kept=%d (out=%s)", processed, kept, out_path)

    except KeyboardInterrupt:
        logging.warning("🟡 Interrupted by user (Ctrl+C). Finalizing JSON output so it remains valid...")
    finally:
        fout.write("\n]\n")
        fout.close()
        logging.info("[done] processed=%d kept=%d wrote=%s", processed, kept, out_path)


# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", default="./rehydration/dehydrated.json", help="Input dehydrated.json")
    ap.add_argument("--out", dest="out_path", default="./rehydration/hydrated.json", help="Output hydrated.json")

    # Engagement mode
    ap.add_argument(
        "--no-update-engagement",
        action="store_true",
        help="Do NOT update engagement counts from API; keep dehydrated counts"
    )

    # Refresh mode
    ap.add_argument(
        "--refresh-all",
        action="store_true",
        help="Refresh ALL non-annotated fields from API (including text). Preserves tweet-level toxicity and computed fields."
    )

    # Thread-id mode
    ap.add_argument("--thread-ids", default=None, help="Comma-separated threadIds to hydrate")
    ap.add_argument("--thread-ids-file", default=None, help="JSON file containing threadIds")

    ap.add_argument("--log-every", type=int, default=10000)

    args = ap.parse_args()

    # Logging (file-only)
    os.makedirs("rehydration/logs", exist_ok=True)
    setup_logging(run_name="rehydrate", log_dir="rehydration/logs", to_stdout=False)

    update_engagement = not args.no_update_engagement
    refresh_all = bool(args.refresh_all)
    thread_id_filter = _load_thread_id_set(args.thread_ids, args.thread_ids_file)

    logging.info("🚀 Starting rehydration")
    logging.info("Input: %s", args.in_path)
    logging.info("Output: %s", args.out_path)
    logging.info("Update engagement: %s", update_engagement)
    logging.info("Refresh all: %s", refresh_all)
    logging.info("Thread filter: %s", ("none" if thread_id_filter is None else f"{len(thread_id_filter)} threads"))

    write_hydrated(
        in_path=args.in_path,
        out_path=args.out_path,
        update_engagement=update_engagement,
        refresh_all=refresh_all,
        thread_id_filter=thread_id_filter,
        log_every=args.log_every,
    )

    logging.info("✅ Rehydration complete")


if __name__ == "__main__":
    main()
