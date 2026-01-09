#!/usr/bin/env python3
"""
Rehydrate dehydrated GrokSet JSON using twitterapi.io.

Goals (current project rules):
- By default, ALL fields already present in dehydrated.json remain in hydrated.json.
  (conversation, thread, tweet level)
- Rehydrate "live / changing" fields from API (optional), like engagement counts.
- Preserve annotations exactly as in dehydrated.json (conversation/thread/tweet).
- Include extra fields returned by twitterapi.io (tweet-level), without deleting existing dehydrated fields.
- Stream large JSON using ijson.
- Always write valid JSON even if interrupted (Ctrl+C).

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

from setuplog import setup_logging
from network.http import http_get
from cleaning.clean_objects_while_scraping import extract_items
from cleaning.clean_threads import clean_text_with_map, GROK_USER_ID


# ------------------------------------------------------------
# Utilities
# ------------------------------------------------------------

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
) -> Dict[str, Any]:
    """
    Return a tweet object that:
    - Includes ALL keys from dehydrated_t (unchanged)
    - Adds hydrated text fields: original_text + cleaned text
    - Adds ALL API fields (without removing any dehydrated fields)
    - Optionally updates engagement counts from API (default ON)
    - Preserves tweet-level annotations (already in dehydrated_t)
    """
    author = api_t.get("author") or {}
    entities = api_t.get("entities") or {}

    # Start by preserving dehydrated fields (and their insertion order).
    hydrated: Dict[str, Any] = dict(dehydrated_t)

    # Bring in ALL API fields as extras (only add if missing to avoid stomping
    # on your dehydrated schema), except for fields we explicitly control.
    # (We still allow counts to be updated below.)
    for k, v in api_t.items():
        if k not in hydrated:
            hydrated[k] = v

    # Hydrated text fields
    original_text = api_t.get("text", "") or ""
    cleaned = clean_text_with_map(original_text, alias_map)

    # Always keep id/inReplyToId as strings from dehydrated
    hydrated["id"] = _as_str(dehydrated_t.get("id") or api_t.get("id"))
    hydrated["inReplyToId"] = _as_str(dehydrated_t.get("inReplyToId") or api_t.get("inReplyToId"))

    # Add hydrated text fields (these are part of your "released schema" layer)
    hydrated["original_text"] = original_text
    hydrated["text"] = cleaned

    # createdAt/lang should exist already in dehydrated; fallback to API if missing
    if not hydrated.get("createdAt"):
        hydrated["createdAt"] = api_t.get("createdAt")
    if not hydrated.get("lang"):
        hydrated["lang"] = api_t.get("lang")

    # Update engagement counts only if requested
    if update_engagement:
        for fld in ["likeCount", "retweetCount", "replyCount", "quoteCount", "viewCount", "bookmarkCount"]:
            if fld in api_t:
                hydrated[fld] = api_t.get(fld)

    # Rehydrate author/entities richer objects, but preserve any dehydrated-only
    # constraints by ensuring isAssistant is set.
    # If you *want* to keep the dehydrated author/entities shape, you can—this
    # keeps them but enriches with API fields.
    is_assistant = _is_assistant_from_api(author)

    # Merge author: keep dehydrated first, then add API fields
    dehy_author = dehydrated_t.get("author") or {}
    merged_author = dict(dehy_author) if isinstance(dehy_author, dict) else {}
    for k, v in author.items():
        if k not in merged_author:
            merged_author[k] = v
    merged_author["isAssistant"] = is_assistant
    hydrated["author"] = merged_author

    # Merge entities similarly
    dehy_entities = dehydrated_t.get("entities") or {}
    merged_entities = dict(dehy_entities) if isinstance(dehy_entities, dict) else {}
    for k, v in entities.items():
        if k not in merged_entities:
            merged_entities[k] = v
    hydrated["entities"] = merged_entities

    return hydrated


def hydrate_thread(
    thread: Dict[str, Any],
    conversation_id: str,
    alias_map: dict,
    update_engagement: bool,
) -> Dict[str, Any]:
    """
    Returns a thread object that:
    - Preserves ALL existing dehydrated thread fields
    - Rehydrates tweets (keeping tweet fields + adding hydrated text/API extras)
    - Updates hasMissingTweets to missing count (int)
    - Emits explicit placeholders for missing tweets
    - Preserves thread.annotations exactly as-is
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

        hydrated_tweets.append(build_hydrated_tweet(api_t, dt, alias_map, update_engagement))

    if missing_count:
        logging.warning("🔴 Thread %s had %d missing tweets (conversation=%s)",
                        thread.get("threadId"), missing_count, conversation_id)

    # Preserve ALL dehydrated thread fields, then override the ones we must update.
    hydrated_thread: Dict[str, Any] = dict(thread)
    hydrated_thread["hasMissingTweets"] = missing_count
    hydrated_thread["tweets"] = hydrated_tweets

    return hydrated_thread


# ------------------------------------------------------------
# Streaming driver
# ------------------------------------------------------------

def write_hydrated(
    in_path: str,
    out_path: str,
    update_engagement: bool,
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
                        # skip conversation entirely
                        continue

                hydrated_threads = [
                    hydrate_thread(t, cid, alias_map, update_engagement)
                    for t in threads
                    if isinstance(t, dict)
                ]

                # Preserve ALL conversation fields, then override threads (hydrated).
                hydrated_conv: Dict[str, Any] = dict(conv)
                hydrated_conv["conversationId"] = cid
                hydrated_conv["threads"] = hydrated_threads

                if not first_out:
                    fout.write(",\n")
                json.dump(hydrated_conv, fout, ensure_ascii=False, indent=2)
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

    # Thread-id mode
    ap.add_argument("--thread-ids", default=None, help="Comma-separated threadIds to hydrate")
    ap.add_argument("--thread-ids-file", default=None, help="JSON file containing threadIds")

    ap.add_argument("--log-every", type=int, default=10000)

    args = ap.parse_args()

    # Logging (file-only)
    os.makedirs("rehydration/logs", exist_ok=True)
    setup_logging(run_name="rehydrate", log_dir="rehydration/logs", to_stdout=False)

    update_engagement = not args.no_update_engagement
    thread_id_filter = _load_thread_id_set(args.thread_ids, args.thread_ids_file)

    logging.info("🚀 Starting rehydration")
    logging.info("Input: %s", args.in_path)
    logging.info("Output: %s", args.out_path)
    logging.info("Update engagement: %s", update_engagement)
    logging.info("Thread filter: %s", ("none" if thread_id_filter is None else f"{len(thread_id_filter)} threads"))

    write_hydrated(
        in_path=args.in_path,
        out_path=args.out_path,
        update_engagement=update_engagement,
        thread_id_filter=thread_id_filter,
        log_every=args.log_every,
    )

    logging.info("✅ Rehydration complete")


if __name__ == "__main__":
    main()
