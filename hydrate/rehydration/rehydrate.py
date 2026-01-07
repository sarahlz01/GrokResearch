#!/usr/bin/env python3
"""
Rehydrate dehydrated GrokSet JSON using twitterapi.io.

Preserves:
- All annotations (conversation + thread)
- All computed fields (except hasMissingTweets, which is updated)

Rehydrates:
- Tweet text
- Engagement counts
- Author + entities
- Any extra API fields

Missing tweets:
- Represented explicitly with { "id": ..., "missing": true }
- hasMissingTweets updated to missing count
"""

import argparse
import json
import logging
import os
from typing import Dict, Any

import ijson

from setuplog import setup_logging
from network.http import http_get
from cleaning.clean_objects_while_scraping import extract_items
from cleaning.clean_threads import clean_text_with_map, GROK_USER_ID


# --------------------------------------------------
# Helpers
# --------------------------------------------------

def build_hydrated_tweet(api_t: Dict[str, Any],
                         dehydrated_t: Dict[str, Any],
                         alias_map: dict) -> Dict[str, Any]:
    author = api_t.get("author") or {}
    entities = api_t.get("entities") or {}

    is_assistant = (
        str(author.get("id")) == GROK_USER_ID or
        (author.get("userName") or "").lower() == "grok"
    )

    hydrated = dict(api_t)  # passthrough ALL API fields

    hydrated.update({
        "id": str(dehydrated_t["id"]),
        "inReplyToId": dehydrated_t.get("inReplyToId"),
        "original_text": api_t.get("text", ""),
        "text": clean_text_with_map(api_t.get("text", ""), alias_map),
        "createdAt": dehydrated_t.get("createdAt") or api_t.get("createdAt"),
        "lang": dehydrated_t.get("lang") or api_t.get("lang"),
        "isMediaOnly": dehydrated_t.get("isMediaOnly"),

        # UPDATED COUNTS (API-first, fallback to dehydrated)
        "likeCount": api_t.get("likeCount", dehydrated_t.get("likeCount", 0)),
        "retweetCount": api_t.get("retweetCount", dehydrated_t.get("retweetCount", 0)),
        "replyCount": api_t.get("replyCount", dehydrated_t.get("replyCount", 0)),
        "quoteCount": api_t.get("quoteCount", dehydrated_t.get("quoteCount", 0)),
        "viewCount": api_t.get("viewCount", dehydrated_t.get("viewCount", 0)),
        "bookmarkCount": api_t.get("bookmarkCount", dehydrated_t.get("bookmarkCount", 0)),

        "author": {
            **author,
            "isAssistant": is_assistant
        },
        "entities": entities
    })

    return hydrated


def hydrate_thread(thread: Dict[str, Any],
                   conversation_id: str,
                   alias_map: dict) -> Dict[str, Any]:

    tweet_ids = [str(t["id"]) for t in thread.get("tweets", [])]
    hydrated_tweets = []
    missing_count = 0

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
                str(t["id"]): t for t in api_items
                if isinstance(t, dict) and "id" in t
            }
        except Exception:
            logging.exception(
                "❌ Failed API call for thread %s (conversation %s)",
                thread.get("threadId"), conversation_id
            )

    for dt in thread.get("tweets", []):
        tid = str(dt.get("id"))
        api_t = api_map.get(tid)

        if not api_t:
            missing_count += 1
            logging.warning(
                "🔴 Missing tweet %s | thread=%s | conversation=%s",
                tid, thread.get("threadId"), conversation_id
            )
            hydrated_tweets.append({
                "id": tid,
                "missing": True
            })
            continue

        hydrated_tweets.append(build_hydrated_tweet(api_t, dt, alias_map))

    if missing_count:
        logging.warning(
            "🔴 Thread %s had %d missing tweets (conversation %s)",
            thread.get("threadId"), missing_count, conversation_id
        )

    return {
        "threadId": thread["threadId"],
        "conversation_id": thread["conversation_id"],
        "hasMissingTweets": missing_count,
        "truncatedThread": thread["truncatedThread"],
        "validTweetCount": thread["validTweetCount"],
        "deletedTweetCount": thread["deletedTweetCount"],
        "annotations": thread.get("annotations", {}),
        "tweets": hydrated_tweets
    }


# --------------------------------------------------
# Streaming driver
# --------------------------------------------------

def write_hydrated(in_path: str, out_path: str):
    with open(out_path, "w", encoding="utf-8") as fout:
        fout.write("[\n")
        first = True

        with open(in_path, "rb") as fin:
            for conv in ijson.items(fin, "item"):
                cid = conv["conversationId"]
                logging.info("💧 Hydrating conversation %s", cid)

                alias_map = {}

                hydrated_threads = [
                    hydrate_thread(t, cid, alias_map)
                    for t in conv.get("threads", [])
                ]

                hydrated_conv = {
                    "conversationId": cid,
                    "annotations": conv.get("annotations", {}),
                    "threads": hydrated_threads
                }

                if not first:
                    fout.write(",\n")
                json.dump(hydrated_conv, fout, ensure_ascii=False, indent=2)
                first = False

        fout.write("\n]\n")


# --------------------------------------------------
# CLI
# --------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True)
    ap.add_argument("--out", dest="out_path", required=True)
    args = ap.parse_args()

    setup_logging(
        run_name="rehydrate",
        log_dir="rehydration/logs",
        to_stdout=False
    )

    logging.info("🚀 Starting rehydration")
    logging.info("Input: %s", args.in_path)
    logging.info("Output: %s", args.out_path)

    write_hydrated(args.in_path, args.out_path)

    logging.info("✅ Rehydration complete")


if __name__ == "__main__":
    main()
