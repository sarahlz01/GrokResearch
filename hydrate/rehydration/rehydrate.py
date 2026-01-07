#!/usr/bin/env python3
"""
Rehydrate a dehydrated GrokSet JSON back into the hydrated release schema.

Preserves:
- conversation.annotations (topic, trolling, discussion)
- thread.annotations (toxicity)
- all computed fields

Rehydrates:
- tweet text / original_text
- author metadata
- entities (including user_mentions)
- url
- any extra twitterapi.io fields

Run:
  python rehydrate.py --in dehydrated.json --out hydrated.json
"""

import argparse
import json
import logging
import os
from typing import Dict, Any, List

import ijson

from setuplog import setup_logging
from network.http import http_get
from cleaning.clean_objects_while_scraping import extract_items
from cleaning.clean_threads import clean_text_with_map, GROK_USER_ID


# ---------------- Helpers ----------------

def build_hydrated_tweet(api_t: Dict[str, Any],
                         dehydrated_t: Dict[str, Any],
                         alias_map: dict) -> Dict[str, Any]:
    """
    Merge dehydrated tweet shell + API tweet payload.
    """
    author = api_t.get("author") or {}
    entities = api_t.get("entities") or {}

    is_assistant = str(author.get("id")) == GROK_USER_ID or \
                   (author.get("userName") or "").lower() == "grok"

    hydrated = dict(api_t)  # include *all* twitterapi.io fields

    hydrated.update({
        "id": str(dehydrated_t["id"]),
        "inReplyToId": dehydrated_t.get("inReplyToId"),
        "original_text": api_t.get("text", ""),
        "text": clean_text_with_map(api_t.get("text", ""), alias_map),
        "createdAt": dehydrated_t.get("createdAt") or api_t.get("createdAt"),
        "lang": dehydrated_t.get("lang") or api_t.get("lang"),
        "isMediaOnly": dehydrated_t.get("isMediaOnly"),
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
    """
    Hydrate one thread using a single /twitter/tweets call.
    """
    tweet_ids = [t["id"] for t in thread.get("tweets", [])]
    hydrated_tweets = []

    if not tweet_ids:
        return {**thread, "tweets": []}

    try:
        page = http_get(
            "/twitter/tweets",
            params={"tweet_ids": ",".join(tweet_ids)},
            conversation_id=conversation_id
        )
        _, api_items = extract_items(page)
    except Exception as e:
        logging.exception(
            "❌ Failed thread hydration %s (conversation %s)",
            thread.get("threadId"), conversation_id
        )
        api_items = []

    api_map = {str(t["id"]): t for t in api_items if isinstance(t, dict)}

    for dt in thread["tweets"]:
        api_t = api_map.get(str(dt["id"]))
        if not api_t:
            logging.warning(
                "⚠️ Missing tweet %s in thread %s",
                dt["id"], thread.get("threadId")
            )
            continue
        hydrated_tweets.append(build_hydrated_tweet(api_t, dt, alias_map))

    return {
        "threadId": thread["threadId"],
        "conversation_id": thread["conversation_id"],
        "hasMissingTweets": thread["hasMissingTweets"],
        "truncatedThread": thread["truncatedThread"],
        "validTweetCount": thread["validTweetCount"],
        "deletedTweetCount": thread["deletedTweetCount"],
        "annotations": thread.get("annotations", {}),
        "tweets": hydrated_tweets
    }


# ---------------- Streaming driver ----------------

def write_hydrated(in_path: str, out_path: str):
    with open(out_path, "w", encoding="utf-8") as out_f:
        out_f.write("[\n")
        first = True

        for conv in ijson.items(open(in_path, "rb"), "item"):
            conversation_id = conv["conversationId"]
            alias_map = {}

            logging.info("Hydrating conversation %s", conversation_id)

            hydrated_threads = [
                hydrate_thread(th, conversation_id, alias_map)
                for th in conv.get("threads", [])
            ]

            hydrated_conv = {
                "conversationId": conversation_id,
                "annotations": conv.get("annotations", {}),
                "threads": hydrated_threads
            }

            if not first:
                out_f.write(",\n")
            json.dump(hydrated_conv, out_f, ensure_ascii=False, indent=2)
            first = False

        out_f.write("\n]\n")


# ---------------- CLI ----------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True)
    ap.add_argument("--out", dest="out_path", required=True)
    args = ap.parse_args()

    os.makedirs("hydration/logs", exist_ok=True)
    setup_logging(run_name="rehydrate", log_dir="hydration/logs")

    logging.info("🚀 Starting rehydration")
    logging.info("Input: %s", args.in_path)
    logging.info("Output: %s", args.out_path)

    write_hydrated(args.in_path, args.out_path)

    logging.info("✅ Rehydration complete")


if __name__ == "__main__":
    main()
