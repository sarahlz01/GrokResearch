#!/usr/bin/env python3
"""
dehydrate.py

STRICT dehydrated schema (drops anything "Needs hydration" in the paper table).

Computed fields:
- hasMissingTweets = incomplete_thread OR has_missing_parent
- truncatedThread = True if any tweet has _stop_reason in
    {"non_assistant_limit", "tweet_counter_reached_150"}
- validTweetCount = len(tweets)
- deletedTweetCount = 0 (for now)
- annotations = {}

isMediaOnly:
- For EACH tweet: True iff tweet text contains ONLY links.
  We remove URLs (using entities.urls + regex fallback), then check if anything remains.

isAssistant (revised):
- True iff BOTH:
    (author.userName.lower() == "grok") AND (str(author.id) == GROK_USER_ID)

Output:
- Pretty-printed JSON array to dehydrated.json (streaming, constant memory).

NOTE:
- This version ALWAYS includes tweet["text"] in the dehydrated output.
  (You can strip it later after toxicity merge.)
"""

import argparse
import json
import re
from typing import Any, Dict, List, Set

import ijson

STOP_REASONS_TRUNCATED: Set[str] = {"non_assistant_limit", "tweet_counter_reached_150"}

# Grok's X user id (from your example). Keep as string for robust comparisons.
GROK_USER_ID = "1720665183188922368"

# Regex fallback for urls in raw text
URL_RE = re.compile(r"(https?://\S+|www\.\S+|t\.co/\S+)", re.IGNORECASE)

# Strip leftover punctuation at edges after URL removal
LEFTOVER_PUNCT_RE = re.compile(
    r"^[\s\.,;:!?\(\)\[\]\{\}\"'`~\-–—_…]+|[\s\.,;:!?\(\)\[\]\{\}\"'`~\-–—_…]+$"
)


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(x)
    except Exception:
        return default


def _normalize_text(s: str) -> str:
    return " ".join((s or "").split())


def _strip_urls_from_text(text: str, entities: Dict[str, Any]) -> str:
    t = text or ""
    urls = (entities or {}).get("urls") or []

    # Remove URLs explicitly listed in entities
    for u in urls:
        if not isinstance(u, dict):
            continue
        for key in ("url", "expanded_url", "display_url"):
            val = u.get(key)
            if isinstance(val, str) and val:
                t = t.replace(val, " ")

    # Regex fallback
    t = URL_RE.sub(" ", t)

    t = _normalize_text(t)
    t = LEFTOVER_PUNCT_RE.sub("", t)
    return t.strip()


def _is_media_only_links(tweet: Dict[str, Any]) -> bool:
    text = tweet.get("text") or ""
    entities = tweet.get("entities") or {}
    remaining = _strip_urls_from_text(text, entities)
    return remaining == ""


def _is_assistant_author(author: Dict[str, Any]) -> bool:
    uname = (author.get("userName") or "").lower().strip()
    aid = str(author.get("id") or "").strip()
    return (uname == "grok") and (aid == GROK_USER_ID)


def convert_tweet(tweet: Dict[str, Any]) -> Dict[str, Any]:
    author = tweet.get("author") or {}
    ents = tweet.get("entities") or {}

    out: Dict[str, Any] = {}

    out["id"] = str(tweet.get("id") or "")
    out["inReplyToId"] = str(tweet.get("inReplyToId") or "")
    out["createdAt"] = tweet.get("createdAt") or ""
    out["lang"] = tweet.get("lang") or ""

    # ✅ ALWAYS keep original text (strip later after toxicity merge)
    out["text"] = tweet.get("text") or ""

    out["isMediaOnly"] = _is_media_only_links(tweet)

    out["likeCount"] = _safe_int(tweet.get("likeCount"), 0)
    out["retweetCount"] = _safe_int(tweet.get("retweetCount"), 0)
    out["replyCount"] = _safe_int(tweet.get("replyCount"), 0)
    out["quoteCount"] = _safe_int(tweet.get("quoteCount"), 0)
    out["viewCount"] = _safe_int(tweet.get("viewCount"), 0)
    out["bookmarkCount"] = _safe_int(tweet.get("bookmarkCount"), 0)

    out_author: Dict[str, Any] = {}
    out_author["isVerified"] = bool(author.get("isVerified", False))
    out_author["followers"] = _safe_int(author.get("followers"), 0)
    out_author["following"] = _safe_int(author.get("following"), 0)
    out_author["isAssistant"] = _is_assistant_author(author)
    out["author"] = out_author

    out_entities: Dict[str, Any] = {}
    out_entities["hashtags"] = ents.get("hashtags") or []
    out_entities["symbols"] = ents.get("symbols") or []
    out_entities["urls"] = ents.get("urls") or []
    out["entities"] = out_entities

    return out


def convert_thread(thread: Dict[str, Any], conversation_id: str) -> Dict[str, Any]:
    tweets_in: List[Dict[str, Any]] = thread.get("tweets") or []

    incomplete = bool(thread.get("incomplete_thread", False))
    has_missing_parent = bool(thread.get("has_missing_parent", False))
    has_missing_tweets = incomplete or has_missing_parent

    truncated = False
    for tw in tweets_in:
        sr = tw.get("_stop_reason")
        if isinstance(sr, str) and sr in STOP_REASONS_TRUNCATED:
            truncated = True
            break

    out: Dict[str, Any] = {}
    out["threadId"] = str(thread.get("threadId") or "")
    out["conversation_id"] = conversation_id
    out["hasMissingTweets"] = has_missing_tweets
    out["truncatedThread"] = truncated
    out["validTweetCount"] = len(tweets_in)
    out["deletedTweetCount"] = 0
    out["tweets"] = [convert_tweet(tw) for tw in tweets_in]

    return out


def convert_conversation(obj: Dict[str, Any]) -> Dict[str, Any]:
    conversation_id = str(obj.get("conversationId") or "")
    threads_in: List[Dict[str, Any]] = obj.get("threads") or []

    out: Dict[str, Any] = {}
    out["conversationId"] = conversation_id
    out["threads"] = [convert_thread(th, conversation_id) for th in threads_in]
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Path to the ORIGINAL large JSON file (top-level array).")
    ap.add_argument("--output", default="dehydrated.json", help="Output path (default: dehydrated.json).")
    ap.add_argument("--log-every", type=int, default=1000, help="Print progress every N conversations.")
    args = ap.parse_args()

    count = 0
    first = True

    with open(args.input, "rb") as fin, open(args.output, "w", encoding="utf-8") as fout:
        fout.write("[\n")

        for conv in ijson.items(fin, "item"):
            out_obj = convert_conversation(conv)

            if not first:
                fout.write(",\n")
            json.dump(out_obj, fout, ensure_ascii=False, indent=2)
            first = False

            count += 1
            if args.log_every > 0 and count % args.log_every == 0:
                print(f"[progress] converted {count} conversations...")

        fout.write("\n]\n")

    print(f"[done] wrote {count} conversations to {args.output}")


if __name__ == "__main__":
    main()
