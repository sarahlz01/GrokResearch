#!/usr/bin/env python3
import argparse
import json
import re
from typing import Any, Dict, Iterable, List, Optional
import ijson

# Reuse your Grok user id constant for isAssistant
try:
    from cleaning.clean_threads import GROK_USER_ID  # uploaded file
except Exception:
    GROK_USER_ID = "1720665183188922368"  # fallback

# ---------- Helpers (no heavy deps, streaming-safe) ----------

_URLS_RE = re.compile(r'https?://\S+')
# Leading mentions block (ASCII @ or full-width ＠)
_LEADING_MENTIONS_BLOCK = re.compile(r'^(?:\s*[@＠][A-Za-z0-9_]{1,15}[^\S\r\n]*)+')

def _as_int(x: Any) -> Optional[int]:
    try:
        return int(x)
    except Exception:
        return None

def _bool(x: Any) -> bool:
    return bool(x)

def _strip_mediaish(text: Optional[str]) -> str:
    """
    Remove leading mentions block and URLs, then collapse whitespace.
    This lets us decide if a tweet has any remaining textual content.
    """
    if not isinstance(text, str):
        return ""
    s = text
    # drop leading mention runs
    s = _LEADING_MENTIONS_BLOCK.sub("", s).lstrip()
    # drop URLs
    s = _URLS_RE.sub("", s)
    # normalize spaces
    s = " ".join(s.split())
    return s

def _is_media_only(t: Dict[str, Any]) -> bool:
    """
    Heuristic: no remaining text after removing leading mentions + URLs,
    but there IS media/links present (extendedEntities.media OR entities.urls).
    """
    body = _strip_mediaish(t.get("text", ""))
    has_any_text = len(body) > 0

    entities = t.get("entities") or {}
    urls = entities.get("urls") or []
    has_urls = isinstance(urls, list) and len(urls) > 0

    ext = t.get("extendedEntities") or {}
    media = ext.get("media") or []
    has_media = isinstance(media, list) and len(media) > 0

    return (not has_any_text) and (has_media or has_urls)

def _is_assistant(author: Optional[Dict[str, Any]]) -> bool:
    a = author or {}
    return str(a.get("id")) == str(GROK_USER_ID)

def _pick_entities(t: Dict[str, Any]) -> Dict[str, List[str]]:
    """
    Keep only hashtags, symbols, urls (drop user_mentions for dehydrate).
    Normalize each to a simple list of strings.
    """
    ent = t.get("entities") or {}
    out: Dict[str, List[str]] = {"hashtags": [], "symbols": [], "urls": []}

    # hashtags: objects may have {text: "..."} or {tag: "..."} or just strings
    for h in ent.get("hashtags") or []:
        if isinstance(h, dict):
            tag = h.get("text") or h.get("tag")
            if isinstance(tag, str):
                out["hashtags"].append(tag)
        elif isinstance(h, str):
            out["hashtags"].append(h)

    # symbols (cashtag-like) may be {"text": "TSLA"} or strings
    for s in ent.get("symbols") or []:
        if isinstance(s, dict):
            sym = s.get("text") or s.get("symbol")
            if isinstance(sym, str):
                out["symbols"].append(sym)
        elif isinstance(s, str):
            out["symbols"].append(s)

    # urls: choose expanded_url if available, else url
    for u in ent.get("urls") or []:
        if isinstance(u, dict):
            url = u.get("expanded_url") or u.get("url")
            if isinstance(url, str):
                out["urls"].append(url)
        elif isinstance(u, str):
            out["urls"].append(u)

    return out

def _pick_author_fields(a: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    a = a or {}
    return {
        "isVerified": _bool(a.get("isVerified")),
        "isBlueVerified": _bool(a.get("isBlueVerified")),
        "followers": _as_int(a.get("followers")),
        "following": _as_int(a.get("following")),
        "isAssistant": _is_assistant(a),
    }

def _pick_tweet_fields(t: Dict[str, Any]) -> Dict[str, Any]:
    out = {
        "id": str(t.get("id")),
        # %original_text and %*text are **hydration-only** → intentionally omitted here
        "retweetCount": _as_int(t.get("retweetCount")),
        "replyCount": _as_int(t.get("replyCount")),
        "likeCount": _as_int(t.get("likeCount")),
        "quoteCount": _as_int(t.get("quoteCount")),
        "viewCount": _as_int(t.get("viewCount")),
        "createdAt": t.get("createdAt"),
        "lang": t.get("lang"),
        "bookmarkCount": _as_int(t.get("bookmarkCount")),
        "isMediaOnly": _is_media_only(t),
        "author": _pick_author_fields(t.get("author")),
        "entities": _pick_entities(t),
    }
    # pass through annotations if present
    if "annotations" in t and isinstance(t["annotations"], dict):
        out["annotations"] = t["annotations"]
    return out

def _headless_flag(thread_tweets: List[Dict[str, Any]], conversation_id: str) -> bool:
    """
    A thread is headless if:
      1. Any tweet has _stop_reason == "non_assistant_limit"   (your original heuristic)
      OR
      2. The conversation root tweet (id == conversation_id) is NOT present in the thread.
    """
    # --- Condition 1: your original rule ---
    for tw in thread_tweets or []:
        if tw.get("_stop_reason") == "non_assistant_limit":
            return True

    # --- Condition 2: true headless thread detection ---
    ids_present = {str(t.get("id")) for t in (thread_tweets or []) if t.get("id")}
    if str(conversation_id) not in ids_present:
        return True

    return False


# ---------------------------------------------------------------------------

def _count_deleted_from_inreply(tweets: List[Dict[str, Any]]) -> int:
    """
    Count how many *distinct* inReplyToId values refer to tweets that are
    not present in this thread.

    Logic:
      - Collect all tweet IDs in the thread.
      - For each tweet, look at inReplyToId:
          * ignore if empty / falsy
          * if it's not in the thread's IDs, count that parent as "missing"
      - Return number of unique missing parent IDs.
    """
    ids_in_thread = {
        str(t.get("id"))
        for t in (tweets or [])
        if t.get("id") is not None
    }

    missing_parents: set[str] = set()
    for t in tweets or []:
        parent = t.get("inReplyToId") or ""
        if not parent:
            continue
        parent_str = str(parent)
        if parent_str not in ids_in_thread:
            missing_parents.add(parent_str)

    return len(missing_parents)


# --- replace your existing _dehydrate_thread with this version ---
def _dehydrate_thread(conv_id: str, thread_obj: Dict[str, Any]) -> Dict[str, Any]:
    tweets = thread_obj.get("tweets") or []

    # valid = tweets we actually have in this thread
    valid_count = len(tweets)

    # deleted = parents referenced by inReplyToId that never appear as tweet ids
    deleted_count = _count_deleted_from_inreply(tweets)

    _has_missing_parent = _bool(thread_obj.get("has_missing_parent"))
    # mark hasMissingTweets if either upstream flag is set OR we found missing parents
    _has_missing = _has_missing_parent or (deleted_count > 0) or _bool(thread_obj.get("tweet_counter_reached_150"))

    return {
        "threadId": str(thread_obj.get("threadId") or ""),
        "conversation_id": str(conv_id),
        "hasMissingTweets": _has_missing,
        "headlessThread": _headless_flag(tweets, conv_id),
        "validTweetCount": valid_count,
        "deletedTweetCount": deleted_count,
        "tweets": [_pick_tweet_fields(t) for t in tweets],
    }


# ---------- Streaming I/O ----------

def _iter_conversations(path: str) -> Iterable[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        # stream top-level array item by item
        for conv in ijson.items(f, "item", use_float=True):
            if isinstance(conv, dict) and "conversationId" in conv:
                yield conv

def write_dehydrated(in_path: str, out_path: str, flat: bool = False) -> None:
    """
    Stream the input, writing either:
      - grouped (default): one object per conversation with dehydrated threads, or
      - flat (--flat): one flat array of dehydrated thread objects.
    """
    with open(out_path, "w", encoding="utf-8") as out_f:
        out_f.write("[\n")
        first_out = True

        if flat:
            # flatten all threads directly
            for conv in _iter_conversations(in_path):
                conv_id = str(conv.get("conversationId"))
                for th in conv.get("threads") or []:
                    item = _dehydrate_thread(conv_id, th)
                    if not first_out:
                        out_f.write(",\n")
                    json.dump(item, out_f, ensure_ascii=False)
                    first_out = False

        else:
            # preserve conversation grouping
            for conv in _iter_conversations(in_path):
                conv_id = str(conv.get("conversationId"))
                dehydrated_threads = [
                    _dehydrate_thread(conv_id, th) for th in (conv.get("threads") or [])
                ]
                if not dehydrated_threads:
                    continue
                out_obj = {
                    "threads": dehydrated_threads,
                }
                if not first_out:
                    out_f.write(",\n")
                json.dump(out_obj, out_f, ensure_ascii=False, indent=2)
                first_out = False

        out_f.write("\n]\n")

# ---------- CLI ----------

def main():
    ap = argparse.ArgumentParser(description="Dehydrate a thread-organized dataset.")
    ap.add_argument("--in", dest="in_path", required=True, help="Input JSON (thread-organized).")
    ap.add_argument("--out", dest="out_path", required=True, help="Output DEHYDRATED JSON path.")
    ap.add_argument("--flat", action="store_true", help="Emit a flat list of threads (no conversation wrapper).")
    args = ap.parse_args()
    write_dehydrated(args.in_path, args.out_path, flat=args.flat)

if __name__ == "__main__":
    main()
