# setup logging
import logging
logging.getLogger(__name__)

from typing import Dict, List, Optional, Set
import json, os
from storage import init_db

def format_time_utc(ts: str) -> str:
    ts = ts.strip()
    if "_UTC" in ts: return ts
    if " " in ts: date, hms = ts.split(" ", 1)
    else: date, hms = ts, "00:00:00"
    return f"{date}_{hms}_UTC"

def build_query(handle: str,
                include_self_threads: bool = False,
                include_quotes: bool = False,
                include_retweets: bool = False,
                since: Optional[str] = None,
                until: Optional[str] = None) -> str:
    parts = [f"from:{handle}", "filter:replies"] # !! remove the entire "to:___" element 
    parts.append("filter:retweets" if include_retweets else "-filter:retweets")
    parts.append("filter:quote" if include_quotes else "-filter:quote")
    parts.append("filter:self_threads" if include_self_threads else "-filter:self_threads")
    if since: parts.append(f"since:{format_time_utc(since)}")
    if until: parts.append(f"until:{format_time_utc(until)}")
    query = " ".join(parts)
    logging.info("Built query:\t%s", query)
    return query

# ---------- Ordered field selection ----------
TWEET_KEY_ORDER = [
    "type", "id", "url", "twitterUrl", "text",
    "retweetCount", "replyCount", "quoteCount",
    "createdAt", "lang", "bookmarkCount", "isReply",
    "inReplyToId", "conversationId", "inReplyToUserId", "inReplyToUsername",
    "possiblySensitive"
]

AUTHOR_KEY_ORDER = [
    "type", "userName", "url", "twitterUrl", "id",
    "followers", "following", "createdAt", "protected"
]

MAX_NESTED_TWEET_DEPTH = 1

def _trim_author(a: Optional[dict]) -> Optional[dict]:
    if not isinstance(a, dict):
        return None
    out = {}
    for k in AUTHOR_KEY_ORDER:
        out[k] = a.get(k)
    return out

def _trim_tweet_core(t: dict) -> dict:
    out = {}
    for k in TWEET_KEY_ORDER:
        out[k] = t.get(k)
    out["author"] = _trim_author(t.get("author"))
    return out

def _format_nested_tweet(t: Optional[dict], remaining_depth: int, seen_ids: Optional[set] = None) -> Optional[dict]:
    """Normalize quoted/retweeted tweets to the same schema and key order, with bounded depth."""
    if not isinstance(t, dict) or remaining_depth <= 0:
        return None
    if seen_ids is None:
        seen_ids = set()
    tid = t.get("id")
    if tid is not None:
        if tid in seen_ids:
            return None
        seen_ids.add(tid)
    base = _trim_tweet_core(t)
    next_depth = remaining_depth - 1
    base["quoted_tweet"]    = _format_nested_tweet(t.get("quoted_tweet"),    next_depth, seen_ids)
    base["retweeted_tweet"] = _format_nested_tweet(t.get("retweeted_tweet"), next_depth, seen_ids)
    return base

def save_fields_old(t: dict) -> dict:
    """Top-level tweet formatter (ordered) + normalized nested tweets."""
    out = _trim_tweet_core(t)
    out["quoted_tweet"]    = _format_nested_tweet(t.get("quoted_tweet"),    MAX_NESTED_TWEET_DEPTH)
    out["retweeted_tweet"] = _format_nested_tweet(t.get("retweeted_tweet"), MAX_NESTED_TWEET_DEPTH)
    return out

def save_fields(t: dict) -> dict: # NO TRIMMING
    return t

def _items_from_thread_page(page: dict) -> List[dict]:
    """Prefer 'replies', fall back to 'tweets' (twitterapi.io sometimes uses either)."""
    if isinstance(page.get("replies"), list):
        return page.get("replies") or []
    if isinstance(page.get("tweets"), list):
        return page.get("tweets") or []
    return []

# ---------- NEW: Build conversations grouped by threads (reply IDs) ----------

def build_conversation_objects_by_threads(
    conv_to_reply_pages: Dict[str, Dict[str, List[dict]]]
) -> List[dict]:
    """
    Input:
      {
        "<conversationId>": {
          "<grok_reply_id_1>": [ {<raw page>}, ... ],
          "<grok_reply_id_2>": [ {<raw page>}, ... ],
          ...
        },
        ...
      }

    Output per conversation (NO originalTweet at top level):
      {
        "conversationId": "<id>",
        "threads": [
          {
            "threadId": "<merged_grok_reply_id_for_branch>",
            "tweets": [ ...trimmed tweets for this branch (root INCLUDED if present)... ],
            "pages": [
              { "has_next_page": bool, "next_cursor": str|None, "status": str|None, "msg": str|None },
              ...
            ]
          },
          ...
        ]
      }
    """
    conversations: List[dict] = []

    for conv_id, threads_dict in (conv_to_reply_pages or {}).items():
        root_id = conv_id  # included inside threads

        # 1) Build id -> inReplyToId map from ALL raw pages in this conversation
        reply_map: Dict[str, Optional[str]] = {}
        for _, pages in threads_dict.items():
            for page in pages or []:
                # pull items from either 'replies' or 'tweets'
                items = _items_from_thread_page(page)
                for tw in items:
                    tid = tw.get("id")
                    if tid:
                        reply_map[tid] = tw.get("inReplyToId")

        # 2) Pre-trim pages per reply id (we trim tweets now; pages will hold only pagination later)
        per_rid_pages_trimmed: Dict[str, List[dict]] = {}
        rid_order: List[str] = []
        for rid, pages in threads_dict.items():
            rid_order.append(rid)
            trimmed_pages: List[dict] = []
            for page in pages or []:
                raw_items = _items_from_thread_page(page)
                page_tweets = [save_fields(t) for t in raw_items]
                # store tweets temporarily for merging; we won't put them under 'pages' in the final output
                trimmed_pages.append({
                    "tweets": page_tweets,
                    "has_next_page": page.get("has_next_page"),
                    "next_cursor": page.get("next_cursor"),
                    "status": page.get("status"),
                    "msg": page.get("msg"),
                })
            per_rid_pages_trimmed[rid] = trimmed_pages

        # 3) Branch key for each Grok reply:
        #    walk up inReplyToId until the parent is the root; that child-of-root is the branch key.
        def branch_key_for(rid: str) -> str:
            seen = set()
            cur = rid
            while cur and cur not in seen:
                seen.add(cur)
                parent = reply_map.get(cur)
                if parent == root_id:
                    return cur  # first child under the root; defines branch
                if parent is None or parent not in reply_map:
                    # parent unknown; fallback to highest ancestor we reached
                    return cur
                cur = parent
            return rid  # conservative fallback

        # 4) Group reply ids by branch key (preserve discovery order)
        branch_order: List[str] = []
        grouped: Dict[str, List[str]] = {}
        for rid in rid_order:
            key = branch_key_for(rid)
            if key not in grouped:
                grouped[key] = []
                branch_order.append(key)
            grouped[key].append(rid)

        # 5) Merge threads per branch (loose across branches; dedupe within branch)
        threads_out: List[dict] = []
        for key in branch_order:
            group_rids = grouped[key]              # reply ids in this branch, discovery order
            representative = group_rids[0]         # earliest reply id becomes the threadId

            logging.debug(
                "Conversation %s → merging Grok replies into branch %s: %s",
                conv_id, representative, group_rids
            )

            seen_ids = set()
            merged_tweets: List[dict] = []
            for rid in group_rids:
                for page in per_rid_pages_trimmed.get(rid, []):
                    # filter tweets for this merged branch (dedupe by tweet id; keep root)
                    filtered: List[dict] = []
                    for tw in page.get("tweets", []) or []:
                        tid = tw.get("id")
                        if not tid or tid in seen_ids:
                            continue
                        seen_ids.add(tid)
                        filtered.append(tw)

                    # append filtered tweets to the thread-level list
                    if filtered:
                        merged_tweets.extend(filtered)

            threads_out.append({
                "threadId": representative,
                "tweets": merged_tweets,
            })

        conversations.append({
            "conversationId": conv_id,
            "threads": threads_out
        })

    return conversations

# ---------- Save helper ----------
def save_json(obj: List[dict], path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    logging.info("Saved %d conversations to %s", len(obj), path)
CHECKPOINT_KEY_TMPL = "export:{path}:last_ts"

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


# ------------------------------------
# Pass 2: TRANSFORM (RAW → THREADED)
# ------------------------------------
def transform_conversations_to_threads(
    raw_path: str,
    out_path: str,
    grok_username: str = "grok",
    merge_single_grok: bool = True,
    include_root_once_in_first: bool = True,  # used only for the single-thread collapse case
) -> List[dict]:
    """
    Read the raw dump and produce threaded JSON per conversation:
      [
        {
          "conversationId": "...",
          "threads": [{"threadId": "...", "tweets": [...]}, ...],
          "hasMissingParent": bool,
          "hasMultipleThreads": bool
        },
        ...
      ]
    """
    with open(raw_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    out_list: List[dict] = []

    for conv in raw:
        conv_id = conv.get("conversationId")
        tweets = [t for t in (conv.get("tweets") or []) if isinstance(t, dict)]
        if not conv_id or not tweets:
            out_list.append({"conversationId": conv_id, "threads": [], "hasMissingParent": False, "hasMultipleThreads": False})
            continue

        by_id: Dict[str, dict] = {}
        parent: Dict[str, Optional[str]] = {}
        ts_by_id: Dict[str, int] = {}
        grok_ids: Set[str] = set()

        for t in tweets:
            tid = t.get("id")
            if not tid:
                continue
            by_id[tid] = t
            parent[tid] = t.get("inReplyToId") or t.get("parent_id")  # support either field
            ts_by_id[tid] = int(t.get("created_at_ts") or t.get("createdAtTs") or 0)
            if (t.get("isReply") and t.get("userName") == grok_username) or t.get("is_grok_reply"):
                grok_ids.add(tid)

        ordered_ids = sorted(by_id.keys(), key=lambda i: (ts_by_id.get(i, 0), i))
        root_id = conv_id
        root_tweet = by_id.get(root_id)

        # --- detect locally-missing parents (likely deleted/never-scraped)
        has_missing_parent = False
        for tid in ordered_ids:
            pid = parent.get(tid)
            if pid and pid not in by_id:  # parent referenced but not present in this convo's raw tweets
                has_missing_parent = True
                break

        def branch_key_for(tid: str) -> str:
            seen = set()
            cur = tid
            while cur and cur not in seen:
                seen.add(cur)
                p = parent.get(cur)
                if p == root_id:
                    return cur         # first child under root
                if p is None or p not in parent:
                    return cur         # highest known ancestor (handles missing parents)
                cur = p
            return tid

        branch_of: Dict[str, str] = {tid: branch_key_for(tid) for tid in ordered_ids}

        # Branch discovery order (skip root)
        branch_order: List[str] = []
        seen_branches = set()
        for tid in ordered_ids:
            if tid == root_id:
                continue
            b = branch_of.get(tid)
            if b and b not in seen_branches:
                seen_branches.add(b)
                branch_order.append(b)

        # Map branches → Grok ids
        branch_to_groks: Dict[str, List[str]] = {}
        for gid in grok_ids:
            key = branch_of.get(gid, gid)
            branch_to_groks.setdefault(key, []).append(gid)

        # Collapse to one thread if exactly one Grok (preferred “before” shape)
        if merge_single_grok and len(grok_ids) == 1:
            merged = []
            seen_ids: Set[str] = set()
            if include_root_once_in_first and root_tweet is not None and root_id not in seen_ids:
                merged.append(by_id[root_id]); seen_ids.add(root_id)
            for tid in ordered_ids:
                if tid in by_id and tid not in seen_ids:
                    merged.append(by_id[tid]); seen_ids.add(tid)
            only_grok = next(iter(grok_ids))
            threads_out = [{"threadId": only_grok, "tweets": merged}]
            out_list.append({
                "conversationId": conv_id,
                "threads": threads_out,
                "hasMissingParent": has_missing_parent,
                "hasMultipleThreads": False
            })
            continue

        # Otherwise: one thread per branch with global de-dup (root will be added to every thread)
        global_seen: Set[str] = set()
        threads_out: List[dict] = []

        for bkey in branch_order:
            branch_ids = [tid for tid in ordered_ids if branch_of.get(tid) == bkey]

            ordered = []
            for tid in branch_ids:
                if tid in by_id and tid not in global_seen:
                    global_seen.add(tid)
                    ordered.append(by_id[tid])

            if not ordered:
                continue

            groks_here = branch_to_groks.get(bkey, [])
            if groks_here:
                rep = max(groks_here, key=lambda i: ts_by_id.get(i, -1))
            else:
                rep = max((t["id"] for t in ordered), key=lambda i: ts_by_id.get(i, -1))

            threads_out.append({"threadId": rep, "tweets": ordered})

        # Insert root at the start of EVERY thread (if present)
        if root_tweet is not None and threads_out:
            for th in threads_out:
                if not th["tweets"] or th["tweets"][0].get("id") != root_id:
                    present_ids = {t.get("id") for t in th["tweets"] if isinstance(t, dict)}
                    if root_id not in present_ids:
                        th["tweets"].insert(0, by_id[root_id])
                    else:
                        th["tweets"] = [by_id[root_id]] + [t for t in th["tweets"] if t.get("id") != root_id]

        out_list.append({
            "conversationId": conv_id,
            "threads": threads_out,
            "hasMissingParent": has_missing_parent,
            "hasMultipleThreads": len(threads_out) > 1
        })

    # write out
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out_list, f, ensure_ascii=False, indent=2)

    logging.info("Transform complete: %d conversation(s) → %s", len(out_list), out_path)
    return out_list



def export_json_from_db(out_path: str, grok_username: str = "grok"):
    raw_path = "./grok_data/raw.json"
    dump_conversations_raw(raw_path)
    return transform_conversations_to_threads(raw_path, out_path, grok_username=grok_username)
