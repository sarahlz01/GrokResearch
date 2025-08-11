import logging
from typing import Dict, List, Optional
import json
import os
from storage import init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


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
    parts = [f"from:{handle}","to:taka_i_32", "filter:replies"]
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

def save_fields(t: dict) -> dict:
    """Top-level tweet formatter (ordered) + normalized nested tweets."""
    out = _trim_tweet_core(t)
    out["quoted_tweet"]    = _format_nested_tweet(t.get("quoted_tweet"),    MAX_NESTED_TWEET_DEPTH)
    out["retweeted_tweet"] = _format_nested_tweet(t.get("retweeted_tweet"), MAX_NESTED_TWEET_DEPTH)
    return out

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

            logger.debug(
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

# ----------- Convert SQL to JSON --------------
def export_json_from_db(out_path: str, grok_username: str = "grok"):
    """
    Build final JSON from SQLite:
    [
      {
        "conversationId": "...",
        "threads": [
          { "threadId": "<grok reply id>", "tweets": [ ... ] }
        ]
      }
    ]
    - Includes the root/original tweet in every branch (loose mode).
    - Orders tweets by true timestamp, ascending.
    - threadId = latest Grok reply in the branch (matches your old JSON).
    """
    if init_db is None:
        logging.error("SQLite export requested but storage/init_db is not available.")
        return None

    from datetime import datetime, timezone

    def parse_dt(s: Optional[str]) -> datetime:
        if not s:
            return datetime.min.replace(tzinfo=timezone.utc)
        try:
            # Example: "Mon Aug 04 17:13:55 +0000 2025"
            return datetime.strptime(s, "%a %b %d %H:%M:%S %z %Y")
        except Exception:
            return datetime.min.replace(tzinfo=timezone.utc)

    conn = init_db()
    cur = conn.execute("SELECT DISTINCT conversation_id FROM tweets ORDER BY conversation_id")
    conv_ids = [row[0] for row in cur.fetchall() if row[0]]

    conversations: List[dict] = []

    for conv_id in conv_ids:
        # Pull all tweets for this conversation (already normalized JSON in 'json' column)
        cur = conn.execute("SELECT json FROM tweets WHERE conversation_id=?", (conv_id,))
        rows = cur.fetchall()
        if not rows:
            continue

        tweets: List[dict] = [json.loads(r[0]) for r in rows]
        by_id: Dict[str, dict] = {t.get("id"): t for t in tweets if t.get("id")}
        parent: Dict[str, Optional[str]] = {t.get("id"): t.get("inReplyToId") for t in tweets if t.get("id")}
        root_id = conv_id
        root_tweet = by_id.get(root_id)

        # Identify Grok replies present in this conversation
        grok_replies: List[str] = []
        for t in tweets:
            if (t.get("author") or {}).get("userName") == grok_username and t.get("isReply") and t.get("id"):
                grok_replies.append(t["id"])
        if not grok_replies:
            # No Grok replies for this conversation — skip
            continue

        # Compute branch key: walk up via inReplyToId until parent == root → return that child (first-child under root)
        def branch_key_for(tid: str) -> str:
            seen = set()
            cur_id = tid
            while cur_id and cur_id not in seen:
                seen.add(cur_id)
                p = parent.get(cur_id)
                if p == root_id:
                    return cur_id  # first child under root
                if p is None or p not in parent:
                    return cur_id  # fallback: highest known ancestor
                cur_id = p
            return tid

        # Assign each tweet to a branch (root will get its own key == root_id)
        tweet_branch: Dict[str, str] = {}
        for t in tweets:
            tid = t.get("id")
            if tid:
                tweet_branch[tid] = branch_key_for(tid)

        # Group Grok replies by branch key
        branch_to_groks: Dict[str, List[str]] = {}
        for rid in grok_replies:
            key = tweet_branch.get(rid, rid)
            branch_to_groks.setdefault(key, []).append(rid)

        threads_out: List[dict] = []

        for branch_key, rids in branch_to_groks.items():
            # Collect tweets whose computed branch == this branch_key
            branch_tweets = [t for t in tweets if tweet_branch.get(t.get("id")) == branch_key]

            # LOOSEN: include the root/original tweet in every branch (if present)
            if root_tweet is not None:
                branch_tweets.append(root_tweet)

            # Deduplicate by id and sort by true timestamp (ascending)
            seen_ids: Set[str] = set()
            ordered: List[dict] = []
            # Sort by datetime; tie-break by id for stability
            branch_tweets.sort(key=lambda x: (parse_dt(x.get("createdAt")), x.get("id") or ""))
            for t in branch_tweets:
                tid = t.get("id")
                if tid and tid not in seen_ids:
                    seen_ids.add(tid)
                    ordered.append(t)

            # Representative = LATEST Grok reply by createdAt in this branch (matches your old JSON)
            grok_in_branch = [by_id[g] for g in rids if g in by_id]
            if grok_in_branch:
                rep = max(grok_in_branch, key=lambda x: (parse_dt(x.get("createdAt")), x.get("id") or "")).get("id")
            else:
                rep = rids[-1]  # fallback

            threads_out.append({
                "threadId": rep,
                "tweets": ordered
            })

        conversations.append({
            "conversationId": conv_id,
            "threads": threads_out
        })

    save_json(conversations, out_path)
    logging.info("Exported JSON from DB to %s (conversations: %d)", out_path, len(conversations))
    return conversations
