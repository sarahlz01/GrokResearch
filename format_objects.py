import logging
from typing import Dict, List, Optional, Tuple, Set
import json, os, tempfile
from storage import init_db, load_checkpoint, save_checkpoint

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
    parts = [f"from:{handle}", "filter:replies"]
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
CHECKPOINT_KEY_TMPL = "export:{path}:last_ts"

def _atomic_write_json(obj: List[dict], out_path: str) -> None:
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp_export_", dir=os.path.dirname(out_path) or ".")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
        os.replace(tmp, out_path)
    finally:
        try:
            os.remove(tmp)
        except OSError:
            pass

def export_json_from_db(out_path: str, grok_username: str = "grok"):
    """
    Incremental JSON export:
      - Rebuilds only conversations that have tweets with created_at_ts > last checkpoint.
      - Leaves other conversations untouched by merging into the existing JSON (if any).
      - Uses DB fields (created_at_ts, parent_id, is_grok_reply) to simplify logic.
    """
    if init_db is None:
        logger.error("SQLite export requested but storage/init_db is not available.")
        return None

    conn = init_db()
    ck_key = CHECKPOINT_KEY_TMPL.format(path=os.path.abspath(out_path))
    last_ts_s = load_checkpoint(conn, ck_key)
    last_ts = int(last_ts_s) if last_ts_s and last_ts_s.isdigit() else 0  # initial full export

    # Load existing JSON (so we only replace convs that changed)
    existing: Dict[str, dict] = {}
    if os.path.exists(out_path):
        try:
            for c in json.load(open(out_path, "r", encoding="utf-8")):
                cid = c.get("conversationId")
                if cid:
                    existing[cid] = c
        except Exception:
            logger.warning("Existing JSON unreadable; rebuilding from scratch.")

    # Find conversations that changed since last checkpoint
    cur = conn.execute(
        "SELECT DISTINCT conversation_id FROM tweets WHERE created_at_ts > ?",
        (last_ts,),
    )
    changed_convs = {r[0] for r in cur.fetchall() if r[0]}

    # Also include conversations missing from the file (first export or new convs)
    if existing:
        cur = conn.execute("SELECT DISTINCT conversation_id FROM tweets")
        all_convs = {r[0] for r in cur.fetchall() if r[0]}
        changed_convs |= (all_convs - set(existing.keys()))
    else:
        cur = conn.execute("SELECT DISTINCT conversation_id FROM tweets")
        changed_convs = {r[0] for r in cur.fetchall() if r[0]}

    def build_conversation(conv_id: str) -> Optional[dict]:
        # Pull id-level data once; sort by (created_at_ts, id) stably
        rows = conn.execute(
            "SELECT id, parent_id, is_grok_reply, created_at_ts, json "
            "FROM tweets WHERE conversation_id=?",
            (conv_id,),
        ).fetchall()
        if not rows:
            return None

        # Unpack minimal vectors
        by_id: Dict[str, dict] = {}
        parent: Dict[str, Optional[str]] = {}
        grok_ids: Set[str] = set()
        tweets: List[Tuple[int, str]] = []  # (created_at_ts, id)

        for tid, pid, is_grok, ts, j in rows:
            t = json.loads(j)
            by_id[tid] = t
            parent[tid] = pid
            if is_grok:  # computed in upsert_tweets using userName + isReply
                grok_ids.add(tid)
            tweets.append((ts or 0, tid))

        if not grok_ids:
            return None  # skip convs without Grok replies

        root_id = conv_id
        root_tweet = by_id.get(root_id)

        # Compute branch key: walk up via parent until parent == root → that child is the branch
        def branch_key_for(tid: str) -> str:
            seen = set()
            cur = tid
            while cur and cur not in seen:
                seen.add(cur)
                p = parent.get(cur)
                if p == root_id:
                    return cur
                if p is None or p not in parent:
                    return cur
                cur = p
            return tid

        # Assign each tweet to a branch
        branch_of: Dict[str, str] = {tid: branch_key_for(tid) for _, tid in tweets}

        # Group Grok reply ids by branch
        branch_to_groks: Dict[str, List[str]] = {}
        for gid in grok_ids:
            key = branch_of.get(gid, gid)
            branch_to_groks.setdefault(key, []).append(gid)

        # Fast, stable ordering with DB timestamps (no datetime parsing)
        tweets.sort(key=lambda p: (p[0], p[1]))  # (created_at_ts, id)

        threads_out: List[dict] = []
        for bkey, groks in branch_to_groks.items():
            # All tweets that map to this branch
            branch_ids = [tid for _, tid in tweets if branch_of.get(tid) == bkey]

            # Include the root/original in every branch (loose mode), if present
            if root_tweet is not None and root_id not in branch_ids:
                branch_ids.insert(0, root_id)

            # Deduplicate in order
            seen: Set[str] = set()
            ordered = []
            for tid in branch_ids:
                if tid not in seen:
                    seen.add(tid)
                    ordered.append(by_id[tid])

            # Representative = latest Grok reply by timestamp in this branch
            groks_sorted = sorted(groks, key=lambda tid: next((ts for ts, i in tweets if i == tid), -1))
            rep = groks_sorted[-1] if groks_sorted else groks[-1]

            threads_out.append({"threadId": rep, "tweets": ordered})

        return {"conversationId": conv_id, "threads": threads_out}

    # Rebuild changed conversations
    for cid in changed_convs:
        conv_obj = build_conversation(cid)
        if conv_obj is not None:
            existing[cid] = conv_obj

    # Write merged list atomically
    merged = [existing[cid] for cid in sorted(existing.keys())]
    _atomic_write_json(merged, out_path)

    # Advance checkpoint to the latest seen timestamp in DB
    cur = conn.execute("SELECT MAX(created_at_ts) FROM tweets")
    max_ts = cur.fetchone()[0] or last_ts
    save_checkpoint(conn, ck_key, str(max_ts))

    logger.info("Exported %d conversation(s): %s (updated: %d, last_ts=%s)",
                len(merged), out_path, len(changed_convs), max_ts)
    return merged