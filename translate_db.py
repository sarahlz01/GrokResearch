import json
from collections import defaultdict
import os
import re

def build_threads_for_raw(raw_conversations):
    out = []
    for conv in raw_conversations:
        threads = threads_for_conversation(conv)  # from above
        out.append({
            "conversationId": conv["conversationId"],
            "threads": threads
        })
    return out

def threads_for_conversation(conv):
    tweets = conv["tweets"]
    id_map = {t["id"]: t for t in tweets}
    
    # Build parent->children index to find leaves
    children = defaultdict(list)
    for t in tweets:
        parent = t.get("inReplyToId")
        if parent:
            children[parent].append(t["id"])

    all_ids = set(id_map.keys())
    leaves = [tid for tid in all_ids if tid not in children]  # no children → leaf

    threads = []
    for leaf_id in leaves:
        chain_ids = []
        has_missing_parent = False
        cur = id_map[leaf_id]
        while True:
            chain_ids.append(cur["id"])
            parent_id = cur.get("inReplyToId")
            if parent_id is None:
                break
            parent = id_map.get(parent_id)
            if parent is None:
                has_missing_parent = True
                break
            cur = parent

        chain_ids.reverse()  # oldest → newest
        threads.append({
            "threadId": leaf_id,  # or last id, or hash(tuple(chain_ids))
            "has_missing_parent": has_missing_parent,
            "tweets": [id_map[tid] for tid in chain_ids],
        })

    # Optional: sort threads by (length desc, last createdAt), etc.
    threads = prune_threads_without_grok(threads) # get rid of threads that dont have grok
    threads = prune_single_tweet_threads(threads) # get rid of single length threads
    threads = ensure_root_in_threads(threads, conv) # add in the initial post to each thread
    return threads

# --- add this helper anywhere above translate(...) ---
def prune_threads_without_grok(threads):
    """
    Keep only threads that include at least one Grok reply.
    We identify Grok tweets by author.userName == 'grok' (case-insensitive).
    """
    def is_grok_tweet(t):
        author = t.get("author") or {}
        return str(author.get("id")) == "1720665183188922368"

    return [th for th in threads if any(is_grok_tweet(tw) for tw in th.get("tweets", []))]

def ensure_root_in_threads(threads, conv):
    """
    Make sure each thread contains the root tweet (id == conversationId).
    If it's already there, do nothing; otherwise, prepend it.
    """
    conv_id = str(conv["conversationId"])
    id_map = {str(t["id"]): t for t in conv["tweets"]}

    root_tweet = id_map.get(conv_id)
    if not root_tweet:
        return threads  # nothing to do if root isn't in the conversation

    for th in threads:
        ids = {str(tw["id"]) for tw in th.get("tweets", [])}
        if conv_id not in ids:
            th["tweets"].insert(0, root_tweet)
    return threads

def prune_single_tweet_threads(threads):
    """Remove threads that only contain a single tweet."""
    return [th for th in threads if len(th.get("tweets", [])) > 1]

def translate(raw_path, out_path):
    os.makedirs(os.path.dirname(raw_path) or ".", exist_ok=True)
    with open(raw_path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    
    # build the output with all metadata
    out = build_threads_for_raw(raw)
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    
    # build the cleaned output    
    cleaned = clean_conversations_minimal(out)
    cleaned_path = out_path.replace(".json", "_CLEANED.json")
    with open(cleaned_path, "w", encoding="utf-8") as f2:
        json.dump(cleaned, f2, ensure_ascii=False, indent=2)
        


# create the cleaned versions of the output for analysis
import re
GROK_USER_ID = "1720665183188922368"

# Leading @mentions block (remove entirely)
_LEADING_MENTIONS = re.compile(r'^(?:\s*@\w+\b[^\S\r\n]*)+')
# Plain word "grok"
_ANY_GROK_WORD = re.compile(r'\bgrok\b', re.IGNORECASE)
# URLs
_URLS = re.compile(r'https?://\S+')

def author_name_from_author(author: dict, alias_map: dict) -> str:
    # Grok stays "ASSISTANT"
    if str((author or {}).get("id")) == GROK_USER_ID:
        return "<ASSISTANT>"

    uname = (author or {}).get("userName")
    if not isinstance(uname, str):
        return "<USER>"  # fallback

    key = uname.lower()
    if key not in alias_map:
        alias_map[key] = f"<USER_{len(alias_map) + 1}>"
    return alias_map[key]

def clean_text_with_map(text: str, alias_map: dict) -> str:
    if not isinstance(text, str):
        return ""

    s = _LEADING_MENTIONS.sub("", text).lstrip()

    # Special-case @grok first → [ASSISTANT]
    s = re.sub(r'@grok\b', "<ASSISTANT>", s, flags=re.IGNORECASE)

    # Replace other mentions with stable <USER_n> tokens
    def _mention_sub(m):
        handle = m.group(0)[1:]
        key = handle.lower()
        if key == "grok":
            return "<ASSISTANT>"
        if key not in alias_map:
            alias_map[key] = f"<USER_{len(alias_map) + 1}>"
        return alias_map[key]

    s = re.sub(r'@(\w+)\b', _mention_sub, s)

    # Replace plain 'grok' (non-mention) with [ASSISTANT]
    s = _ANY_GROK_WORD.sub("<ASSISTANT>", s)

    # Replace links with [LINK]
    s = _URLS.sub("<LINK>", s)

    # Normalize whitespace
    return " ".join(s.split())

def clean_tweet_minimal(t: dict, alias_map: dict) -> dict:
    return {
        "text": clean_text_with_map(t.get("text", ""), alias_map),
        "authorName": author_name_from_author(t.get("author"), alias_map),
    }
def clean_conversations_minimal(out_obj: list) -> list:
    """
    Keeps {conversationId, threads[]} but trims each tweet to {text, authorName}.
    Mentions get stable per-conversation <USER_n> aliases.
    """
    cleaned = []
    for conv in out_obj or []:
        alias_map = {}  # <-- stable mapping for this conversation
        threads = []
        for th in (conv.get("threads") or []):
            new_th = {
                "threadId": th.get("threadId"),
                **({k: v for k, v in th.items() if k not in ("threadId", "tweets")}),
                "tweets": [clean_tweet_minimal(t, alias_map) for t in (th.get("tweets") or [])],
            }
            threads.append(new_th)
        cleaned.append({
            "conversationId": conv.get("conversationId"),
            "threads": threads
        })
    return cleaned
