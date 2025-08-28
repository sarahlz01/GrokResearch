import json
from collections import defaultdict
import os

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
    return threads

def translate(raw_path, out_path):
    os.makedirs(os.path.dirname(raw_path) or ".", exist_ok=True)
    with open(raw_path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    out = build_threads_for_raw(raw)
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)