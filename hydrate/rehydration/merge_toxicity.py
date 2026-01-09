#!/usr/bin/env python3
"""
merge_toxicity_message_level.py

Attach toxicity annotations at the TWEET (message) level:

tweet.annotations.toxicity = [ ... ]

Mapping rule:
- Compare dehydrated tweet["text"] to toxicity_row["grok_reply"]
- If match, attach toxicity payload (excluding conversationId/threadId/grok_reply)
- Multiple matches allowed -> list

Output key order:
- conversation: conversationId -> annotations -> threads
- thread: threadId -> conversation_id -> computed fields -> annotations -> tweets
- tweet: keeps existing fields; adds/overwrites tweet.annotations.toxicity
"""

import argparse
import json
import ijson
from typing import Any, Dict, List
from decimal import Decimal
import re


def json_default(o: Any):
    if isinstance(o, Decimal):
        return float(o)
    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")


_ws_re = re.compile(r"\s+")


def norm_text(s: str, do_normalize: bool) -> str:
    if s is None:
        return ""
    if not isinstance(s, str):
        s = str(s)
    if not do_normalize:
        return s
    # normalize: strip + collapse whitespace
    return _ws_re.sub(" ", s).strip()


def load_toxicity_by_text(path: str, do_normalize: bool) -> Dict[str, List[Dict[str, Any]]]:
    """
    Build mapping:
      normalized(grok_reply) -> [tox_payload, tox_payload, ...]
    where tox_payload excludes IDs + the grok_reply text.
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    idx: Dict[str, List[Dict[str, Any]]] = {}

    for row in data:
        gr = row.get("grok_reply")
        key = norm_text(gr, do_normalize)
        if not key:
            continue

        payload = dict(row)
        # remove join keys / redundant copies
        payload.pop("conversationId", None)
        payload.pop("threadId", None)
        payload.pop("grok_reply", None)

        idx.setdefault(key, []).append(payload)

    return idx


def dump_conversation_ordered(fout, conversation_id: str, annotations: Dict[str, Any], threads: Any):
    fout.write('{\n')
    fout.write('  "conversationId": ')
    fout.write(json.dumps(conversation_id, ensure_ascii=False, default=json_default))
    fout.write(',\n')

    fout.write('  "annotations": ')
    fout.write(json.dumps(annotations, ensure_ascii=False, indent=2, default=json_default))
    fout.write(',\n')

    fout.write('  "threads": ')
    fout.write(json.dumps(threads, ensure_ascii=False, indent=2, default=json_default))
    fout.write('\n}')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="dehydrated_base.json (big) OR current annotated file")
    ap.add_argument("--toxicity", required=True, help="toxicity JSON array (contains grok_reply)")
    ap.add_argument("--output", default="dehydrated_with_message_toxicity.json")
    ap.add_argument("--log-every", type=int, default=1000)
    ap.add_argument(
        "--no-normalize",
        action="store_true",
        help="disable whitespace normalization; require exact string equality"
    )
    args = ap.parse_args()

    do_normalize = not args.no_normalize
    tox_by_text = load_toxicity_by_text(args.toxicity, do_normalize)

    first = True
    conv_count = 0

    with open(args.input, "rb") as fin, open(args.output, "w", encoding="utf-8") as fout:
        fout.write("[\n")

        for conv in ijson.items(fin, "item"):
            cid = str(conv.get("conversationId") or "").strip()

            conv_ann = conv.get("annotations")
            if not isinstance(conv_ann, dict):
                conv_ann = {}

            out_threads = []
            for th in conv.get("threads", []) or []:
                th_ann = th.get("annotations")
                if not isinstance(th_ann, dict):
                    th_ann = {}

                out_tweets = []
                for tw in th.get("tweets", []) or []:
                    # assume dehydrated has text for now (per your note)
                    tw_text = norm_text(tw.get("text", ""), do_normalize)

                    tw_ann = tw.get("annotations")
                    if not isinstance(tw_ann, dict):
                        tw_ann = {}

                    tw_ann["toxicity"] = tox_by_text.get(tw_text, [])

                    # keep tweet fields, just ensure annotations exists
                    tw_out = dict(tw)
                    tw_out["annotations"] = tw_ann
                    out_tweets.append(tw_out)

                out_threads.append({
                    "threadId": th.get("threadId"),
                    "conversation_id": th.get("conversation_id"),
                    "hasMissingTweets": th.get("hasMissingTweets"),
                    "truncatedThread": th.get("truncatedThread"),
                    "validTweetCount": th.get("validTweetCount"),
                    "deletedTweetCount": th.get("deletedTweetCount"),
                    "annotations": th_ann,   # unchanged thread annotations
                    "tweets": out_tweets
                })

            if not first:
                fout.write(",\n")
            dump_conversation_ordered(fout, cid, conv_ann, out_threads)
            first = False

            conv_count += 1
            if args.log_every > 0 and conv_count % args.log_every == 0:
                print(f"[progress] wrote {conv_count} conversations...")

        fout.write("\n]\n")

    print(f"[done] wrote {conv_count} conversations to {args.output}")


if __name__ == "__main__":
    main()
