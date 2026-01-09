#!/usr/bin/env python3
"""
merge_topic.py (MULTI)

conversation.annotations.topic becomes an ARRAY OF TOPIC LISTS:
  annotations: { topic: [ ["479: ...", ...], ["123: ...", ...] ] }

If no mapping: topic = [ [] ]  (i.e., one empty run)
If you prefer topic = [] instead, change the default below.
"""

import argparse
import json
import ijson
from typing import Any, Dict, List
from decimal import Decimal


def json_default(o: Any):
    if isinstance(o, Decimal):
        return float(o)
    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")


def load_topics_index(path: str) -> Dict[str, List[Any]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    idx: Dict[str, List[Any]] = {}
    for row in data:
        cid = str(row.get("conversationId") or "").strip()
        if not cid:
            continue
        topics = row.get("topics")
        if not isinstance(topics, list):
            topics = []
        idx[cid] = topics
    return idx


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--topics", required=True)
    ap.add_argument("--output", default="dehydrated_toxicity_topics.json")
    ap.add_argument("--log-every", type=int, default=1000)
    args = ap.parse_args()

    topics_by_conv = load_topics_index(args.topics)

    first = True
    conv_count = 0

    with open(args.input, "rb") as fin, open(args.output, "w", encoding="utf-8") as fout:
        fout.write("[\n")

        for conv in ijson.items(fin, "item"):
            cid = str(conv.get("conversationId") or "").strip()

            ann = conv.get("annotations")
            if not isinstance(ann, dict):
                ann = {}

            # ensure topic is an array-of-runs
            existing = ann.get("topic")
            if existing is None:
                ann["topic"] = []
            elif isinstance(existing, list):
                # if it looks like the old schema (list of strings), wrap it once
                if len(existing) > 0 and not isinstance(existing[0], list):
                    ann["topic"] = [existing]
            else:
                ann["topic"] = []

            ann["topic"].append(topics_by_conv.get(cid, []))

            ordered_conv: Dict[str, Any] = {}
            ordered_conv["conversationId"] = cid
            ordered_conv["annotations"] = ann
            ordered_conv["threads"] = conv.get("threads") or []

            if not first:
                fout.write(",\n")
            json.dump(ordered_conv, fout, ensure_ascii=False, indent=2, default=json_default)
            first = False

            conv_count += 1
            if args.log_every > 0 and conv_count % args.log_every == 0:
                print(f"[progress] merged {conv_count} conversations...")

        fout.write("\n]\n")

    print(f"[done] wrote {conv_count} conversations to {args.output}")


if __name__ == "__main__":
    main()
