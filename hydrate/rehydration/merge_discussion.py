#!/usr/bin/env python3
"""
merge_discussion.py (MULTI)

conversation.annotations.discussion becomes an ARRAY:
  annotations: { discussion: [ {...}, {...} ] }

If no mapping: discussion = []
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


def load_discussion_index(path: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Build:
      conversationId -> [discussion_payload, ...]
    Each payload excludes conversationId.
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    idx: Dict[str, List[Dict[str, Any]]] = {}
    for row in data:
        cid = str(row.get("conversationId") or "").strip()
        if not cid:
            continue

        payload = dict(row)
        payload.pop("conversationId", None)
        idx.setdefault(cid, []).append(payload)

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
    ap.add_argument("--input", required=True)
    ap.add_argument("--discussion", required=True)
    ap.add_argument("--output", default="dehydrated_with_discussion.json")
    ap.add_argument("--log-every", type=int, default=1000)
    args = ap.parse_args()

    discussion_by_conv = load_discussion_index(args.discussion)

    first = True
    conv_count = 0

    with open(args.input, "rb") as fin, open(args.output, "w", encoding="utf-8") as fout:
        fout.write("[\n")

        for conv in ijson.items(fin, "item"):
            cid = str(conv.get("conversationId") or "").strip()

            ann = conv.get("annotations")
            if not isinstance(ann, dict):
                ann = {}

            # always an array now
            ann["discussion"] = discussion_by_conv.get(cid, [])

            threads = conv.get("threads") or []

            if not first:
                fout.write(",\n")
            dump_conversation_ordered(fout, cid, ann, threads)
            first = False

            conv_count += 1
            if args.log_every > 0 and conv_count % args.log_every == 0:
                print(f"[progress] merged {conv_count} conversations...")

        fout.write("\n]\n")

    print(f"[done] wrote {conv_count} conversations to {args.output}")


if __name__ == "__main__":
    main()
