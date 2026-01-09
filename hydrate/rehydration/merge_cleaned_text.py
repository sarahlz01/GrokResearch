#!/usr/bin/env python3
"""
merge_cleaned_text.py

Goal:
- Read dehydrated_base_with_text.json (large) with ijson
- Read output_CLEANED.json (cleaned minimal) and map cleaned tweet texts into dehydrated tweets
- Add tweet["cleaned_text"] = <cleaned tweet text>

Mapping rule:
- Match by threadId, and tweet order within that thread (index-based).
- Assumes order matches (as you said).

Output:
- Pretty-printed JSON array.
"""

import argparse
import json
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List

import ijson


def json_default(o: Any):
    if isinstance(o, Decimal):
        return float(o)
    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")


def load_cleaned_index(cleaned_path: Path, log_every: int = 10000) -> Dict[str, List[str]]:
    """
    Build: threadId -> [cleaned tweet text in order]
    This is in-memory. output_CLEANED.json is usually much smaller than output.json.
    """
    idx: Dict[str, List[str]] = {}
    conv_count = 0

    with open(cleaned_path, "rb") as f:
        for conv in ijson.items(f, "item"):
            conv_count += 1
            for th in (conv.get("threads") or []):
                tid = str(th.get("threadId") or "").strip()
                if not tid:
                    continue
                tweets = th.get("tweets") or []
                idx[tid] = [str(t.get("text") or "") for t in tweets]

            if log_every and conv_count % log_every == 0:
                print(f"[cleaned_index] loaded {conv_count} conversations...")

    print(f"[cleaned_index] indexed threads: {len(idx)}")
    return idx


def merge(dehydrated_in: Path, cleaned_path: Path, out_path: Path, log_every: int = 10000):
    cleaned_idx = load_cleaned_index(cleaned_path, log_every=log_every)

    conv_count = 0
    first = True
    missing_threads = 0
    missing_tweet_slots = 0

    with open(dehydrated_in, "rb") as fin, open(out_path, "w", encoding="utf-8") as fout:
        fout.write("[\n")

        for conv in ijson.items(fin, "item"):
            for th in (conv.get("threads") or []):
                tid = str(th.get("threadId") or "").strip()
                cleaned_texts = cleaned_idx.get(tid)

                if cleaned_texts is None:
                    missing_threads += 1
                    # Still add cleaned_text="", so schema is consistent
                    for tw in (th.get("tweets") or []):
                        if isinstance(tw, dict):
                            tw.setdefault("cleaned_text", "")
                    continue

                tweets = th.get("tweets") or []
                for i, tw in enumerate(tweets):
                    if not isinstance(tw, dict):
                        continue
                    if i < len(cleaned_texts):
                        tw["cleaned_text"] = cleaned_texts[i]
                    else:
                        missing_tweet_slots += 1
                        tw["cleaned_text"] = ""

            if not first:
                fout.write(",\n")
            json.dump(conv, fout, ensure_ascii=False, indent=2, default=json_default)
            first = False

            conv_count += 1
            if log_every and conv_count % log_every == 0:
                print(f"[merge_cleaned_text] processed {conv_count} conversations...")

        fout.write("\n]\n")

    print(f"[merge_cleaned_text] wrote {out_path} ({conv_count} conversations)")
    print(f"[merge_cleaned_text] missing threads (no cleaned mapping): {missing_threads}")
    print(f"[merge_cleaned_text] missing tweet slots (index overflow): {missing_tweet_slots}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="dehydrated_base_with_text.json")
    ap.add_argument("--cleaned", required=True, help="output_CLEANED.json")
    ap.add_argument("--output", required=True, help="dehydrated_base_with_text_cleaned.json")
    ap.add_argument("--log-every", type=int, default=10000)
    args = ap.parse_args()

    merge(Path(args.input), Path(args.cleaned), Path(args.output), log_every=args.log_every)


if __name__ == "__main__":
    main()
