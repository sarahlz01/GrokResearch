#!/usr/bin/env python3
import argparse
import os
import subprocess
import sys
import ijson
import json
from typing import Any, Dict


def run_cmd(cmd, cwd=None):
    print("\n▶️", " ".join(cmd))
    subprocess.run(cmd, cwd=cwd, check=True)


def strip_text_field(in_path: str, out_path: str, log_every: int = 10000):
    """
    Remove tweet["text"] from every tweet object (streaming).
    Keeps everything else unchanged.
    """
    conv_count = 0
    first = True

    with open(in_path, "rb") as fin, open(out_path, "w", encoding="utf-8") as fout:
        fout.write("[\n")
        for conv in ijson.items(fin, "item"):
            # conv: {conversationId, annotations, threads}
            threads = conv.get("threads") or []
            for th in threads:
                tweets = th.get("tweets") or []
                for tw in tweets:
                    if isinstance(tw, dict):
                        tw.pop("text", None)

            if not first:
                fout.write(",\n")
            json.dump(conv, fout, ensure_ascii=False, indent=2)
            first = False

            conv_count += 1
            if log_every and conv_count % log_every == 0:
                print(f"[strip_text] processed {conv_count} conversations...")

        fout.write("\n]\n")
    print(f"[strip_text] wrote {out_path} ({conv_count} conversations)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--input",
        default=os.path.join("..", "hydration", "output.json"),
        help="Path to original big hydrated JSON (default: ../hydration/output.json)"
    )
    ap.add_argument(
        "--out-dir",
        default=".",
        help="Where to write intermediate dehydrated_*.json files (default: current dir)"
    )

    # Annotation inputs (defaults assume your tree: ./annotations/to_merge/...)
    ap.add_argument("--toxicity", default=os.path.join("annotations", "to_merge", "toxicity_merged_results.json"))
    ap.add_argument("--topics", default=os.path.join("annotations", "to_merge", "output_CLEANED_with_topics.json"))
    ap.add_argument("--trolling", default=os.path.join("annotations", "to_merge", "trolling_merged_results.json"))
    ap.add_argument("--discussion", default=os.path.join("annotations", "to_merge", "discussions_merged_results.json"))

    ap.add_argument("--log-every", type=int, default=80000)
    args = ap.parse_args()

    out_dir = args.out_dir
    os.makedirs(out_dir, exist_ok=True)

    # -------- Step outputs (intermediate checkpoints) --------
    p0 = os.path.join(out_dir, "dehydrated_base_with_text.json")
    p1 = os.path.join(out_dir, "dehydrated_toxicity_with_text.json")
    p2 = os.path.join(out_dir, "dehydrated_toxicity.json")  # text removed here
    p3 = os.path.join(out_dir, "dehydrated_toxicity_topics.json")
    p4 = os.path.join(out_dir, "dehydrated_toxicity_topics_trolling.json")
    p5 = os.path.join(out_dir, "dehydrated_toxicity_topics_trolling_discussion.json")

    py = sys.executable

    # 1) Dehydrate (keep text for now)
    run_cmd([
        py, "dehydrate.py",
        "--input", args.input,
        "--output", p0,
        "--log-every", str(args.log_every),
    ])

    # 2) Merge toxicity (still has text)
    run_cmd([
        py, "merge_toxicity.py",
        "--dehydrated", p0,
        "--toxicity", args.toxicity,
        "--output", p1,
        "--log-every", str(args.log_every),
    ])

    # 3) Strip text AFTER toxicity merge
    strip_text_field(p1, p2, log_every=args.log_every)

    # 4) Merge topics
    run_cmd([
        py, "merge_topic.py",
        "--input", p2,
        "--topics", args.topics,
        "--output", p3,
        "--log-every", str(args.log_every),
    ])

    # 5) Merge trolling
    run_cmd([
        py, "merge_trolling.py",
        "--input", p3,
        "--trolling", args.trolling,
        "--output", p4,
        "--log-every", str(args.log_every),
    ])

    # 6) Merge discussion
    run_cmd([
        py, "merge_discussion.py",
        "--input", p4,
        "--discussion", args.discussion,
        "--output", p5,
        "--log-every", str(args.log_every),
    ])

    print("\n✅ Done.")
    print("Final:", p5)
    print("Intermediates:")
    print(" -", p0)
    print(" -", p1)
    print(" -", p2)
    print(" -", p3)
    print(" -", p4)


if __name__ == "__main__":
    main()
