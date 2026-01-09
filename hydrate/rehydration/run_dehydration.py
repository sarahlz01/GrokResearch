#!/usr/bin/env python3
import argparse
import json
import subprocess
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any, List

import ijson


def run_cmd(cmd, cwd: Path):
    print("\n▶️", " ".join(map(str, cmd)))
    subprocess.run(list(map(str, cmd)), cwd=str(cwd), check=True)


def json_default(o: Any):
    if isinstance(o, Decimal):
        return float(o)
    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")


def strip_fields(in_path: Path, out_path: Path, fields: List[str], log_every: int = 10000):
    """
    Remove tweet[field] for each field in `fields` from every tweet object (streaming).
    Keeps everything else unchanged.
    """
    conv_count = 0
    first = True

    with open(in_path, "rb") as fin, open(out_path, "w", encoding="utf-8") as fout:
        fout.write("[\n")

        for conv in ijson.items(fin, "item"):
            threads = conv.get("threads") or []
            for th in threads:
                tweets = th.get("tweets") or []
                for tw in tweets:
                    if isinstance(tw, dict):
                        for f in fields:
                            tw.pop(f, None)

            if not first:
                fout.write(",\n")
            json.dump(conv, fout, ensure_ascii=False, indent=2, default=json_default)
            first = False

            conv_count += 1
            if log_every and conv_count % log_every == 0:
                print(f"[strip_fields] processed {conv_count} conversations...")

        fout.write("\n]\n")

    print(f"[strip_fields] wrote {out_path} ({conv_count} conversations)")


def main():
    base_dir = Path(__file__).resolve().parent  # .../hydrate/rehydration

    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--input",
        default="../output.json",
        help="Path to original big JSON (default: ../output.json relative to rehydration/)"
    )
    ap.add_argument(
        "--cleaned",
        default=None,
        help="Path to output_CLEANED.json (default: alongside --input as output_CLEANED.json)"
    )
    ap.add_argument(
        "--out-dir",
        default="./annotations/merged",
        help="Where to write intermediate dehydrated_*.json files (default: ./annotations/merged)"
    )

    # Annotation inputs (inside rehydration/annotations/to_merge/)
    ap.add_argument("--toxicity", default="./annotations/to_merge/toxicity_merged_results.json")
    ap.add_argument("--topics", default="./annotations/to_merge/output_CLEANED_with_topics.json")
    ap.add_argument("--trolling", default="./annotations/to_merge/trolling_merged_results.json")
    ap.add_argument("--discussion", default="./annotations/to_merge/discussions_merged_results.json")

    ap.add_argument("--log-every", type=int, default=10000)
    args = ap.parse_args()

    def resolve(p: str) -> Path:
        pp = Path(p)
        if pp.is_absolute():
            return pp
        return (base_dir / pp).resolve()

    input_path = resolve(args.input)
    out_dir = resolve(args.out_dir)
    tox_path = resolve(args.toxicity)
    topics_path = resolve(args.topics)
    troll_path = resolve(args.trolling)
    disc_path = resolve(args.discussion)

    if not input_path.exists():
        raise FileNotFoundError(f"Input not found: {input_path}")

    # Default cleaned path = sibling of output.json
    cleaned_path = resolve(args.cleaned) if args.cleaned else (input_path.parent / "output_CLEANED.json").resolve()
    if not cleaned_path.exists():
        raise FileNotFoundError(f"output_CLEANED.json not found: {cleaned_path}")

    out_dir.mkdir(parents=True, exist_ok=True)

    # Print resolved paths so you can sanity-check quickly
    print("[paths] base_dir   =", base_dir)
    print("[paths] input      =", input_path)
    print("[paths] cleaned    =", cleaned_path)
    print("[paths] out_dir    =", out_dir)
    print("[paths] toxicity   =", tox_path)
    print("[paths] topics     =", topics_path)
    print("[paths] trolling   =", troll_path)
    print("[paths] discussion =", disc_path)

    # -------- Step outputs (intermediate checkpoints) --------
    p0 = out_dir / "dehydrated_base_with_text.json"
    p0c = out_dir / "dehydrated_base_with_text_cleaned.json"  # <-- NEW (adds cleaned_text)
    p1 = out_dir / "dehydrated_toxicity_with_text.json"
    p2 = out_dir / "dehydrated_toxicity.json"  # <-- strip BOTH text + cleaned_text here
    p3 = out_dir / "dehydrated_toxicity_topics.json"
    p4 = out_dir / "dehydrated_toxicity_topics_trolling.json"
    p5 = out_dir / "dehydrated_toxicity_topics_trolling_discussion.json"

    py = sys.executable  # uses your venv python if activated

    # 1) Dehydrate (KEEP text; your dehydrate.py includes text)
    run_cmd([
        py, "dehydrate.py",
        "--input", str(input_path),
        "--output", str(p0),
        "--log-every", str(args.log_every),
    ], cwd=base_dir)

    # 2) Merge cleaned_text from output_CLEANED.json
    # Requires merge_cleaned_text.py to exist in rehydration/
    run_cmd([
        py, "merge_cleaned_text.py",
        "--input", str(p0),
        "--cleaned", str(cleaned_path),
        "--output", str(p0c),
        "--log-every", str(args.log_every),
    ], cwd=base_dir)

    # 3) Merge toxicity (has text + cleaned_text)
    run_cmd([
        py, "merge_toxicity.py",
        "--input", str(p0c),
        "--toxicity", str(tox_path),
        "--output", str(p1),
        "--log-every", str(args.log_every),
    ], cwd=base_dir)

    # 4) Strip BOTH text fields AFTER toxicity merge
    strip_fields(p1, p2, fields=["text", "cleaned_text"], log_every=args.log_every)

    # 5) Merge topics
    run_cmd([
        py, "merge_topic.py",
        "--input", str(p2),
        "--topics", str(topics_path),
        "--output", str(p3),
        "--log-every", str(args.log_every),
    ], cwd=base_dir)

    # 6) Merge trolling
    run_cmd([
        py, "merge_trolling.py",
        "--input", str(p3),
        "--trolling", str(troll_path),
        "--output", str(p4),
        "--log-every", str(args.log_every),
    ], cwd=base_dir)

    # 7) Merge discussion
    run_cmd([
        py, "merge_discussion.py",
        "--input", str(p4),
        "--discussion", str(disc_path),
        "--output", str(p5),
        "--log-every", str(args.log_every),
    ], cwd=base_dir)

    print("\n✅ Done.")
    print("Final:", p5)
    print("Intermediates:")
    print(" -", p0)
    print(" -", p0c)
    print(" -", p1)
    print(" -", p2)
    print(" -", p3)
    print(" -", p4)


if __name__ == "__main__":
    main()
