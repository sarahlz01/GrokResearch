#!/usr/bin/env python3
"""
run_annotate.py

Orchestrates:
1) annotations/dehydrate.py
2) annotations/merge_cleaned_text.py
3) annotations/merge_toxicity.py
4) strip text fields (text + cleaned_text)
5) annotations/merge_topic.py
6) annotations/merge_trolling.py
7) annotations/merge_discussion.py

Writes intermediates to: rehydration/annotations/merged/
Final output: rehydration/dehydrated.json
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List

import ijson


def run_cmd(cmd: List[str], cwd: Path):
    print("\n▶️", " ".join(map(str, cmd)))
    subprocess.run(list(map(str, cmd)), cwd=str(cwd), check=True)

from decimal import Decimal

def json_default(o):
    if isinstance(o, Decimal):
        return float(o)
    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")


def normalize_thread_conversation_id(in_path, out_path, log_every=10000):
    """
    For each thread:
      - Rename 'conversation_id' -> 'conversationId'
      - Ensure value equals the parent conversation's 'conversationId'
    Streaming + Decimal-safe.
    """
    conv_count = 0
    first = True

    with open(in_path, "rb") as fin, open(out_path, "w", encoding="utf-8") as fout:
        fout.write("[\n")

        for conv in ijson.items(fin, "item"):
            conv_count += 1
            cid = str(conv.get("conversationId") or "").strip()

            threads = conv.get("threads") or []
            for th in threads:
                if not isinstance(th, dict):
                    continue

                # Rename conversation_id -> conversationId (and normalize value)
                if "conversation_id" in th:
                    th.pop("conversation_id", None)
                th["conversationId"] = cid

            if not first:
                fout.write(",\n")
            json.dump(conv, fout, ensure_ascii=False, indent=2, default=json_default)
            first = False

            if log_every and conv_count % log_every == 0:
                print(f"[normalize_thread_conversation_id] processed {conv_count} conversations...")

        fout.write("\n]\n")

    print(f"[normalize_thread_conversation_id] wrote {out_path} ({conv_count} conversations)")



def strip_fields(in_path: Path, out_path: Path, fields: List[str], log_every: int = 10000):
    """
    Stream JSON array and remove selected fields from each tweet (if present).
    """
    conv_count = 0
    first = True

    with open(in_path, "rb") as fin, open(out_path, "w", encoding="utf-8") as fout:
        fout.write("[\n")

        for conv in ijson.items(fin, "item"):
            conv_count += 1

            threads = conv.get("threads") or []
            for th in threads:
                tweets = th.get("tweets") or []
                for tw in tweets:
                    if not isinstance(tw, dict):
                        continue
                    for f in fields:
                        tw.pop(f, None)

            if not first:
                fout.write(",\n")
            json.dump(conv, fout, ensure_ascii=False, indent=2, default=json_default)
            first = False

            if log_every and conv_count % log_every == 0:
                print(f"[strip_fields] processed {conv_count} conversations...")

        fout.write("\n]\n")

    print(f"[strip_fields] wrote {out_path} ({conv_count} conversations)")


def _pick_existing_toxicity_file(to_merge_dir: Path, desired: Path) -> Path:
    """
    If desired doesn't exist, try to find a reasonable toxicity file in to_merge/.
    """
    if desired.exists():
        return desired

    if not to_merge_dir.exists():
        raise FileNotFoundError(f"to_merge directory not found: {to_merge_dir}")

    # heuristic search
    candidates: List[Path] = []
    for pat in [
        "*toxicity*merged*.json",
        "*toxicity*.json",
        "*merged*tox*.json",
        "*.json",
    ]:
        candidates = sorted(to_merge_dir.glob(pat))
        if candidates:
            break

    if len(candidates) == 1:
        print(f"[warn] toxicity file not found at default path:\n  {desired}\n"
              f"[warn] using detected candidate:\n  {candidates[0]}")
        return candidates[0]

    # If multiple, prefer the one with 'toxicity' in name
    tox_named = [p for p in candidates if "toxicity" in p.name.lower()]
    if len(tox_named) == 1:
        print(f"[warn] toxicity file not found at default path:\n  {desired}\n"
              f"[warn] using detected candidate:\n  {tox_named[0]}")
        return tox_named[0]

    # Otherwise fail with a useful message
    listing = "\n".join(f"  - {p.name}" for p in sorted(to_merge_dir.glob("*")))
    raise FileNotFoundError(
        f"Toxicity file not found:\n  {desired}\n"
        f"Looked in:\n  {to_merge_dir}\n"
        f"Files present:\n{listing or '  (none)'}\n\n"
        f"Fix: put the file there OR pass --toxicity /absolute/or/relative/path.json"
    )


def main():
    base_dir = Path(__file__).resolve().parent          # .../hydrate/rehydration
    scripts_dir = base_dir / "annotations"             # .../rehydration/annotations
    to_merge_dir = scripts_dir / "to_merge"

    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--input",
        default="./output.json",
        help="Path to original big JSON (default: ./output.json relative to rehydration/)"
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

    tox_path_desired = resolve(args.toxicity)
    tox_path = _pick_existing_toxicity_file(to_merge_dir, tox_path_desired)

    topics_path = resolve(args.topics)
    troll_path = resolve(args.trolling)
    disc_path = resolve(args.discussion)

    if not input_path.exists():
        raise FileNotFoundError(f"Input not found: {input_path}")

    cleaned_path = resolve(args.cleaned) if args.cleaned else (input_path.parent / "output_CLEANED.json").resolve()
    if not cleaned_path.exists():
        raise FileNotFoundError(f"output_CLEANED.json not found: {cleaned_path}")

    out_dir.mkdir(parents=True, exist_ok=True)

    # Print resolved paths so you can sanity-check quickly
    print("[paths] base_dir   =", base_dir)
    print("[paths] scripts   =", scripts_dir)
    print("[paths] input      =", input_path)
    print("[paths] cleaned    =", cleaned_path)
    print("[paths] out_dir    =", out_dir)
    print("[paths] toxicity   =", tox_path)
    print("[paths] topics     =", topics_path)
    print("[paths] trolling   =", troll_path)
    print("[paths] discussion =", disc_path)

    # -------- Step outputs (intermediate checkpoints) --------
    p0 = out_dir / "dehydrated_base_with_text.json"
    p0c = out_dir / "dehydrated_base_with_text_cleaned.json"
    p1 = out_dir / "dehydrated_toxicity_with_text.json"
    p2 = out_dir / "dehydrated_toxicity.json"
    p3 = out_dir / "dehydrated_toxicity_topics.json"
    p4 = out_dir / "dehydrated_toxicity_topics_trolling.json"
    p5_tmp = out_dir / "dehydrated_final_tmp.json"
    p5 = base_dir / "dehydrated.json"

    py = sys.executable  # uses your venv python if activated

    # Scripts (in ./annotations/)
    dehydrate_py = scripts_dir / "dehydrate.py"
    merge_cleaned_py = scripts_dir / "merge_cleaned_text.py"
    merge_tox_py = scripts_dir / "merge_toxicity.py"
    merge_topic_py = scripts_dir / "merge_topic.py"
    merge_troll_py = scripts_dir / "merge_trolling.py"
    merge_disc_py = scripts_dir / "merge_discussion.py"

    for s in [dehydrate_py, merge_cleaned_py, merge_tox_py, merge_topic_py, merge_troll_py, merge_disc_py]:
        if not s.exists():
            raise FileNotFoundError(f"Missing script: {s}")

    # 1) Dehydrate (KEEP text; your dehydrate.py includes text)
    run_cmd([
        py, str(dehydrate_py),
        "--input", str(input_path),
        "--output", str(p0),
        "--log-every", str(args.log_every),
    ], cwd=base_dir)

    # 2) Merge cleaned_text from output_CLEANED.json
    run_cmd([
        py, str(merge_cleaned_py),
        "--input", str(p0),
        "--cleaned", str(cleaned_path),
        "--output", str(p0c),
        "--log-every", str(args.log_every),
    ], cwd=base_dir)

    # 3) Merge toxicity (has text + cleaned_text)
    run_cmd([
        py, str(merge_tox_py),
        "--input", str(p0c),
        "--toxicity", str(tox_path),
        "--output", str(p1),
        "--log-every", str(args.log_every),
    ], cwd=base_dir)

    # 4) Strip BOTH text fields AFTER toxicity merge
    strip_fields(p1, p2, fields=["text", "cleaned_text"], log_every=args.log_every)

    # 5) Merge topics
    run_cmd([
        py, str(merge_topic_py),
        "--input", str(p2),
        "--topics", str(topics_path),
        "--output", str(p3),
        "--log-every", str(args.log_every),
    ], cwd=base_dir)

    # 6) Merge trolling
    run_cmd([
        py, str(merge_troll_py),
        "--input", str(p3),
        "--trolling", str(troll_path),
        "--output", str(p4),
        "--log-every", str(args.log_every),
    ], cwd=base_dir)

    # 7) Merge discussion -> tmp
    run_cmd([
        py, str(merge_disc_py),
        "--input", str(p4),
        "--discussion", str(disc_path),
        "--output", str(p5_tmp),
        "--log-every", str(args.log_every),
    ], cwd=base_dir)
    
    # 8) Normalize thread conversation id field name
    normalize_thread_conversation_id(p5_tmp, p5, log_every=args.log_every)


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
