#!/usr/bin/env python3
"""
merge_toxicity.py

Goal:
- Merge toxicity annotations into a dehydrated JSON (streaming with ijson).

Behavior:
1) Tweet-level (preferred):
   - Every tweet gets: tweet["annotations"]["toxicity"] = []
   - We ONLY try to match/annotate Grok tweets:
       (tweet.get("author", {}).get("isAssistant") == True)
   - If a match is found, we append the FULL toxicity row dict(s) (including grok_reply text)
     into tweet["annotations"]["toxicity"] (array).

2) Thread-level fallback:
   - Every thread gets: thread["annotations"] = {"toxicity": []}  (always present)
   - For toxicity rows belonging to that thread that are NOT matched to any Grok tweet,
     we append them to thread["annotations"]["toxicity"].

3) Matching:
   For a Grok tweet, we try to match toxicity rows by checking keys derived from:
   - tweet.text (raw whitespace-normalized)
   - tweet.cleaned_text (raw whitespace-normalized)  [if present from your merge_cleaned_text step]
   - clean_text_with_map(tweet.text)                (your local cleaner)
   - clean_text_with_map(tweet.cleaned_text)

   A toxicity row provides:
   - grok_reply
   We index toxicity rows by both:
   - normalize_ws(grok_reply)
   - clean_text_with_map(grok_reply)

4) Ordering:
   - For EVERY tweet, we rewrite the tweet dict so "annotations" is the FIRST field,
     even when toxicity is empty or tweet is non-assistant.

CLI:
  python3 merge_toxicity.py --input <dehydrated.json> --toxicity <tox.json> --output <out.json> --log-every 40000
"""

import argparse
import json
import logging
import re
import sys
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Tuple

import ijson


# ---------------- JSON helpers ----------------

def json_default(o: Any):
    if isinstance(o, Decimal):
        return float(o)
    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")


def normalize_ws(s: str) -> str:
    return " ".join((s or "").split())


def put_annotations_first(tw: dict, ann: dict) -> dict:
    """Return a new dict with 'annotations' inserted first, preserving other key order."""
    new_tw = {"annotations": ann}
    for k, v in tw.items():
        if k == "annotations":
            continue
        new_tw[k] = v
    return new_tw


# ---------------- Local cleaning logic (NO external imports) ----------------
# Keep this minimal + stable; it mirrors the typical behavior you described:
# - Replace URLs with <LINK>
# - Replace @grok / grok references with <ASSISTANT> (best-effort)
# - Strip leading mention block
# - Collapse whitespace

_LEADING_MENTIONS_BLOCK = re.compile(r'^(?:\s*[@＠][A-Za-z0-9_]{1,15}[^\S\r\n]*)+')
_URLS = re.compile(r'https?://\S+')
_AT_GROK = re.compile(r'[@＠]grok\b', re.IGNORECASE)
_ANY_GROK = re.compile(r'\bgrok\b', re.IGNORECASE)

def clean_text_with_map(text: str, alias_map: dict) -> str:
    if not isinstance(text, str):
        return ""
    s = text

    m = _LEADING_MENTIONS_BLOCK.match(s)
    if m:
        s = s[m.end():].lstrip()

    s = _URLS.sub("<LINK>", s)
    s = _AT_GROK.sub("<ASSISTANT>", s)
    s = _ANY_GROK.sub("<ASSISTANT>", s)

    return normalize_ws(s)


# ---------------- Toxicity loading + indexing ----------------

def load_toxicity_rows(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)]
    if isinstance(data, dict) and isinstance(data.get("items"), list):
        return [r for r in data["items"] if isinstance(r, dict)]
    return []


def build_by_conv_thread(rows: List[Dict[str, Any]]) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
    """
    Key by (conversationId, threadId) since toxicity rows include both.
    """
    out: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        cid = str(r.get("conversationId") or "").strip()
        tid = str(r.get("threadId") or "").strip()
        if cid and tid:
            out[(cid, tid)].append(r)
    return out


def build_indexes(rows_for_thread: List[Dict[str, Any]]) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, List[Dict[str, Any]]]]:
    """
    raw_idx[normalize_ws(grok_reply)] -> [row, ...]
    clean_idx[clean_text_with_map(grok_reply)] -> [row, ...]
    """
    raw_idx: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    clean_idx: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    alias_map: dict = {}

    for r in rows_for_thread:
        gr = r.get("grok_reply") or ""
        rk = normalize_ws(gr)
        if rk:
            raw_idx[rk].append(r)

        ck = clean_text_with_map(gr, alias_map)
        if ck:
            clean_idx[ck].append(r)

    return raw_idx, clean_idx


# ---------------- Merge ----------------

def merge_stream(input_path: str, toxicity_path: str, output_path: str, log_every: int):
    tox_rows = load_toxicity_rows(toxicity_path)
    tox_map = build_by_conv_thread(tox_rows)

    logging.info("[toxicity] loaded %d rows total", len(tox_rows))
    logging.info("[toxicity] conv-thread keys: %d", len(tox_map))

    conv_count = 0
    grok_scanned = 0
    grok_matched = 0
    tweet_payloads_attached = 0
    thread_fallback_attached = 0

    first = True
    with open(input_path, "rb") as fin, open(output_path, "w", encoding="utf-8") as fout:
        fout.write("[\n")

        for conv in ijson.items(fin, "item"):
            conv_count += 1
            cid = str(conv.get("conversationId") or "").strip()

            threads = conv.get("threads") or []
            for th_i, th in enumerate(threads):
                tid = str(th.get("threadId") or "").strip()

                # ✅ Ensure thread default annotations: {"toxicity": []}
                th_ann = th.get("annotations")
                if not isinstance(th_ann, dict):
                    th_ann = {}
                tox_list = th_ann.get("toxicity")
                if not isinstance(tox_list, list):
                    th_ann["toxicity"] = []
                th["annotations"] = th_ann

                # toxicity rows for THIS thread
                rows_for_thread = tox_map.get((cid, tid), [])
                raw_idx, clean_idx = build_indexes(rows_for_thread)

                # track which rows were used at tweet-level
                used_row_ids = set()

                tweets = th.get("tweets") or []
                for i, tw in enumerate(tweets):
                    if not isinstance(tw, dict):
                        continue

                    ann = tw.get("annotations")
                    if not isinstance(ann, dict):
                        ann = {}
                    # always present, always list
                    ann["toxicity"] = []

                    is_assistant = (tw.get("author") or {}).get("isAssistant", False)

                    if is_assistant and rows_for_thread:
                        grok_scanned += 1

                        alias_map: dict = {}

                        t_text = tw.get("text") or ""
                        ct_text = tw.get("cleaned_text") or ""

                        # build candidate keys from BOTH fields
                        keys_raw = {
                            normalize_ws(t_text),
                            normalize_ws(ct_text),
                        }
                        keys_clean = {
                            clean_text_with_map(t_text, alias_map),
                            clean_text_with_map(ct_text, alias_map),
                        }

                        matches: List[Dict[str, Any]] = []

                        for k in keys_raw:
                            if k:
                                matches.extend(raw_idx.get(k, []))

                        for k in keys_clean:
                            if k:
                                for r in clean_idx.get(k, []):
                                    if r not in matches:
                                        matches.append(r)

                        if matches:
                            ann["toxicity"].extend(matches)
                            grok_matched += 1
                            tweet_payloads_attached += len(matches)
                            for r in matches:
                                used_row_ids.add(id(r))

                    # ✅ ALWAYS rewrite tweet with annotations first
                    tweets[i] = put_annotations_first(tw, ann)

                # ---- Thread-level fallback: append any unused rows_for_thread ----
                if rows_for_thread:
                    for r in rows_for_thread:
                        if id(r) not in used_row_ids:
                            th_ann["toxicity"].append(r)
                            thread_fallback_attached += 1
                threads[th_i] = put_thread_annotations_after_deleted(th, th_ann)

            if not first:
                fout.write(",\n")
            json.dump(conv, fout, ensure_ascii=False, indent=2, default=json_default)
            first = False

            if log_every and conv_count % log_every == 0:
                logging.info("[progress] merged %d conversations...", conv_count)

        fout.write("\n]\n")

    logging.info("[done] wrote %d conversations -> %s", conv_count, output_path)
    logging.info("[toxicity] grok tweets scanned: %d", grok_scanned)
    logging.info("[toxicity] grok tweets matched (>=1 row): %d", grok_matched)
    logging.info("[toxicity] tweet-level payloads attached: %d", tweet_payloads_attached)
    logging.info("[toxicity] thread-fallback payloads attached: %d", thread_fallback_attached)
    logging.info("[toxicity] total payloads attached: %d", tweet_payloads_attached + thread_fallback_attached)

def put_thread_annotations_after_deleted(th: dict, th_ann: dict) -> dict:
    """
    Return a new thread dict where 'annotations' is placed immediately
    after 'deletedTweetCount' (if present). If 'deletedTweetCount' is missing,
    put 'annotations' right after 'validTweetCount' if present, else at the start.
    Preserves the rest of the original key order.
    """
    new_th = {}
    inserted = False

    for k, v in th.items():
        # skip existing annotations (we will re-insert)
        if k == "annotations":
            continue

        new_th[k] = v

        if k == "deletedTweetCount":
            new_th["annotations"] = th_ann
            inserted = True

    if not inserted:
        # fallback: try after validTweetCount
        if "validTweetCount" in new_th and "annotations" not in new_th:
            # rebuild to insert after validTweetCount
            rebuilt = {}
            for k, v in new_th.items():
                rebuilt[k] = v
                if k == "validTweetCount":
                    rebuilt["annotations"] = th_ann
            new_th = rebuilt
            inserted = True

    if "annotations" not in new_th:
        # final fallback: put first
        newer = {"annotations": th_ann}
        newer.update(new_th)
        new_th = newer

    return new_th


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="dehydrated_base_with_text(_cleaned).json")
    ap.add_argument("--toxicity", required=True, help="toxicity_merged_results.json")
    ap.add_argument("--output", required=True, help="output file")
    ap.add_argument("--log-every", type=int, default=10000)
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    merge_stream(args.input, args.toxicity, args.output, args.log_every)


if __name__ == "__main__":
    main()
