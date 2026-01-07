#!/usr/bin/env python3
"""
lmm.py — LMM with conversation-level aggregation (prevents OOM / "killed").

Instead of one row per tweet, we aggregate within each conversation:
  - mean log1p(metric) for Grok replies
  - mean log1p(metric) for Human replies

Then fit:
  mean_log_value ~ is_brand + (1 | conversation_id)

This still answers the within-conversation question while drastically reducing rows.
"""

import argparse
import csv
import os
from dataclasses import dataclass
from typing import Dict, Iterator, Optional, Tuple

import numpy as np
import pandas as pd
import statsmodels.formula.api as smf

GROK_USER_ID = "1720665183188922368"
GROK_AUTHORNAME_FALLBACK = "<ASSISTANT>"

METRICS = ["bookmarks"]


def _to_int(x) -> int:
    try:
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return 0


def is_grok_tweet(t: dict) -> bool:
    author = t.get("author") or {}
    if str(author.get("id") or "") == GROK_USER_ID:
        return True
    if t.get("authorName") == GROK_AUTHORNAME_FALLBACK:
        return True
    return False


def extract_metrics(t: dict) -> Dict[str, int]:
    m = t.get("metrics") or t.get("public_metrics") or {}

    likes = t.get("likeCount") or m.get("like_count") or 0
    replies = t.get("replyCount") or m.get("reply_count") or t.get("commentCount") or 0
    retweets = t.get("retweetCount") or m.get("retweet_count") or 0
    quotes = t.get("quoteCount") or m.get("quote_count") or 0
    bookmarks = t.get("bookmarkCount") or m.get("bookmark_count") or 0

    reposts = _to_int(retweets) + _to_int(quotes)

    return {
        "likes": _to_int(likes),
        "replies": _to_int(replies),
        "reposts": _to_int(reposts),
        "quotes": _to_int(quotes),
        "bookmarks": _to_int(bookmarks),
    }


def is_reply_like(t: dict) -> bool:
    if "isReply" in t:
        try:
            return bool(t.get("isReply"))
        except Exception:
            pass
    in_reply_to = t.get("inReplyToId")
    if isinstance(in_reply_to, str) and in_reply_to.strip() != "":
        return True
    return False


def iter_conversations(path: str) -> Iterator[dict]:
    import ijson
    with open(path, "r", encoding="utf-8") as f:
        for conv in ijson.items(f, "item"):
            yield conv


def iter_reply_tweets(conv: dict) -> Iterator[Tuple[str, dict]]:
    conv_id = str(conv.get("conversationId") or "")
    threads = conv.get("threads") or []
    for th in threads:
        tweets = th.get("tweets") or []
        if len(tweets) <= 1:
            continue

        for t in tweets[1:]:  # drop root
            if ("isReply" in t) or ("inReplyToId" in t):
                if not is_reply_like(t):
                    continue
            yield conv_id, t


@dataclass
class SharedFlags:
    has_grok: bool = False
    has_human: bool = False


def first_pass_shared_flags(input_path: str, min_per_conv: int) -> Dict[str, SharedFlags]:
    flags: Dict[str, SharedFlags] = {}
    counts: Dict[str, int] = {}

    for conv in iter_conversations(input_path):
        for conv_id, t in iter_reply_tweets(conv):
            if not conv_id:
                continue
            if conv_id not in flags:
                flags[conv_id] = SharedFlags()

            if is_grok_tweet(t):
                flags[conv_id].has_grok = True
            else:
                flags[conv_id].has_human = True

            counts[conv_id] = counts.get(conv_id, 0) + 1

    keep: Dict[str, SharedFlags] = {}
    for cid, fl in flags.items():
        if fl.has_grok and fl.has_human and counts.get(cid, 0) >= min_per_conv:
            keep[cid] = fl
    return keep


def build_aggregated_csv(
    input_path: str,
    shared: Dict[str, SharedFlags],
    metric: str,
    out_csv: str,
    max_rows: Optional[int] = None,
) -> int:
    """
    Build a small CSV with conversation-level means:
      conversation_id,is_brand,mean_log_value,n

    We compute mean over tweets within each conversation for each group (brand/human).
    """
    # (conversation_id, is_brand) -> (sum_log, count)
    sums: Dict[Tuple[str, int], float] = {}
    counts: Dict[Tuple[str, int], int] = {}

    seen_rows = 0

    for conv in iter_conversations(input_path):
        for conv_id, t in iter_reply_tweets(conv):
            if conv_id not in shared:
                continue

            em = extract_metrics(t)
            v = em.get(metric, 0)
            lv = float(np.log1p(v))

            key = (conv_id, 1 if is_grok_tweet(t) else 0)
            sums[key] = sums.get(key, 0.0) + lv
            counts[key] = counts.get(key, 0) + 1

            seen_rows += 1
            if max_rows is not None and seen_rows >= max_rows:
                break
        if max_rows is not None and seen_rows >= max_rows:
            break

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["conversation_id", "is_brand", "mean_log_value", "n"])
        n_out = 0
        for (conv_id, is_brand), s in sums.items():
            c = counts[(conv_id, is_brand)]
            w.writerow([conv_id, is_brand, s / c, c])
            n_out += 1

    return n_out


def fit_from_aggregated_csv(csv_path: str, alpha: float, print_full_summary: bool):
    df = pd.read_csv(csv_path)
    if df.empty:
        raise RuntimeError(f"{csv_path}: empty")

    model = smf.mixedlm("mean_log_value ~ is_brand", df, groups=df["conversation_id"])
    result = model.fit(reml=True, method="lbfgs")

    coef = float(result.params.get("is_brand", np.nan))
    se = float(result.bse.get("is_brand", np.nan))
    p = float(result.pvalues.get("is_brand", np.nan))
    ci_lo, ci_hi = result.conf_int().loc["is_brand"].tolist()

    # Effect on log scale -> multiplicative change on (metric+1) scale
    pct = (float(np.exp(coef)) - 1.0) * 100.0
    pct_lo = (float(np.exp(ci_lo)) - 1.0) * 100.0
    pct_hi = (float(np.exp(ci_hi)) - 1.0) * 100.0

    sig = bool(p < alpha)

    if print_full_summary:
        print(result.summary())

    return {
        "coef_log": coef,
        "se": se,
        "p_value": p,
        "sig": sig,
        "pct": pct,
        "pct_ci": (pct_lo, pct_hi),
        "n_rows": len(df),
        "n_convs": df["conversation_id"].nunique(),
        "n_brand_rows": int(df["is_brand"].sum()),
        "n_human_rows": int((1 - df["is_brand"]).sum()),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--alpha", type=float, default=0.05)
    ap.add_argument("--min-per-conv", type=int, default=1)
    ap.add_argument("--max-rows", type=int, default=None, help="Debug cap on tweet-rows scanned per metric.")
    ap.add_argument("--out-dir", default="lmm_tmp", help="Where to write aggregated CSVs.")
    ap.add_argument("--print-full-summary", action="store_true")
    ap.add_argument("--keep-csv", action="store_true")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    print("[Pass 1] Scanning for shared conversations (>=1 Grok reply AND >=1 human reply)...")
    shared = first_pass_shared_flags(args.input, min_per_conv=args.min_per_conv)
    print(f"[Pass 1] Shared conversations kept: {len(shared):,}")

    print("\n=== LMM Results (conversation-level means -> MixedLM) ===")
    sig_metrics = []

    for metric in METRICS:
        out_csv = os.path.join(args.out_dir, f"{metric}_agg.csv")

        print(f"\n[Pass 2] Aggregating {metric} -> {out_csv}")
        n_out = build_aggregated_csv(
            args.input, shared, metric=metric, out_csv=out_csv, max_rows=args.max_rows
        )
        print(f"[Data:{metric}] Aggregated rows written: {n_out:,} (<= 2 per conversation)")

        print(f"[Fit:{metric}] Fitting MixedLM...")
        out = fit_from_aggregated_csv(out_csv, alpha=args.alpha, print_full_summary=args.print_full_summary)

        direction = "higher" if out["coef_log"] > 0 else "lower"
        print(f"[{metric}]")
        print(f"  Rows: {out['n_rows']:,} | Convs: {out['n_convs']:,}")
        print(f"  is_brand coef (log1p): {out['coef_log']:.6f}  (SE {out['se']:.6f})")
        print(f"  p-value: {out['p_value']:.6g}  | alpha={args.alpha}  | significant={out['sig']}")
        print(f"  Effect (exp(coef)-1): {out['pct']:+.2f}% ({direction} for Grok vs humans within same convo)")
        print(f"  95% CI on % effect: [{out['pct_ci'][0]:+.2f}%, {out['pct_ci'][1]:+.2f}%]")

        if out["sig"]:
            sig_metrics.append(metric)

        if not args.keep_csv:
            try:
                os.remove(out_csv)
            except Exception:
                pass

    print("\n=== Significant metrics ===")
    print(", ".join(sig_metrics) if sig_metrics else "None")
    print("\nDone.")


if __name__ == "__main__":
    main()
