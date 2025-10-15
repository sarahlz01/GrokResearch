#!/usr/bin/env python3
import argparse
import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional, Iterable

import matplotlib.pyplot as plt
import matplotlib as mpl
mpl.rcParams.update({
    "font.size": 16,        # base
    "axes.titlesize": 18,
    "axes.labelsize": 18,
    "xtick.labelsize": 16,
    "ytick.labelsize": 16,
    "legend.fontsize": 14,
})

# ---------- Config ----------
TWITTER_DATE_FMT = "%a %b %d %H:%M:%S %z %Y"
GROK_USER_ID = "1720665183188922368"  # consistent with your cleaners

# Weekly window (inclusive). Everything before START_DATE -> "Before Mar". After END_DATE ignored.
WINDOW_YEAR = 2025
START_DATE = datetime(WINDOW_YEAR, 3, 1, tzinfo=timezone.utc)
END_DATE   = datetime(WINDOW_YEAR, 10, 31, tzinfo=timezone.utc)

LANGUAGE_LABELS = {
    "en": "English",
    "es": "Spanish",
    "pt": "Portuguese",
    "tr": "Turkish",
    "fa": "Persian (Farsi)",
    "pl": "Polish",
    "ja": "Japanese",
    "ar": "Arabic",
    "tl": "Tagalog",
    "fr": "French",
    "in": "Indonesian",   # legacy code; new is 'id'
    "id": "Indonesian",
    "hi": "Hindi",
    "ta": "Tamil",
    "ro": "Romanian",
    "ca": "Catalan",
    "it": "Italian",
    "nl": "Dutch",
    "ru": "Russian",
    "ko": "Korean",
    "el": "Greek",
    "iw": "Hebrew",       # legacy code; new is 'he'
    "he": "Hebrew",
    "mr": "Marathi",
    "vi": "Vietnamese",
    "de": "German",
    "et": "Estonian",
    "cy": "Welsh",
    "ur": "Urdu",
    "zh": "Chinese",
    "ht": "Haitian Creole",
    "lt": "Lithuanian",
    "cs": "Czech",
    "ps": "Pashto",
    "fi": "Finnish",
    "da": "Danish",
    "ne": "Nepali",
    "uk": "Ukrainian",
    "te": "Telugu",
    "gu": "Gujarati",
    "lv": "Latvian",
    "or": "Odia",
    "sv": "Swedish",
    "ml": "Malayalam",
    "sl": "Slovenian",
    "no": "Norwegian",
    "bn": "Bengali",
    "kn": "Kannada",
    "si": "Sinhala",
    "pa": "Punjabi",
    "eu": "Basque",
    "sr": "Serbian",
    "am": "Amharic",
    "ckb": "Central Kurdish",
    "hu": "Hungarian",
    "bg": "Bulgarian",
    "th": "Thai",
    "is": "Icelandic",
    "sd": "Sindhi",
    "ka": "Georgian",
}

# Twitter "non-linguistic" codes → either SKIP or GROUP under a single label
NON_LINGUISTIC_CODES = {
    "qam": "Mentions-only",
    "qct": "Cashtags-only",
    "qht": "Hashtags-only",
    "qme": "Media link",
    "qst": "Very short text",
    "zxx": "No linguistic content",
    "und": "Undetermined",
    "art": "Artificial / constructed",
}

GROUP_NON_LINGUISTIC_UNDER = "Non-linguistic"   # set to None to DROP them entirely

def normalize_lang_code(code: str) -> Optional[str]:
    """Return display label or None to drop."""
    if not code:
        return None
    c = str(code).lower()

    # Map legacy aliases
    if c == "iw": c = "he"
    if c == "in": c = "id"

    # Non-linguistic handling
    if c in NON_LINGUISTIC_CODES:
        return GROUP_NON_LINGUISTIC_UNDER or None

    # Regular languages
    return LANGUAGE_LABELS.get(c, c.upper())  # fallback: show code uppercased

# ---------- Utilities ----------
def parse_date_safe(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        dt = datetime.strptime(s, TWITTER_DATE_FMT)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def week_start(dt: datetime) -> datetime:
    """Return Monday 00:00Z of dt's ISO week."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    d = dt - timedelta(days=dt.weekday())
    return d.replace(hour=0, minute=0, second=0, microsecond=0)


def week_range_inclusive(start_dt: datetime, end_dt: datetime) -> List[datetime]:
    start = week_start(start_dt)
    end   = week_start(end_dt)
    cur = start
    out = []
    while cur <= end:
        out.append(cur)
        cur += timedelta(days=7)
    return out


def is_grok_tweet(t: dict) -> bool:
    # Prefer raw author.id check; fallback to cleaned authorName sentinel.
    author = (t.get("author") or {})
    if str(author.get("id")) == GROK_USER_ID:
        return True
    if t.get("authorName") == "<ASSISTANT>":
        return True
    return False


def load_json_list_stream(path: str) -> Iterable[dict]:
    """Stream a top-level JSON array from disk if ijson is available; else load fully."""
    try:
        import ijson  # type: ignore
        with open(path, "r", encoding="utf-8") as f:
            for item in ijson.items(f, "item"):
                yield item
        return
    except Exception:
        pass
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    for item in data or []:
        yield item


def iter_conversations(path: str) -> Iterable[dict]:
    yield from load_json_list_stream(path)


def iter_threads(path: str) -> Iterable[dict]:
    for conv in iter_conversations(path):
        for th in conv.get("threads", []) or []:
            yield th


def majority_language_for_conversation(conv: dict) -> str:
    """(Kept for totals/compat) Majority language inside a conversation."""
    counts = Counter()
    for th in conv.get("threads", []) or []:
        for t in th.get("tweets", []) or []:
            lang = (
                t.get("lang")
                or t.get("language")
                or (t.get("author") or {}).get("lang")
                or "Other"
            )
            counts[str(lang)] += 1
    return counts.most_common(1)[0][0] if counts else "Other"


# ---------- Aggregation ----------
def aggregate_stats(input_path: str) -> Dict:
    """
    Build all stats in one pass (streamed):
      - language tweet counts split by author (user vs grok)
      - turn buckets (2..10+), percent plotted later (unchanged)
      - weekly THREADS/time split by last author (user/grok), + 'Before Mar'
      - weekly TWEETS/time split by author (user/grok), + 'Before Mar'
    """
    # (A) Conversations total + (legacy) majority language per conversation
    total_conversations = 0
    conv_language_counts = Counter()
    for conv in iter_conversations(input_path):
        total_conversations += 1
        conv_language_counts[majority_language_for_conversation(conv)] += 1

    # (B) Turn distribution for threads (keep as-is; not stacked)
    turn_buckets = Counter()  # keys: "2".."9","10+"
    total_threads = 0
    for th in iter_threads(input_path):
        n = len(th.get("tweets", []) or [])
        if n <= 1:
            continue
        total_threads += 1
        if n >= 10:
            turn_buckets["10+"] += 1
        else:
            turn_buckets[str(n)] += 1

    # (C) Language tweet counts split by author
    lang_user = Counter()
    lang_grok = Counter()
    for conv in iter_conversations(input_path):
        for th in conv.get("threads", []) or []:
            for t in th.get("tweets", []) or []:
                raw_lang = (
                    t.get("lang")
                    or t.get("language")
                    or (t.get("author") or {}).get("lang")
                    or "und"
                )
                label = normalize_lang_code(raw_lang)
                if label is None:
                    continue  # drop non-linguistic if configured
                if is_grok_tweet(t):
                    lang_grok[label] += 1
                else:
                    lang_user[label] += 1

    # (D) Weekly THREADS/time split by last author
    wk_threads_user = Counter()
    wk_threads_grok = Counter()
    before_mar_threads_user = 0
    before_mar_threads_grok = 0
    for th in iter_threads(input_path):
        tweets = th.get("tweets", []) or []
        if not tweets:
            continue
        # determine last tweet and date
        dated = [(parse_date_safe(t.get("createdAt")), t) for t in tweets]
        dated = [(d, t) for d, t in dated if d]
        if not dated:
            continue
        last_dt, last_t = max(dated, key=lambda p: p[0])
        is_g = is_grok_tweet(last_t)
        if last_dt < START_DATE:
            if is_g:
                before_mar_threads_grok += 1
            else:
                before_mar_threads_user += 1
        elif last_dt <= END_DATE:
            wk = week_start(last_dt).isoformat()
            if is_g:
                wk_threads_grok[wk] += 1
            else:
                wk_threads_user[wk] += 1
        # > END_DATE ignored

    # (E) Weekly TWEETS/time split by author
    wk_tweets_user = Counter()
    wk_tweets_grok = Counter()
    before_mar_tweets_user = 0
    before_mar_tweets_grok = 0
    for conv in iter_conversations(input_path):
        for th in conv.get("threads", []) or []:
            for t in th.get("tweets", []) or []:
                dt = parse_date_safe(t.get("createdAt"))
                if not dt:
                    continue
                is_g = is_grok_tweet(t)
                if dt < START_DATE:
                    if is_g:
                        before_mar_tweets_grok += 1
                    else:
                        before_mar_tweets_user += 1
                elif dt <= END_DATE:
                    wk = week_start(dt).isoformat()
                    if is_g:
                        wk_tweets_grok[wk] += 1
                    else:
                        wk_tweets_user[wk] += 1
                # > END_DATE ignored

    return {
        "totals": {
            "conversations": total_conversations,
            "threads": total_threads,
        },
        "turn_buckets": dict(turn_buckets),  # "2".."9","10+"
        "conv_language_counts": dict(conv_language_counts),  # kept for reference
        "lang_user": dict(lang_user),
        "lang_grok": dict(lang_grok),
        "wk_threads_user": dict(wk_threads_user),
        "wk_threads_grok": dict(wk_threads_grok),
        "before_mar_threads_user": before_mar_threads_user,
        "before_mar_threads_grok": before_mar_threads_grok,
        "wk_tweets_user": dict(wk_tweets_user),
        "wk_tweets_grok": dict(wk_tweets_grok),
        "before_mar_tweets_user": before_mar_tweets_user,
        "before_mar_tweets_grok": before_mar_tweets_grok,
        "window": {"start": START_DATE.isoformat(), "end": END_DATE.isoformat()},
    }


# ---------- Plotting ----------
def plot_turns(stats: Dict, save_prefix: str):
    """Unchanged from your latest: percent of total (not stacked)."""
    buckets: Dict[str, int] = stats.get("turn_buckets", {})
    total_threads = max(sum(buckets.values()), 1)
    xs = [str(i) for i in range(2, 10)] + ["10+"]
    ys = [(buckets.get(x, 0) * 100.0 / total_threads) for x in xs]

    plt.figure(figsize=(9, 6))
    plt.bar(xs, ys)
    plt.xlabel("Number of turns")
    plt.ylabel("% of total conversations")
    plt.ylim(0, 100)
    plt.tight_layout()
    plt.savefig(f"{save_prefix}_turns.png", dpi=220)


def _remap_langs_for_plot(d: Dict[str, int]) -> Counter:
    out = Counter()
    for k, v in (d or {}).items():
        key = (k or "").lower()
        if key in NON_LINGUISTIC_CODES or key == "non-linguistic":
            out["Other"] += int(v)
        else:
            label = LANGUAGE_LABELS.get(key, key.upper())  # “en”->“EN” unless mapped
            out[label] += int(v)
    return out

def plot_languages_stacked(stats: Dict, save_prefix: str):
    """
    Stacked bar per language: User (blue) + Grok (orange).
    Y-axis shows absolute tweet counts by language (not percent), stacked.
    """
    user = _remap_langs_for_plot(stats.get("lang_user", {}))
    grok = _remap_langs_for_plot(stats.get("lang_grok", {}))

    # Compose totals per language
    all_langs = set(user.keys()) | set(grok.keys())
    items = sorted(((lang, user[lang] + grok[lang]) for lang in all_langs),
                   key=lambda kv: kv[1], reverse=True)

    # Keep top-9 non-"Other" + merge remainder into "Other"
    other_explicit_user = user.get("Other", 0)
    other_explicit_grok = grok.get("Other", 0)
    non_other_items = [(k, v) for k, v in items if k != "Other"]

    top9 = non_other_items[:9]
    remainder_user = other_explicit_user + sum(user.get(k, 0) for k, _ in non_other_items[9:])
    remainder_grok = other_explicit_grok + sum(grok.get(k, 0) for k, _ in non_other_items[9:])

    labels = [k for k, _ in top9]
    user_counts = [user.get(k, 0) for k, _ in top9]
    grok_counts = [grok.get(k, 0) for k, _ in top9]
    if (remainder_user + remainder_grok) > 0:
        labels.append("Other")
        user_counts.append(remainder_user)
        grok_counts.append(remainder_grok)

    # Plot stacked counts (legend: User / Grok; Grok is orange)
    plt.figure(figsize=(12, 6))
    plt.bar(labels, user_counts, label="User")
    plt.bar(labels, grok_counts, bottom=user_counts, label="Grok", color="orange")
    plt.ylabel("Tweets")
    plt.xticks(rotation=30, ha="right")
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{save_prefix}_languages.png", dpi=220)


def _week_labels() -> List[str]:
    return ["Before Mar"] + [w.strftime("%Y-%m-%d") for w in week_range_inclusive(START_DATE, END_DATE)]


def plot_threads_over_weeks_stacked(stats: Dict, save_prefix: str):
    """
    Weekly bars: THREADS counted by last-tweet date, Grok+User merged.
    """
    weeks = week_range_inclusive(START_DATE, END_DATE)
    user_map: Dict[str, int] = stats.get("wk_threads_user", {})
    grok_map: Dict[str, int] = stats.get("wk_threads_grok", {})

    labels = ["Before Mar"] + [w.strftime("%Y-%m-%d") for w in weeks]
    before_total = int(stats.get("before_mar_threads_user", 0)) + int(stats.get("before_mar_threads_grok", 0))
    counts = [before_total] + [
        int(user_map.get(w.isoformat(), 0)) + int(grok_map.get(w.isoformat(), 0))
        for w in weeks
    ]

    plt.figure(figsize=(14, 6))
    plt.bar(labels, counts)  # single solid series
    plt.xlabel("Week (start date)")
    plt.ylabel("Threads")
    plt.xticks(rotation=60, ha="right")
    plt.tight_layout()
    plt.savefig(f"{save_prefix}_threads_per_week.png", dpi=220)


def plot_tweets_over_weeks_stacked(stats: Dict, save_prefix: str):
    """
    Stacked weekly bars: TWEETS counted by tweet author.
    """
    weeks = week_range_inclusive(START_DATE, END_DATE)
    user_map: Dict[str, int] = stats.get("wk_tweets_user", {})
    grok_map: Dict[str, int] = stats.get("wk_tweets_grok", {})

    labels = _week_labels()
    user_counts = [int(stats.get("before_mar_tweets_user", 0))] + [int(user_map.get(w.isoformat(), 0)) for w in weeks]
    grok_counts = [int(stats.get("before_mar_tweets_grok", 0))] + [int(grok_map.get(w.isoformat(), 0)) for w in weeks]

    plt.figure(figsize=(14, 6))
    plt.bar(labels, user_counts, label="User")
    plt.bar(labels, grok_counts, bottom=user_counts, label="Grok", color="orange")
    plt.xlabel("Week (start date)")
    plt.ylabel("Tweets")
    plt.xticks(rotation=60, ha="right")
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{save_prefix}_tweets_per_week.png", dpi=220)


# ---------- I/O ----------
def save_stats(stats: Dict, out_path: str):
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)


def load_stats(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ---------- CLI ----------
def main():
    p = argparse.ArgumentParser(description="Aggregate and plot WildChat dataset statistics.")
    p.add_argument("--input", type=str, help="Path to full conversations JSON (thread-organized).")
    p.add_argument("--aggregate", action="store_true", help="Aggregate stats from --input and write --out.")
    p.add_argument("--out", type=str, default="stats.json", help="Where to write aggregated stats JSON.")
    p.add_argument("--plot", action="store_true", help="Plot charts from an aggregated stats JSON via --stats.")
    p.add_argument("--stats", type=str, help="Path to aggregated stats JSON.")
    p.add_argument("--save-prefix", type=str, default="wildchat", help="Prefix for saved plot files.")
    args = p.parse_args()

    if args.aggregate:
        if not args.input:
            raise SystemExit("--aggregate requires --input")
        stats = aggregate_stats(args.input)
        save_stats(stats, args.out)
        print(f"Wrote aggregated stats → {args.out}")

    if args.plot:
        path = args.stats or args.out
        stats = load_stats(path)
        # Turns (keep as percent, not stacked)
        plot_turns(stats, args.save_prefix)
        # Stacked bar graphs everywhere else
        plot_languages_stacked(stats, args.save_prefix)
        plot_threads_over_weeks_stacked(stats, args.save_prefix)
        plot_tweets_over_weeks_stacked(stats, args.save_prefix)
        print(
            "Saved:",
            f"{args.save_prefix}_turns.png,",
            f"{args.save_prefix}_languages.png,",
            f"{args.save_prefix}_threads_per_week.png,",
            f"{args.save_prefix}_tweets_per_week.png",
        )


if __name__ == "__main__":
    main()
