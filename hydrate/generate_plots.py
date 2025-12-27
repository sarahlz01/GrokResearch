#!/usr/bin/env python3
import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional, Iterable
import math
import numpy as np
from matplotlib import transforms as mtransforms

import matplotlib.pyplot as plt
import matplotlib as mpl
from matplotlib.ticker import FuncFormatter
from scipy.stats import mannwhitneyu


mpl.rcParams.update({
    "font.size": 24,        # base
    "axes.titlesize": 20,
    "axes.labelsize": 28,
    "xtick.labelsize": 26,
    "ytick.labelsize": 26,
    "legend.fontsize": 26,
})

# ---------- Config ----------
TWITTER_DATE_FMT = "%a %b %d %H:%M:%S %z %Y"
GROK_USER_ID = "1720665183188922368"  # consistent with your cleaners

# colors
USER_COLOR = "#0081a7"
GROK_COLOR = "#f07167"



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

def proper_case_label(code_or_label: str) -> str:
    """
    Turn language codes / labels into a proper-noun display label.
    - Known codes: use LANGUAGE_LABELS mapping (already proper cased).
    - Unknown codes: Title Case (e.g. "klingon" -> "Klingon"), or for short codes -> uppercase ("kk" -> "KK").
    """
    if not code_or_label:
        return "Unknown"

    s = str(code_or_label).strip()
    low = s.lower()

    # Map legacy aliases
    if low == "iw": low = "he"
    if low == "in": low = "id"

    # Non-linguistic handling
    if low in NON_LINGUISTIC_CODES or low == "non-linguistic":
        return "Other"  # or "Non-linguistic" if you prefer

    # If it's a known language code
    if low in LANGUAGE_LABELS:
        return LANGUAGE_LABELS[low]

    # If it's already a readable label (not just a 2–3 char code), title-case it
    if len(low) > 3 and any(ch.isalpha() for ch in low):
        # keep hyphenated / spaced labels nice
        return " ".join(part.capitalize() for part in s.replace("-", " ").split())

    # fallback: treat as code
    return low.upper()


ENG_METRICS = ("views", "likes", "reposts", "replies", "quotes", "bookmarks")
ENG_BINS = [0,1,2,5,10,20,50,100,200,500,1_000,2_000,5_000,10_000,50_000,100_000,1_000_000]

def _to_int(x): 
    try: return int(x)
    except: 
        try: return int(float(x))
        except: return 0

def _extract_engagement(t: dict) -> dict:
    m = t.get("metrics") or t.get("public_metrics") or {}
    views = t.get("views") or m.get("impression_count") or m.get("views") or t.get("viewCount") or 0
    likes = t.get("likeCount") or m.get("like_count") or 0
    replies = t.get("replyCount") or m.get("reply_count") or t.get("commentCount") or 0
    retweets = t.get("retweetCount") or m.get("retweet_count") or 0
    quotes = t.get("quoteCount") or m.get("quote_count") or 0
    bookmarks = t.get("bookmarkCount") or m.get("bookmark_count") or 0
    return {
        "views": _to_int(views), "likes": _to_int(likes), "replies": _to_int(replies),
        "quotes": _to_int(quotes), "reposts": _to_int(retweets)+_to_int(quotes), "bookmarks": _to_int(bookmarks)
    }

def _empty_hist(): return {m:[0]*len(ENG_BINS) for m in ENG_METRICS}
def _bin_index(v:int)->int: 
    for i in range(len(ENG_BINS)-1):
        if ENG_BINS[i]<=v<ENG_BINS[i+1]: return i
    return len(ENG_BINS)-1

def _update_engagement_aggregates(agg_sums,agg_counts,agg_hists,metrics):
    for m in ENG_METRICS:
        val=_to_int(metrics.get(m,0))
        agg_sums[m]+=val
        agg_counts[m]+=1
        agg_hists[m][_bin_index(val)]+=1

def parse_date_safe(s):
    if not s: return None
    try:
        dt=datetime.strptime(s,TWITTER_DATE_FMT)
        if dt.tzinfo is None: dt=dt.replace(tzinfo=timezone.utc)
        return dt
    except: return None

def week_start(dt):
    if dt.tzinfo is None: dt=dt.replace(tzinfo=timezone.utc)
    d=dt-timedelta(days=dt.weekday())
    return d.replace(hour=0,minute=0,second=0,microsecond=0)

def week_range_inclusive(start_dt,end_dt):
    start=week_start(start_dt); end=week_start(end_dt)
    cur=start; out=[]
    while cur<=end:
        out.append(cur)
        cur+=timedelta(days=7)
    return out

def is_grok_tweet(t):
    author=(t.get("author") or {})
    if str(author.get("id"))==GROK_USER_ID: return True
    if t.get("authorName")=="<ASSISTANT>": return True
    return False

def load_json_list_stream(path):
    try:
        import ijson
        with open(path,"r",encoding="utf-8") as f:
            for item in ijson.items(f,"item"): yield item
        return
    except: pass
    with open(path,"r",encoding="utf-8") as f: data=json.load(f)
    for item in data or []: yield item

def iter_conversations(path): yield from load_json_list_stream(path)
def iter_threads(path):
    for conv in iter_conversations(path):
        for th in conv.get("threads",[]) or []: yield th

# ---------- Aggregation ----------
def aggregate_stats(input_path:str)->Dict:
    total_conversations=0
    conv_language_counts=Counter()
    complete_threads=0
    incomplete_threads=0

    for conv in iter_conversations(input_path):
        total_conversations+=1
        conv_language_counts["lang:"+str(conv.get("conversationId"))]=1
        for th in conv.get("threads",[]) or []:
            if th.get("incomplete_thread") or th.get("has_missing_parent"):
                incomplete_threads+=1
            else:
                complete_threads+=1

    turn_buckets=Counter()
    total_threads=0
    for th in iter_threads(input_path):
        n=len(th.get("tweets",[]) or [])
        if n<=1: continue
        total_threads+=1
        if n>=10: turn_buckets["10+"]+=1
        else: turn_buckets[str(n)]+=1
    # --- NEW: collect tweet counts per thread for std computation ---
    all_thread_lengths = []
    for th in iter_threads(input_path):
        n = len(th.get("tweets", []) or [])
        all_thread_lengths.append(n)
    thread_tweetcount_std = float(np.std(all_thread_lengths)) if all_thread_lengths else 0.0


    lang_user=Counter(); lang_grok=Counter()
    for conv in iter_conversations(input_path):
        for th in conv.get("threads",[]) or []:
            for t in th.get("tweets",[]) or []:
                raw_lang=t.get("lang") or t.get("language") or (t.get("author") or {}).get("lang") or "und"
                label=normalize_lang_code(raw_lang)
                if label is None: continue
                if is_grok_tweet(t): lang_grok[label]+=1
                else: lang_user[label]+=1

    wk_threads_user=Counter(); wk_threads_grok=Counter()
    before_mar_threads_user=0; before_mar_threads_grok=0
    for th in iter_threads(input_path):
        tweets=th.get("tweets",[]) or []
        if not tweets: continue
        dated=[(parse_date_safe(t.get("createdAt")),t) for t in tweets]
        dated=[(d,t) for d,t in dated if d]
        if not dated: continue
        last_dt,last_t=max(dated,key=lambda p:p[0])
        is_g=is_grok_tweet(last_t)
        if last_dt<START_DATE:
            if is_g: before_mar_threads_grok+=1
            else: before_mar_threads_user+=1
        elif last_dt<=END_DATE:
            wk=week_start(last_dt).isoformat()
            if is_g: wk_threads_grok[wk]+=1
            else: wk_threads_user[wk]+=1

    wk_tweets_user=Counter(); wk_tweets_grok=Counter()
    before_mar_tweets_user=0; before_mar_tweets_grok=0
    eng_user_sums=defaultdict(int); eng_grok_sums=defaultdict(int)
    eng_user_counts=defaultdict(int); eng_grok_counts=defaultdict(int)
    eng_user_hists=_empty_hist(); eng_grok_hists=_empty_hist()
    eng_user_vals=defaultdict(list); eng_grok_vals=defaultdict(list)

    # ==== CHANGED BLOCK: skip the first tweet in each thread for engagement + weekly counts ====
    for conv in iter_conversations(input_path):
        for th in conv.get("threads", []) or []:
            tweets = th.get("tweets", []) or []
            if len(tweets) <= 1:
                # no “downstream” tweets to measure engagement on
                continue

            # Skip the first tweet in the thread; only use replies / follow-ups
            for t in tweets[1:]:
                # 1) Author & engagement always counted (even if no createdAt)
                is_g = is_grok_tweet(t)
                em = _extract_engagement(t)
                if is_g:
                    _update_engagement_aggregates(eng_grok_sums, eng_grok_counts, eng_grok_hists, em)
                    for m, v in em.items():
                        eng_grok_vals[m].append(v)
                else:
                    _update_engagement_aggregates(eng_user_sums, eng_user_counts, eng_user_hists, em)
                    for m, v in em.items():
                        eng_user_vals[m].append(v)

                # 2) Time bucketing only if we have a valid date
                dt = parse_date_safe(t.get("createdAt"))
                if not dt:
                    continue

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
    # ==== END CHANGED BLOCK ====
    ALPHA = 0.01

    mwu_results = {}
    for metric in ["likes", "replies"]:
        u = eng_user_vals[metric]
        g = eng_grok_vals[metric]

        U, p = mannwhitneyu(u, g, alternative="two-sided")

        mwu_results[metric] = {
            "test": "Mann–Whitney U (two-sided)",
            "U": float(U),
            "p_value": float(p),
            "alpha": ALPHA,
            "reject_H0": bool(p < ALPHA),
            "n_user": len(u),
            "n_grok": len(g),
        }
                
            

    # Compute new stats: std, median
    def _std(vals): return float(np.std(vals)) if vals else 0.0
    def _median(vals): return float(np.median(vals)) if vals else 0.0
    eng_user_std={m:_std(eng_user_vals[m]) for m in ENG_METRICS}
    eng_grok_std={m:_std(eng_grok_vals[m]) for m in ENG_METRICS}
    eng_user_median={m:_median(eng_user_vals[m]) for m in ENG_METRICS}
    eng_grok_median={m:_median(eng_grok_vals[m]) for m in ENG_METRICS}

    completeness_ratio = complete_threads / max(complete_threads+incomplete_threads,1)

    return {
        "totals":{"conversations":total_conversations,"threads":total_threads},
        "turn_buckets":dict(turn_buckets),
        "thread_tweetcount_std": thread_tweetcount_std,
        "lang_user":dict(lang_user),"lang_grok":dict(lang_grok),
        "wk_threads_user":dict(wk_threads_user),"wk_threads_grok":dict(wk_threads_grok),
        "before_mar_threads_user":before_mar_threads_user,"before_mar_threads_grok":before_mar_threads_grok,
        "wk_tweets_user":dict(wk_tweets_user),"wk_tweets_grok":dict(wk_tweets_grok),
        "before_mar_tweets_user":before_mar_tweets_user,"before_mar_tweets_grok":before_mar_tweets_grok,
        "thread_completeness":{"complete":complete_threads,"incomplete":incomplete_threads,"ratio":completeness_ratio},
        "engagement":{
            "bins":ENG_BINS,"metrics":ENG_METRICS,
            "user":{"sums":dict(eng_user_sums),"counts":dict(eng_user_counts),"hists":eng_user_hists,
                    "std":eng_user_std,"median":eng_user_median},
            "grok":{"sums":dict(eng_grok_sums),"counts":dict(eng_grok_counts),"hists":eng_grok_hists,
                    "std":eng_grok_std,"median":eng_grok_median},
            "mwu": mwu_results,
        },
        "window":{"start":START_DATE.isoformat(),"end":END_DATE.isoformat()},
    }

def _human_int(v):
    v = float(v)
    absv = abs(v)
    if absv >= 1_000_000_000:
        return f"{v/1_000_000_000:.0f}B"
    if absv >= 1_000_000:
        return f"{v/1_000_000:.0f}M"
    if absv >= 1_000:
        return f"{v/1_000:.0f}K"
    return f"{int(v):d}"

def _apply_human_y(ax):
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, pos: _human_int(x)))
    
def plot_tweets_over_weeks_stacked(stats: Dict, save_prefix: str):
    """
    Stacked weekly bars: TWEETS counted by tweet author.
    X-axis shows only month starts (and 'Before Mar').
    """
    weeks = week_range_inclusive(START_DATE, END_DATE)
    user_map: Dict[str, int] = stats.get("wk_tweets_user", {})
    grok_map: Dict[str, int] = stats.get("wk_tweets_grok", {})

    before_user = int(stats.get("before_mar_tweets_user", 0))
    before_grok = int(stats.get("before_mar_tweets_grok", 0))
    user_counts = [before_user] + [int(user_map.get(w.isoformat(), 0)) for w in weeks]
    grok_counts = [before_grok] + [int(grok_map.get(w.isoformat(), 0)) for w in weeks]

    # numeric x positions (0 = 'Before Mar', 1..N = weekly bars)
    x = np.arange(len(weeks) + 1)

    plt.figure(figsize=(14, 6))
    plt.bar(x, user_counts, label="User", color=USER_COLOR)
    plt.bar(x, grok_counts, bottom=user_counts, label="Grok", color=GROK_COLOR)


    # month ticks: 0 ('Before Mar') + index where month changes
    month_pos = []
    month_lab = []
    last_month = None
    for i, w in enumerate(weeks, start=1):  # 0 is the 'pre-March' bar
        if w < START_DATE:
            continue  # don't label the Feb-24 week
        if w.month != last_month:
            month_pos.append(i)
            month_lab.append(w.strftime("%b"))  # Mar, Apr, ...
            last_month = w.month

    plt.xticks(month_pos, month_lab, rotation=0, ha="center")
    plt.xlabel("Month")
    plt.ylabel("Tweets")
    _apply_human_y(plt.gca())
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{save_prefix}_tweets_per_week.png", dpi=220)
    
# ---------- Plotting ----------
def plot_turns(stats: Dict, save_prefix: str):
    """Unchanged from your latest: percent of total (not stacked)."""
    buckets: Dict[str, int] = stats.get("turn_buckets", {})
    total_threads = max(sum(buckets.values()), 1)
    xs = [str(i) for i in range(2, 10)] + ["10+"]
    ys = [(buckets.get(x, 0) * 100.0 / total_threads) for x in xs]

    plt.figure(figsize=(9, 6))
    plt.bar(xs, ys, color=USER_COLOR)
    plt.xlabel("Number of turns")
    plt.ylabel("% of total conversations")
    plt.ylim(0, 50)
    plt.tight_layout()
    plt.savefig(f"{save_prefix}_turns.png", dpi=220)


def _remap_langs_for_plot(d: Dict[str, int]) -> Counter:
    out = Counter()
    for k, v in (d or {}).items():
        label = proper_case_label(k)
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

     # --- plot ---
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(labels, user_counts, label="User", color=USER_COLOR)
    ax.bar(labels, grok_counts, bottom=user_counts, label="Grok", color=GROK_COLOR)

    ax.set_ylabel("Tweets")
    _apply_human_y(ax)

    # Normal tick styling
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=30, ha="right")

    # >>> Nudge tick labels a few pixels to the RIGHT <<<
    dx_pts = 18  # change to 2/6/etc to taste
    offset = mtransforms.ScaledTranslation(dx_pts / 72.0, 0.0, fig.dpi_scale_trans)
    for tick in ax.get_xticklabels():
        tick.set_transform(tick.get_transform() + offset)

    ax.legend()
    fig.tight_layout()
    fig.savefig(f"{save_prefix}_languages.png", dpi=220)


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
    plt.bar(labels, counts, color=USER_COLOR)  # single solid series
    plt.xlabel("Week (start date)")
    plt.ylabel("Threads")
    _apply_human_y(plt.gca())
    plt.xticks(rotation=90, ha="center")
    plt.tight_layout()
    plt.savefig(f"{save_prefix}_threads_per_week.png", dpi=220)

def plot_engagement_totals(stats: Dict, save_prefix: str, use_log_scale: bool = True):
    """
    Compare AVERAGE engagement per tweet for User vs Grok.
    Uses stats['engagement']['sums'] and ['counts'] to compute means.

    Also prints mean ± 95% CI for each metric to stdout.
    """
    eng = stats.get("engagement", {})

    # Use original labels for plot/printing, but lowercase keys for lookup
    metric_labels: List[str] = list(eng.get("metrics", []))
    metric_keys: List[str]   = [m.lower() for m in metric_labels]

    user = (eng.get("user") or {})
    grok = (eng.get("grok") or {})

    user_sums: Dict[str, int]   = user.get("sums", {}) or {}
    grok_sums: Dict[str, int]   = grok.get("sums", {}) or {}
    user_counts: Dict[str, int] = user.get("counts", {}) or {}
    grok_counts: Dict[str, int] = grok.get("counts", {}) or {}
    user_std: Dict[str, float]  = user.get("std", {}) or {}
    grok_std: Dict[str, float]  = grok.get("std", {}) or {}

    if not metric_labels:
        print("No engagement metrics found in stats['engagement'].")
        return

    # --- helpers (expect LOWERCASE metric key) ---
    def _mean(sums: Dict[str, int], counts: Dict[str, int], key: str) -> float:
        n = int(counts.get(key, 0))
        if n <= 0:
            return 0.0
        return float(sums.get(key, 0)) / n

    def _se(std_map: Dict[str, float], counts: Dict[str, int], key: str) -> float:
        n = int(counts.get(key, 0))
        if n <= 1:
            return 0.0
        return float(std_map.get(key, 0.0)) / math.sqrt(n)

    def _ci95(mean: float, se: float) -> tuple[float, float]:
        half = 1.96 * se
        return mean - half, mean + half

    # values for plotting (use lowercase keys)
    u_vals = [_mean(user_sums, user_counts, k) for k in metric_keys]
    g_vals = [_mean(grok_sums, grok_counts, k) for k in metric_keys]

    # --- print table to stdout ---
    print("\n=== Engagement means ± 95% CI (per tweet) ===")
    for label, key in zip(metric_labels, metric_keys):
        u_mean = _mean(user_sums, user_counts, key)
        g_mean = _mean(grok_sums, grok_counts, key)
        u_se = _se(user_std, user_counts, key)
        g_se = _se(grok_std, grok_counts, key)
        u_lo, u_hi = _ci95(u_mean, u_se)
        g_lo, g_hi = _ci95(g_mean, g_se)

        print(
            f"{label:>8} | "
            f"User: {u_mean:.3f} ± {1.96*u_se:.3f}  ({u_lo:.3f}, {u_hi:.3f})  |  "
            f"Grok: {g_mean:.3f} ± {1.96*g_se:.3f}  ({g_lo:.3f}, {g_hi:.3f})"
        )

    # --- plot as before (use original labels on x-axis) ---
    x = list(range(len(metric_labels)))
    width = 0.45

    plt.figure(figsize=(14, 6))
    plt.bar([i - width/2 for i in x], u_vals, width=width, label="User", color=USER_COLOR)
    plt.bar([i + width/2 for i in x], g_vals, width=width, label="Grok", color=GROK_COLOR)


    plt.xlabel("Engagement metric")
    plt.ylabel("Average per tweet")
    _apply_human_y(plt.gca())
    plt.xticks(x, metric_labels, rotation=0)
    if use_log_scale:
        plt.yscale("log")
        plt.ylabel("Average per tweet")
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{save_prefix}_eng_avgs.png", dpi=220)
    plt.close()



def _format_bin_labels(edges: List[int]) -> List[str]:
    """
    Turn edges like [0,1,2,5,10,...,1000000] into:
    ["0", "1", "2–4", "5–9", "10–19", ..., "1000000+"]
    """
    labels = []
    for i in range(len(edges) - 1):
        a, b = edges[i], edges[i + 1]
        if b - a == 1:
            labels.append(f"{a}")
        elif a == 0 and b == 1:
            labels.append("0")
        else:
            labels.append(f"{a}–{b-1}")
    labels.append(f"{edges[-1]}+")
    return labels




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
    p.add_argument("--save-prefix", type=str, default="grokset", help="Prefix for saved plot files.")
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
        plot_engagement_totals(stats, args.save_prefix)
        print(
            "Saved:",
            f"{args.save_prefix}_turns.png,",
            f"{args.save_prefix}_languages.png,",
            f"{args.save_prefix}_threads_per_week.png,",
            f"{args.save_prefix}_tweets_per_week.png",
        )


if __name__ == "__main__":
    main()
