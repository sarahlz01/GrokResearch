import ijson
import math
import matplotlib.pyplot as plt
import statistics as stats  # for median

# ====== CONFIG ======
INPUT_PATH = "./hydration/dehydrated.json"  # <- your file
ASSISTANT_NAME = "<ASSISTANT>"
CUTOFF = 50  # for cropped views

# ====== AGG STATE ======
thread_lengths = []
thread_counter = 0
len_1_counter = 0
max_len = float("-inf")
max_len_thread_id = None

# Grok ratio plot
grok_ratios = []   # per-thread ratio = (# ASSISTANT tweets) / (thread length)
grok_ratios_with_more_than_50 = []
thread_ids = []
threads_greater_20_less_100 = 0

# Tweet text lengths
tweet_text_lengths = []

# ====== NEW: RECOVERY METRICS ======
missing_thread_ids = []
missing_thread_count = 0
total_tweets = 0
tweets_in_complete_threads = 0

# ====== STREAM & COLLECT ======
with open(INPUT_PATH, "r") as f:
    for thread in ijson.items(f, "item.threads.item"):
        threadid = thread.get("threadId", 0)
        tweets = thread.get("tweets", []) or []
        n = len(tweets)

        # ---- recovery flags ----
        # Mark as missing if EITHER flag is True
        is_missing = bool(thread.get("incomplete_thread")) or bool(thread.get("has_missing_parent"))
        if is_missing:
            missing_thread_count += 1
            missing_thread_ids.append(threadid)

        # tweet-level recovery accumulation
        total_tweets += n
        if not is_missing:
            tweets_in_complete_threads += n

        # ---- existing stats ----
        thread_counter += 1
        thread_lengths.append(n)

        if n <= 100 and n>=20:
            threads_greater_20_less_100 += 1
        if n > max_len:
            max_len = n
            max_len_thread_id = threadid
        if n <= 1:
            len_1_counter += 1

        # tweet text lengths
        for t in tweets:
            txt = t.get("text", "")
            if isinstance(txt, str):
                tweet_text_lengths.append(len(txt))

        # grok ratio
        if n > 0:
            grok_count = sum(1 for t in tweets if t.get("authorName") == ASSISTANT_NAME)
            ratio = grok_count / n
        else:
            ratio = math.nan

        grok_ratios.append(ratio)
        if ratio > 0.5:
            grok_ratios_with_more_than_50.append(threadid)
        thread_ids.append(threadid)

# ====== PRINT STATS ======
if thread_lengths:
    avg_thread_length = sum(thread_lengths) / len(thread_lengths)
    median_thread_length = stats.median(thread_lengths)

    valid_groks = [g for g in grok_ratios if not math.isnan(g)]
    avg_grok_ratio = (sum(valid_groks) / len(valid_groks)) if valid_groks else float("nan")

    # ---- NEW: recovery summaries ----
    complete_thread_count = thread_counter - missing_thread_count
    pct_threads_recovered = (complete_thread_count / thread_counter * 100.0) if thread_counter else float("nan")
    pct_tweets_recovered  = (tweets_in_complete_threads / total_tweets * 100.0) if total_tweets else float("nan")

    print(f"Number of threads: {thread_counter}")
    print(f"Average thread length: {avg_thread_length:.3f}")
    print(f"Median thread length:  {median_thread_length:.3f}")
    print(f"Average Grok reply ratio: {avg_grok_ratio:.3f}")
    print(f"Threads with length <= 1: {len_1_counter}")
    print(f"Max thread length: {max(thread_lengths)} (threadId: {max_len_thread_id})")
    print(f"Number of threads 20 <= len <= 100:\t{threads_greater_20_less_100}")
    print(f"Grok threads with > 0.5 ratio {len(grok_ratios_with_more_than_50)}")

    # ---- NEW: recovery output ----
    print("\n--- Recovery ---")
    print(f"Missing/incomplete threads: {missing_thread_count} out of {thread_counter} "
          f"({pct_threads_recovered:.2f}% threads recovered)")
    print(f"Tweets in complete threads: {tweets_in_complete_threads} out of {total_tweets} "
          f"({pct_tweets_recovered:.2f}% tweets recovered)")
else:
    print("No threads found.")

# ====== TWEET LENGTH (characters) STATS ======
if tweet_text_lengths:
    avg_tweet_len = sum(tweet_text_lengths) / len(tweet_text_lengths)
    median_tweet_len = stats.median(tweet_text_lengths)
    print(f"\nAverage tweet text length (chars): {avg_tweet_len:.3f}")
    print(f"Median tweet text length (chars):  {median_tweet_len:.3f}")
else:
    print("\nNo tweet texts found to compute tweet-length statistics.")

# ====== HISTOGRAMS (unchanged) ======
if thread_lengths:
    # Full histogram
    plt.figure(figsize=(8, 6))
    plt.hist(thread_lengths, bins=range(1, max(thread_lengths) + 2), edgecolor='black', alpha=0.7)
    plt.title("Distribution of Thread Lengths")
    plt.xlabel("Thread Length (number of tweets)")
    plt.ylabel("Frequency")
    plt.xticks(range(1, max(thread_lengths) + 1, max(1, max(thread_lengths)//20)))
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()

    # Cropped histogram (≤ CUTOFF)
    tl_le_cut = [x for x in thread_lengths if x <= CUTOFF]
    plt.figure(figsize=(8, 6))
    plt.hist(tl_le_cut, bins=range(1, CUTOFF + 2), edgecolor='black', alpha=0.7)
    plt.title(f"Distribution of Thread Lengths (≤ {CUTOFF} tweets)")
    plt.xlabel("Thread Length (number of tweets)")
    plt.ylabel("Frequency")
    plt.xlim(1, CUTOFF)
    plt.xticks(range(1, CUTOFF + 1, max(1, CUTOFF // 10)))
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()

# ====== SCATTER — Grok reply ratio vs. thread length (unchanged) ======
plt.figure(figsize=(9, 6))
plt.scatter(thread_lengths, grok_ratios, alpha=0.7)
plt.title("Grok Replies Ratio vs. Thread Length")
plt.xlabel("Thread Length (# tweets)")
plt.ylabel("Grok Reply Ratio")
plt.ylim(-0.05, 1.05)
plt.grid(True, linestyle='--', alpha=0.5)
plt.tight_layout()

# Cropped scatter
x_crop, y_crop = [], []
for L, r in zip(thread_lengths, grok_ratios):
    if L <= CUTOFF:
        x_crop.append(L)
        y_crop.append(r)

plt.figure(figsize=(9, 6))
plt.scatter(x_crop, y_crop, alpha=0.7)
plt.title(f"Grok Replies Ratio vs. Thread Length (x ≤ {CUTOFF})")
plt.xlabel("Thread Length (# tweets)")
plt.ylabel("Grok Reply Ratio")
plt.xlim(0.5, CUTOFF + 0.5)
plt.ylim(-0.05, 1.05)
plt.grid(True, linestyle='--', alpha=0.5)
plt.tight_layout()

plt.show()
