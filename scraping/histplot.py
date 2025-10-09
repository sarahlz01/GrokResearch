import ijson
import math
import matplotlib.pyplot as plt

# ====== CONFIG ======
INPUT_PATH = "./output_CLEANED.json"  # <- your file
ASSISTANT_NAME = "<ASSISTANT>"
CUTOFF = 50# for cropped views

# ====== AGG STATE ======
thread_lengths = []
thread_counter = 0
len_1_counter = 0
max_len = float("-inf")
max_len_thread_id = None

# New collections for Grok ratio plot
grok_ratios = []   # per-thread ratio = (# ASSISTANT tweets) / (thread length)
grok_ratios_with_more_than_50 = []
thread_ids = []    # keep ids around for debug/stats if needed
threads_greater_100_count = 0
# ====== STREAM & COLLECT ======
with open(INPUT_PATH, "r") as f:
    # stream each thread object
    for thread in ijson.items(f, "item.threads.item"):
        threadid = thread.get("threadId", 0)
        tweets = thread.get("tweets", []) or []
        n = len(tweets)

        # basic stats you've had
        thread_counter += 1
        thread_lengths.append(n)
        
        if n >= 100:
            threads_greater_100_count += 1
        if n > max_len:
            max_len = n
            max_len_thread_id = threadid
        if n <= 1:
            len_1_counter += 1

        # --- NEW: compute grok ratio for this thread ---
        if n > 0:
            grok_count = sum(1 for t in tweets if t.get("authorName") == ASSISTANT_NAME)
            ratio = grok_count / n
        else:
            ratio = math.nan  # should not happen with n==0, but safe-guard

        grok_ratios.append(ratio)
        if ratio > 0.5:
            grok_ratios_with_more_than_50.append(threadid)
        thread_ids.append(threadid)

# ====== PRINT STATS ======
if thread_lengths:
    avg_length = sum(thread_lengths) / len(thread_lengths)
    avg_grok_ratio = sum(g for g in grok_ratios if not math.isnan(g)) / len(grok_ratios)
    print(f"Number of threads: {thread_counter}")
    print(f"Average thread length: {avg_length:.3f}")
    print(f"Average Grok reply ratio: {avg_grok_ratio:.3f}")
    print(f"Threads with length <= 1: {len_1_counter}")
    print(f"Max thread length: {max(thread_lengths)} (threadId: {max_len_thread_id})")
    print(f"Number of threads >= 100:\t{threads_greater_100_count}")
    print(f"Grok threads with > 0.5 ratio {len(grok_ratios_with_more_than_50)}")
else:
    print("No threads found.")

# ====== HISTOGRAMS (as in your original) ======
if thread_lengths:
    # Full histogram
    plt.figure(figsize=(8, 6))
    # Use a sensible bins range (1..max_len+1) so edges align on integers
    plt.hist(thread_lengths, bins=range(1, max(thread_lengths) + 2), edgecolor='black', alpha=0.7)
    plt.title("Distribution of Thread Lengths")
    plt.xlabel("Thread Length (number of tweets)")
    plt.ylabel("Frequency")
    plt.xticks(range(1, max(thread_lengths) + 1, max(1, max(thread_lengths)//20)))  # avoid too many ticks
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

# ====== NEW: SCATTER — Grok reply ratio vs. thread length ======
# Full scatter
plt.figure(figsize=(9, 6))
plt.scatter(thread_lengths, grok_ratios, alpha=0.7)
plt.title("Grok Replies Ratio vs. Thread Length")
plt.xlabel("Thread Length (# tweets)")
plt.ylabel("Grok Reply Ratio")
plt.ylim(-0.05, 1.05)
plt.grid(True, linestyle='--', alpha=0.5)
plt.tight_layout()

# Cropped scatter (x-axis capped at CUTOFF)
x_crop = []
y_crop = []
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
