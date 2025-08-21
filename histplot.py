import ijson
import matplotlib.pyplot as plt

thread_lengths = []

with open("./grok_data/data.json", "r") as f:
    # stream through "threads.item.tweets" arrays
    for thread in ijson.items(f, "item.threads.item"):
        tweets = thread.get("tweets", [])
        thread_lengths.append(len(tweets))

# Plot histogram
print(len(thread))
plt.figure(figsize=(8, 6))
plt.hist(thread_lengths, bins=range(1, max(thread_lengths) + 2), edgecolor='black', alpha=0.7)
plt.title("Distribution of Thread Lengths")
plt.xlabel("Thread Length (number of tweets)")
plt.ylabel("Frequency")
plt.xticks(range(1, max(thread_lengths) + 1))
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()
