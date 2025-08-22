import ijson
import matplotlib.pyplot as plt

thread_lengths = []
longest_threads = []
get_tweets = True
with open("./grok_data/data.json", "r") as f:
    # stream through "threads.item.tweets" arrays
    for thread in ijson.items(f, "item.threads.item"):
        threadId = thread.get("threadId", "")
        
        tweets = thread.get("tweets", [])
        length_of_current_tweeet = len(tweets)
        if (length_of_current_tweeet >= 40) and get_tweets:   # get 3 tweets > 40
            longest_threads.append(threadId)
            if (len(longest_threads) > 3):
                get_tweets = False
        thread_lengths.append(length_of_current_tweeet)

# Plot histogram
print("Number conversations: " + str(len(thread)))
print("Longest threads:")
for i,thd in enumerate(longest_threads):
    print(thd)
    print("\n\n\n\n")
plt.figure(figsize=(8, 6))
plt.hist(thread_lengths, bins=range(1, max(thread_lengths) + 2), edgecolor='black', alpha=0.7)
plt.title("Distribution of Thread Lengths")
plt.xlabel("Thread Length (number of tweets)")
plt.ylabel("Frequency")
plt.xticks(range(1, max(thread_lengths) + 1))
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()
