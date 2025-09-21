import ijson
import matplotlib.pyplot as plt

thread_lengths = []
max_len = float("-inf")
thid = 0
with open("./grok_data/1-2july/output.json", "r") as f: # !! replace with the output.json (NOT CLEANED)
    # stream through "threads.item.tweets" arrays
    for thread in ijson.items(f, "item.threads.item"):
        threadid = thread.get("threadId", 0)
        tweets = thread.get("tweets", [])
        thread_lengths.append(len(tweets))
        if len(tweets) >   max_len:
            max_len = len(tweets)
            thid = threadid
if thread_lengths:
    avg_length = sum(thread_lengths) / len(thread_lengths)
    print(f"Average thread length: {avg_length:.2f} tweets")
else:
    print("No threads found.")

# Plot histogram
print("max thread length\t" + str(max(thread_lengths)) +"\t With threadId: " + str(thid) )
plt.figure(figsize=(8, 6))
plt.hist(thread_lengths, bins=range(1, 50), edgecolor='black', alpha=0.7)
plt.title("Distribution of Thread Lengths")
plt.xlabel("Thread Length (number of tweets)")
plt.ylabel("Frequency")
plt.xticks(range(1, max(thread_lengths) + 1, 50))
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()
