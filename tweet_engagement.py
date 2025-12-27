import ijson
from collections import defaultdict
import matplotlib.pyplot as plt
import numpy as np

data_path = '/Users/minhle007/Desktop/GrokResearch/output.json'
engagement = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

print("Processing conversations ...")
with open(data_path, 'rb') as f:
    for convo in ijson.items(f, 'item'):
        for thread in convo.get('threads', []):
            tweets = thread.get('tweets', [])
            for depth, tweet in enumerate(tweets, 1):  # Start depth at 1
                author = tweet.get('author', {}).get('userName', '').lower()
                author_type = 'grok' if author == 'grok' else 'human'
                # Ensure values are integers
                try:
                    replies = int(tweet.get('replyCount', 0) or 0)
                    likes = int(tweet.get('likeCount', 0) or 0)
                    reposts = int(tweet.get('retweetCount', 0) or 0)
                    views = int(tweet.get('viewCount', 0) or 0)
                except (ValueError, TypeError):
                    replies = likes = reposts = views = 0
                engagement[depth]['replies'][author_type].append(replies)
                engagement[depth]['likes'][author_type].append(likes)
                engagement[depth]['reposts'][author_type].append(reposts)
                engagement[depth]['views'][author_type].append(views)

print("Preparing data for plotting ...")
depths_with_data = sorted(engagement.keys())
max_depth = 15  # Limit to depth 15
# Create continuous range of depths from 1 to 15
depths = list(range(1, max_depth + 1))
metrics = ['replies', 'likes', 'reposts']

# Define colors: orange for Grok, blue for Human
colors = {'human': '#1f77b4', 'grok': '#ff7f0e'}  # Blue for human, orange for grok
labels = {'human': 'Human', 'grok': 'Grok'}
# Line styles by metric: solid for replies, dotted for likes, dashed for reposts
metric_styles = {'replies': '-', 'likes': ':', 'reposts': '--'}
metric_titles = {'replies': 'Replies', 'likes': 'Likes', 'reposts': 'Reposts'}
# Different markers for each metric: triangles for replies, squares for likes, circles for reposts
markers = {'replies': '^', 'likes': 's', 'reposts': 'o'}

# Create a single plot with all metrics
fig, ax = plt.subplots(figsize=(14, 8))

# Plot all metrics and author types on the same plot
for metric in metrics:
    for author_type in ['human', 'grok']:
        # Fill in all depths, using NaN for missing depths (breaks the line)
        y = []
        for d in depths:
            if d in engagement and engagement[d][metric][author_type]:
                values = engagement[d][metric][author_type]
                avg = sum(values) / len(values)
                # Note: Averages can be fractional (e.g., 0.5 means on average 0.5 likes per tweet)
                # This is normal when some tweets have 0 engagement and others have 1+
                # For log scale: use a very small value (0.01) for zero averages to avoid log(0) issues
                y.append(avg if avg > 0 else 0.01)  # Use small value for zeros to avoid log scale issues
            else:
                y.append(np.nan)  # Use NaN for missing depths to break the line
        
        ax.plot(
            depths,
            y,
            color=colors[author_type],
            linestyle=metric_styles[metric],
            marker=markers[metric],
            markersize=12,
            linewidth=2,
            alpha=0.8,
            label=f"{labels[author_type]} {metric_titles[metric]}"
        )

ax.set_xlabel('Thread Depth', fontsize=32)
# ax.set_ylabel('Average Count', fontsize=17)
# ax.set_title('Engagement Metrics vs Thread Depth: Human vs Grok', fontsize=18, fontweight='bold')
ax.set_yscale('log')  # Use logarithmic scale to better visualize the data
ax.grid(True, linestyle='--', alpha=0.3, color='gray')
ax.legend(loc='best', fontsize=32, ncol=2)
ax.set_xlim(0.5, 15.5)  # Set x-axis limits to show depth range 1-15
ax.tick_params(axis='both', which='major', labelsize=20)
plt.tight_layout()
plt.show()