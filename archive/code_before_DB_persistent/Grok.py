import requests
import json
import time
from collections import defaultdict
from tqdm import tqdm

# Configuration for Twitter API
API_KEY = "YOUR_TWITTERAPI_IO_KEY"  # <-- Replace with your actual API key
HEADERS = {"x-api-key": API_KEY}
BASE_URL = "https://api.twitterapi.io"

# Number of tweets to fetch in search results
SEARCH_COUNT = 100  
# Number of conversations to fetch
MAX_CONVERSATIONS = 30  

# Search for tweets mentioning @grok
def search_grok_tweets(query="@grok", count=SEARCH_COUNT):
    url = f"{BASE_URL}/search/tweets"
    params = {"q": query, "count": count, "lang": "en", "result_type": "recent"}
    response = requests.get(url, headers=HEADERS, params=params)
    response.raise_for_status()
    return response.json()["statuses"]

# Extract unique conversation IDs
def extract_conversation_starters(tweets):
    starters = {}
    for tweet in tweets:
        tweet_id = tweet.get("id_str")
        if tweet.get("in_reply_to_status_id") is None:
            starters[tweet_id] = tweet
    return starters

# Get all replies to a tweet ID
def get_replies_to(tweet_id):
    query = f"in_reply_to_status_id:{tweet_id}"
    replies = search_grok_tweets(query=query, count=100)
    return replies

# Reconstruct full conversations
def build_conversations(conversation_starters):
    all_conversations = defaultdict(list)

    for conv_id, starter in tqdm(conversation_starters.items(), desc="Fetching conversations"):
        all_conversations[conv_id].append(starter)
        try:
            replies = get_replies_to(conv_id)
            all_conversations[conv_id].extend(replies)
            time.sleep(1)  # polite rate limit
        except Exception as e:
            print(f"Error fetching replies for {conv_id}: {e}")

    return all_conversations

# main
if __name__ == "__main__":
    print("Searching for tweets mentioning @grok...")
    tweets = search_grok_tweets()
    starters = extract_conversation_starters(tweets)

    if len(starters) == 0:
        print("No conversation starters found.")
        exit()

    print(f"Found {len(starters)} conversation starters.")
    limited_starters = dict(list(starters.items())[:MAX_CONVERSATIONS])

    print(f"Fetching full conversations (max {MAX_CONVERSATIONS})...")
    conversations = build_conversations(limited_starters)

    # Save output
    with open("grok_conversations.json", "w") as f:
        json.dump(conversations, f, indent=2)

    print("Conversations saved to grok_conversations.json")
