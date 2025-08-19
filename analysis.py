import json
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

sns.set(style="whitegrid")

# fucntion to load JSON data from file 

def load_json_data(filepath: str):
    with open(filepath,"r", encoding="utf-8") as f:
        return json.load(f)
    

# preprocess_data flattens the nested fields, converts timestamps and computes reply lengths...
def preprocess_data(data):
    rows=[]
    for conv in data:
        for thread in conv.get("threads",[]):
            for tweet in thread.get("tweets",[]):
                author=tweet.get("author",{}):
                rows.append({
                    "conversationId": tweet.get("conversationId"),
                    "threadId": thread.get("threadId"),
                    "tweetId": tweet.get("id"),
                    "text": tweet.get("text"),
                    "authorId": author.get("id"),
                    "authorName": author.get("name"),
                    "authorUsername": author.get("userName"),
                    "verified": author.get("isVerified", False),
                    "followersCount": author.get("followers", 0),
                    "createdAt": datetime.strptime(tweet.get("createdAt"), "%a %b %d %H:%M:%S %z %Y") if tweet.get("createdAt") else None,
                    "retweetCount": tweet.get("retweetCount", 0),
                    "likeCount": tweet.get("likeCount", 0),
                    "replyCount": tweet.get("replyCount", 0),
                    "quoteCount": tweet.get("quoteCount", 0),
                    "lang": tweet.get("lang"),
                    "isReply": tweet.get("isReply", False)
                })
    df = pd.DataFrame(rows)
    df["reply.length"]= de["text"].str.len()
    return df