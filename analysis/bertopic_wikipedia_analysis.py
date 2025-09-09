import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import re
from bertopic import BERTopic

sns.set(style="whitegrid")  # for plots


def clean_tweet(text):
    """Basic cleaning for tweets"""
    if not isinstance(text, str):
        return ""
    # removes URLs
    text = re.sub(r"http\S+|www\S+|https\S+", "", text)
    # removes mentions (@user)
    text = re.sub(r"@\w+", "", text)
    # removes hashtags but keeps the text (#AI -> AI)
    text = re.sub(r"#", "", text)
    # removes digits and underscores
    text = re.sub(r"\d+", "", text)
    text = re.sub(r"_", " ", text)
    # removes extra spaces
    text = re.sub(r"\s+", " ", text).strip()
    return text


def load_json_data(filepath: str):
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def preprocess_data(data, max_conversations=2):
    """
    Flattens the cleaned JSON into a DataFrame.
    Keeps only the text, author, and conversation info.
    Can limit the number of conversations for performance.
    """
    rows = []
    for i, conv in enumerate(data):
        if max_conversations and i >= max_conversations:
            break
        for thread in conv.get("threads", []):
            for tweet in thread.get("tweets", []):
                rows.append({
                    "conversationId": conv.get("conversationId"),
                    "threadId": thread.get("threadId"),
                    "text": tweet.get("text", ""),
                    "authorName": tweet.get("authorName", "")
                })
    df = pd.DataFrame(rows)
    df["replyLength"] = df["text"].str.len()
    return df
