import json
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter
from wordcloud import WordCloud, STOPWORDS
from sklearn.feature_extraction.text import CountVectorizer

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
                author=tweet.get("author",{})
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
    df["replyLength"]= df["text"].str.len()
    return df

def basic_statistics(df:pd.DataFrame):
    #This funciton provides basic stats to get a quick overview of the dataset
    print("Total tweets:", len(df))
    print("Total conversations:", df["conversationId"].nunique())
    print("Total unique users:", df["authorId"].nunique())
    print("Avg reply length:", df["replyLength"].mean())

def user_distribution(df: pd.DataFrame):
    # categorizes user distribution by verification status and follower count
    print("Verified users:", df["verified"].sum())
    print("Unverified users:", len(df) - df["verified"].sum())
    bins = [0, 100, 1000, 10000, 100000, np.inf]
    labels = ["0-100", "101-1k", "1k-10k", "10k-100k", "100k+"]
    df["follower_bucket"] = pd.cut(df["followersCount"], bins=bins, labels=labels)
    print(df["follower_bucket"].value_counts())

def topic_analysis(df: pd.DataFrame, top_n=20, plot=True):
    # analyzes word frequency and optional word cloud
    # takes in top_n ( number of words to display) as one of the arguments, can be modified
    # returns a list  of tuples(word,count) of the most common words

    text_data = df["text"].dropna().str.lower().str.replace(r'http\S+|[^a-z\s]', '', regex=True)
    all_words = " ".join(text_data).split()
    all_words = [w for w in all_words if w not in STOPWORDS]

    word_counts = Counter(all_words)
    top_words = word_counts.most_common(top_n)

    #plotting the bar chart and word cloud when requested 
    if plot:
        # Barplot for top N words
        plt.figure(figsize=(10,5))
        sns.barplot(x=[w[1] for w in top_words], y=[w[0] for w in top_words], palette="viridis")
        plt.title(f"Top {top_n} Words in Tweets")
        plt.xlabel("Count")
        plt.ylabel("Word")
        plt.tight_layout()
        plt.show()

        # Word cloud visualization
        wc = WordCloud(width=800, height=400, background_color="white", stopwords=STOPWORDS)
        wc.generate(" ".join(all_words))
        plt.figure(figsize=(12,6))
        plt.imshow(wc, interpolation="bilinear")
        plt.axis("off")
        plt.show()
    return top_words


if __name__=="__main__":
    filepath="grok_data/data.json"
    data=load_json_data(filepath)
    df=preprocess_data(data)
    print("basic stats")
    basic_statistics(df)
    print("\n user distr")
    user_distribution(df)
    print("\n=== Topic Analysis (Top Words & WordCloud) ===")
    top_words = topic_analysis(df, top_n=20, plot=True) 








