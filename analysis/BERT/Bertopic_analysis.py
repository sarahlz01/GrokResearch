import json
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import stopwordsiso as stopwords
from sklearn.feature_extraction.text import CountVectorizer
import re 


def clean_tweet(text):
    if not isinstance(text, str):
        return ""
    # removes URLs
    text = re.sub(r"http\S+|www\S+|https\S+", "", text)
    # removes  mentions (@user)
    text = re.sub(r"@\w+", "", text)
    # removes hashtags but keeps the text (#AI -> AI)
    text = re.sub(r"#", "", text)
    # removes digits and underscores
    text = re.sub(r"\d+", "", text)
    text = re.sub(r"_", " ", text)
    # removes extra spaces
    text = re.sub(r"\s+", " ", text).strip()
    return text


sns.set_theme(style="whitegrid") #for plots

# BERTopic imports
from bertopic import BERTopic
from sentence_transformers import SentenceTransformer

# loading  JSON data from file 
def load_json_data(filepath: str):
    with open(filepath,"r", encoding="utf-8") as f:
        return json.load(f)
    

# preprocess_data flattens the nested fields, converts timestamps and computes reply lengths...
#returns a panda DataFrame
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




# multilingual Topic modelling using BerTopic

def topic_analysis(df: pd.DataFrame, embedding_model_name="paraphrase-multilingual-MiniLM-L12-v2", min_topic_size=10):

    # Filters out empty texts
    texts = df["text"].dropna().tolist()    

    # Loads multilingual embedding model
    embedding_model = SentenceTransformer(embedding_model_name)

    multi_stopwords = set()  #builds multilingial stopword set
    for lang in stopwords.langs():
        multi_stopwords |= stopwords.stopwords(lang)

    #tweets specific stopwords
    twitter_stopwords = {"grok", "Grok", "Elon","rt", "via", "amp", "https", "http","x'"}
    multi_stopwords |= twitter_stopwords  

    #creating vecotrizer
    vectorizer_model = CountVectorizer(
        stop_words=list(multi_stopwords)
    )


    # Initializing BERTopic
    topic_model = BERTopic(
        embedding_model=embedding_model,
        vectorizer_model=vectorizer_model,
        min_topic_size=min_topic_size,
        language="multilingual"
    ) 

    # Fit the model
    topics, probs = topic_model.fit_transform(texts)
    
    # Assign topics back to dataframe
    df["topic"] = topics
    df["topic_prob"] = probs
    
    # Extracting topic info
    topic_info = topic_model.get_topic_info()

    #printing only the main info to check
    topic_summary = topic_info[["Topic", "Count", "Name", "Representation"]]
    print("\nTop Topics:\n", topic_summary.head(10).to_string(index=False))
    

    #saving topic summary to CSV
    topic_info.to_csv("topic_summary.csv", index=False, encoding="utf-8-sig")
    print("topic summary has been saved ")

    #saving representative examples for each topic
    reps=[]
    for topic_num in topic_info["Topic"].unique():
        if topic_num ==-1: #skips outliers
            continue 
        docs=topic_model.get_representative_docs(topic_num)
        reps.append({
            "Topic":topic_num,
            "keywords": ", ".join([word for word, _ in topic_model.get_topic(topic_num)[:10]]),
            "Example_Doc": docs[0] if docs else ""
        })
    reps_df = pd.DataFrame(reps)
    reps_df.to_csv("topic_examples.csv", index=False, encoding="utf-8-sig")
    print(" Topic examples saved as topic_examples.csv")

    # Visualize top words per topic
    for topic_num in topic_info["Topic"].unique():
        if topic_num == -1:
            continue  # Skip outliers
        words = topic_model.get_topic(topic_num)
        if not words:
            continue
        words, scores = zip(*words)
        plt.figure(figsize=(8, 4))
        plt.barh(words, scores, color="skyblue")
        plt.xlabel("Importance")
        plt.title(f"Topic {topic_num} Top Words")
        plt.gca().invert_yaxis()
        plt.show()
    
    return topic_model, df

if __name__=="__main__":
    filepath="grok_data/data.json"
    data=load_json_data(filepath)
    df=preprocess_data(data)

    print("basic stats")
    basic_statistics(df)
    print("\n user distr")
    user_distribution(df)

    df["clean_text"] = df["text"].apply(clean_tweet)
    # Aggregating tweets per conversation 
    df_conversations = df.groupby("conversationId")["clean_text"].apply(lambda texts: " ".join(texts)).reset_index()
    df_conversations.rename(columns={"clean_text": "conversation_text"}, inplace=True)

    print("\n=== Topic Analysis with BERTopic ===")
    topic_model, df_topics = topic_analysis(df_conversations.rename(columns={"conversation_text":"text"}))









