import json
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from transformers import Counter

sns.set(style="whitegrid") #for plots

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

    # Initializing BERTopic
    topic_model = BERTopic(embedding_model=embedding_model, min_topic_size=min_topic_size, language="multilingual") 

    # Fit the model
    topics, probs = topic_model.fit_transform(texts)
    
    # Assign topics back to dataframe
    df["topic"] = topics
    df["topic_prob"] = probs
    
    # Extracting topic info
    topic_info = topic_model.get_topic_info()
    print("\nTop Topics:\n", topic_info.head(10))
    
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

    # # Create dictionary and corpus
    # dictionary = corpora.Dictionary(df["tokens"])
    # corpus = [dictionary.doc2bow(text) for text in df["tokens"]]

    # # Training LDA model
    # lda_model = models.LdaModel(corpus, num_topics=num_topics, id2word=dictionary, passes=10, random_state=42)

    # # Printing for now
    # topics = lda_model.print_topics(num_words=num_words)
    # for topic in topics:
    #     print(topic)
    
    # return lda_model, dictionary, corpus


    # text_data = df["text"].dropna().str.lower().str.replace(r'http\S+|[^a-z\s]', '', regex=True)
    # all_words = " ".join(text_data).split()
    # all_words = [w for w in all_words if w not in STOPWORDS]

    # word_counts = Counter(all_words)
    # top_words = word_counts.most_common(top_n)

    # #plotting the bar chart and word cloud when requested 
    # if plot:
    #     # Barplot for top N words
    #     plt.figure(figsize=(10,5))
    #     sns.barplot(x=[w[1] for w in top_words], y=[w[0] for w in top_words], palette="viridis")
    #     plt.title(f"Top {top_n} Words in Tweets")
    #     plt.xlabel("Count")
    #     plt.ylabel("Word")
    #     plt.tight_layout()
    #     plt.show()

    #     # Word cloud visualization
    #     wc = WordCloud(width=800, height=400, background_color="white", stopwords=STOPWORDS)
    #     wc.generate(" ".join(all_words))
    #     plt.figure(figsize=(12,6))
    #     plt.imshow(wc, interpolation="bilinear")
    #     plt.axis("off")
    #     plt.show()
    # return top_words


if __name__=="__main__":
    filepath="grok_data/data.json"
    data=load_json_data(filepath)
    df=preprocess_data(data)
    print("basic stats")
    basic_statistics(df)
    print("\n user distr")
    user_distribution(df)
    print("\n=== Topic Analysis with BERTopic ===")
    topic_model, df = topic_analysis_bertopic(df)









