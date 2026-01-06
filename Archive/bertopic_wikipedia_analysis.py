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

def topic_analysis(df: pd.DataFrame):

    texts = df["text"].dropna().tolist()

    # Loads pretrained BERTopic model
    topic_model = BERTopic.load("MaartenGr/BERTopic_Wikipedia")

    # Transforms texts into topics
    topics, probs = topic_model.transform(texts)

    # Assigning topics back to dataframe
    df["topic"] = topics
    df["topic_prob"] = probs

    # Extract topic info
    topic_info = topic_model.get_topic_info()

    reps = []
    for topic_num in topic_info["Topic"].unique():
        if topic_num == -1:  # skip outliers
            continue
        docs = topic_model.get_representative_docs(topic_num)
        reps.append({
            "Topic": topic_num,
            "keywords": ", ".join([word for word, _ in topic_model.get_topic(topic_num)[:10]]),
            "Example_Doc": docs[0] if docs else ""
        })
    reps_df = pd.DataFrame(reps)

    # Merges into one dataframe
    df_final = df.merge(
        reps_df, how="left", left_on="topic", right_on="Topic"
    ).drop(columns=["Topic"])

    # Saving everything to one CSV
    df_final.to_csv("conversation_topics.csv", index=False, encoding="utf-8-sig")
    print("✅ All results saved in conversation_topics.csv")

    return topic_model, df_final

if __name__ == "__main__":
    filepath = "grok_data/output_CLEANED.json" 
    data = load_json_data(filepath)
    df = preprocess_data(data, max_conversations=2)  # limit to 2 conversations

    # Clean text
    df["clean_text"] = df["text"].apply(clean_tweet)

    # Aggregate per conversation
    df_conversations = (
        df.groupby("conversationId")["clean_text"]
          .apply(lambda texts: " ".join(texts))
          .reset_index()
    )
    df_conversations.rename(columns={"clean_text": "conversation_text"}, inplace=True)

    # Runs topic modeling
    topic_model, df_topics = topic_analysis(
        df_conversations.rename(columns={"conversation_text": "text"})
    )
