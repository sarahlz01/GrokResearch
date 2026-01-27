import ijson
import pandas as pd
import stopwordsiso as stopwords
import re
import os

from sklearn.feature_extraction.text import CountVectorizer
from sentence_transformers import SentenceTransformer
from bertopic import BERTopic


# Configuration
DATA_PATH = "output_CLEANED (1).json"
OUTPUT_DIR = "outputs"

MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"
MIN_TOPIC_SIZE = 10

# Number of top keywords used to form lightweight topic labels
K_KEYWORDS = 5

os.makedirs(OUTPUT_DIR, exist_ok=True)



# Text preprocessing
def clean_tweet(text: str) -> str:
    """
    Applies minimal normalization,
    The goal is not aggressive cleaning, but removal of elements
    that add noise to topic discovery.
    """
    if not isinstance(text, str):
        return ""

    text = re.sub(r"http\S+|www\S+|https\S+", "", text)  # removes URLs
    text = re.sub(r"@\w+", "", text)                     # removes mentions
    text = re.sub(r"#", "", text)                        # keeps hashtag words
    text = re.sub(r"\d+", "", text)                      # removes numbers
    text = re.sub(r"_", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text



# Data loading
def load_json_data(filepath: str):
    """
    Streams a large JSON array using ijson to avoid loading
    the entire file into memory at once.
    """
    items = []
    count = 0

    with open(filepath, "r", encoding="utf-8") as f:
        for item in ijson.items(f, "item"):
            items.append(item)
            count += 1

            if count % 100 == 0:
                print(f"Loaded {count} conversations...")

    print(f"Finished loading {count} conversations.")
    return items


def preprocess_data(data):
    """
    Aggregates all tweets belonging to the same conversation
    into a single document.
    """
    rows = []

    for conv in data:
        conv_id = conv.get("conversationId")
        text_buffer = []

        for thread in conv.get("threads", []):
            for tweet in thread.get("tweets", []):
                text_buffer.append(tweet.get("text", ""))

        rows.append({
            "conversationId": conv_id,
            "text": " ".join(text_buffer)
        })

    df = pd.DataFrame(rows)
    print(f"Prepared {len(df)} conversation-level documents.")
    return df



# Stopword handling
def build_stopwords():
    """
    Constructs a multilingual stopword list using stopwordsiso
    and extends it with common Twitter-specific noise tokens.
    """
    multilingual_stopwords = set()

    for lang in stopwords.langs():
        multilingual_stopwords |= stopwords.stopwords(lang)

    twitter_specific = {
        "rt", "via", "amp", "http", "https", "grok", "elon"
    }

    multilingual_stopwords |= twitter_specific
    return list(multilingual_stopwords)



# Topic modeling 
def topic_analysis_and_save(df: pd.DataFrame):
    """
    Trains a BERTopic model and saves:
    - the trained model
    - topic-level summaries
    - lightweight keyword-based annotations
    - conversation-to-topic assignments
    """
    texts = df["text"].fillna("").tolist()

    embedding_model = SentenceTransformer(MODEL_NAME)
    vectorizer_model = CountVectorizer(
        stop_words=build_stopwords()
    )

    topic_model = BERTopic(
        embedding_model=embedding_model,
        vectorizer_model=vectorizer_model,
        min_topic_size=MIN_TOPIC_SIZE,
        language="multilingual"
    )

    topics, probs = topic_model.fit_transform(texts)
    df["topic"] = topics
    df["topic_prob"] = probs

    # Saves trained model
    model_dir = os.path.join(OUTPUT_DIR, "bertopic_model")
    topic_model.save(model_dir)
    print(f"Saved BERTopic model to {model_dir}")

    # Saves topic summary table
    topic_info = topic_model.get_topic_info()
    topic_summary_path = os.path.join(OUTPUT_DIR, "topic_summary.csv")
    topic_info.to_csv(topic_summary_path, index=False, encoding="utf-8-sig")
    print(f"Saved topic summary to {topic_summary_path}")

    # Building per-topic annotation table
    annotation_rows = []

    for topic_id in topic_info["Topic"]:
        if topic_id == -1:
            continue  # skip outlier cluster

        topic_words = topic_model.get_topic(topic_id) or []
        top_keywords = [w for w, _ in topic_words[:K_KEYWORDS]]

        label = (
            f"{topic_id}: " + "_".join(top_keywords)
            if top_keywords else f"{topic_id}:"
        )

        reps = topic_model.get_representative_docs(topic_id)
        example_doc = reps[0] if reps else ""

        annotation_rows.append({
            "topic": topic_id,
            "label_k5": label,
            "keywords_top10": ", ".join(
                w for w, _ in topic_words[:10]
            ),
            "example_doc": example_doc
        })

    annotations_df = (
        pd.DataFrame(annotation_rows)
        .sort_values("topic")
    )

    annotations_path = os.path.join(
        OUTPUT_DIR, "topic_annotations_k5.csv"
    )
    annotations_df.to_csv(
        annotations_path,
        index=False,
        encoding="utf-8-sig"
    )
    print(f"Saved topic annotations to {annotations_path}")

    # Saves conversation-level topic assignments
    assignments_path = os.path.join(
        OUTPUT_DIR, "conversation_topic_assignments.csv"
    )
    df[["conversationId", "topic", "topic_prob"]].to_csv(
        assignments_path,
        index=False,
        encoding="utf-8-sig"
    )
    print(f"Saved conversation-topic assignments to {assignments_path}")

    return topic_model, df



# main section
if __name__ == "__main__":
    data = load_json_data(DATA_PATH)
    df = preprocess_data(data)

    df["text"] = df["text"].apply(clean_tweet)

    print("Starting topic modeling pipeline...")
    topic_model, df_topics = topic_analysis_and_save(df)
    print("Pipeline completed successfully.")
