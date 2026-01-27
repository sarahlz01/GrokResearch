import os
import re
from collections import defaultdict

import ijson
import numpy as np
import pandas as pd
import torch

import stopwordsiso as stopwords
from sentence_transformers import SentenceTransformer
from sklearn.feature_extraction.text import TfidfVectorizer

import umap
import hdbscan
import joblib


# Config
DATA_PATH = "output_CLEANED (1).json"
OUTPUT_DIR = "qwen"
MODEL_NAME = "Qwen/Qwen3-Embedding-0.6B"

MAX_TOKENS = 512
MAX_CHARS = 20000
BATCH_SIZE = 8  

MIN_CLUSTER_SIZE = 10
TOP10_KEYWORDS = 10


ANNOTATIONS_OUT = os.path.join(OUTPUT_DIR, "topic_annotaion_prdered_qwen.csv")
MAPPING_OUT = os.path.join(OUTPUT_DIR, "conversation_topic_mapping_qwen.csv")

os.makedirs(OUTPUT_DIR, exist_ok=True)



# Stopwords

def build_stopwords():
    multi = set()
    for lang in stopwords.langs():
        multi |= set(stopwords.stopwords(lang))

    domain = {
        "grok", "elon", "musk", "x", "twitter", "rt", "via", "amp",
        "http", "https", "www", "com"
    }

    extra_common = {
        "the", "a", "an", "in", "on", "at", "of", "for", "to", "from", "by", "with",
        "about", "into", "over", "under", "between", "after", "before", "during",
        "through", "within", "without", "against", "around", "among",
        "and", "or", "but", "if", "then", "than", "so",
        "is", "are", "was", "were", "be", "been", "being",
        "it", "its", "this", "that", "these", "those",
        "i", "me", "my", "mine", "you", "your", "yours", "we", "our", "ours",
        "they", "them", "their", "theirs", "he", "him", "his", "she", "her", "hers",
        "not", "no", "yes", "do", "does", "did", "doing",
        "can", "could", "would", "should", "will", "just",
    }

    # Vectorizer lowercases by default, so lowercase stopwords
    return list({str(s).lower() for s in multi} | {s.lower() for s in domain} | {s.lower() for s in extra_common})


STOPWORDS = build_stopwords()



# Helper functions
def clean_text(text: str) -> str:
    if not isinstance(text, str):
        return ""
    text = re.sub(r"http\S+|www\S+|https\S+", "", text)
    text = re.sub(r"@\w+", "", text)
    text = re.sub(r"#", "", text)
    text = re.sub(r"\d+", "", text)
    text = re.sub(r"_", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def load_conversations_df(filepath: str) -> pd.DataFrame:
    rows = []
    count = 0
    with open(filepath, "r", encoding="utf-8") as f:
        for conv in ijson.items(f, "item"):
            cid = conv.get("conversationId")
            parts = []
            for thread in conv.get("threads", []) or []:
                for tweet in thread.get("tweets", []) or []:
                    parts.append(tweet.get("text", ""))
            text = clean_text(" ".join(parts))[:MAX_CHARS]
            rows.append({"conversationId": cid, "text": text})
            count += 1
            if count % 1000 == 0:
                print(f"Loaded {count} conversations...")
    print(f"\n Finished loading {len(rows)} conversations total.")
    return pd.DataFrame(rows)


def extract_keywords(texts, k=10):
    if len(texts) < 2:
        return []
    vec = TfidfVectorizer(
        stop_words=STOPWORDS,
        lowercase=True,
        strip_accents="unicode",
        ngram_range=(1, 2),
        min_df=2,
        max_df=0.30,      # drops terms appearing in >30% of cluster docs
        sublinear_tf=True,
        max_features=50000
    )
    X = vec.fit_transform(texts)
    if X.shape[1] == 0:
        return []
    scores = np.asarray(X.mean(axis=0)).ravel()
    terms = np.array(vec.get_feature_names_out())
    top = np.argsort(-scores)[:k]
    return terms[top].tolist()


def batched(iterable, n):
    for i in range(0, len(iterable), n):
        yield i, iterable[i:i+n]


def get_sentence_transformer_modules(st_model: SentenceTransformer):
    transformer = st_model[0]
    pooling = st_model[1]
    return transformer, pooling


def remap_topics_by_size(labels: np.ndarray):
    """
    Remap cluster labels so the largest cluster becomes 0, next 1, ...
    Noise stays -1
    Returns (new_labels, remap_dict, counts_dict).
    """
    labels = np.asarray(labels).astype(int)
    unique = np.unique(labels)
    valid = [int(t) for t in unique if int(t) != -1]
    counts = {t: int((labels == t).sum()) for t in valid}
    sorted_topics = sorted(valid, key=lambda t: counts[t], reverse=True)

    remap = {old: new for new, old in enumerate(sorted_topics)}
    remap[-1] = -1

    new_labels = np.array([remap[int(t)] for t in labels], dtype=int)
    new_counts = {remap[t]: counts[t] for t in valid}  # counts in new id space

    return new_labels, remap, new_counts



# Main
if __name__ == "__main__":
    df = load_conversations_df(DATA_PATH)
    texts = df["text"].fillna("").tolist()


    st_model = SentenceTransformer(
        MODEL_NAME,
        model_kwargs={"attn_implementation": "eager"}  # avoids SDPA spikes
    )

    transformer, pooling = get_sentence_transformer_modules(st_model)
    tokenizer = transformer.tokenizer
    device = st_model.device

    # Determines embedding dim
    with torch.no_grad():
        sample = tokenizer(
            ["hello"],
            padding=True,
            truncation=True,
            max_length=MAX_TOKENS,
            return_tensors="pt"
        )
        sample = {k: v.to(device) for k, v in sample.items()}
        out = transformer(sample)
        out = pooling(out)
        emb_dim = out["sentence_embedding"].shape[1]

    embeddings = np.zeros((len(texts), emb_dim), dtype=np.float32)

    print(f" Embedding dim: {emb_dim}")
    print(f" Using device: {device}")
    print(f" Hard max tokens: {MAX_TOKENS}")
    print(f" Batch size: {BATCH_SIZE}")

    # Encodes with HARD truncation
    st_model.eval()
    for start_idx, batch_texts in batched(texts, BATCH_SIZE):
        with torch.no_grad():
            features = tokenizer(
                batch_texts,
                padding=True,
                truncation=True,
                max_length=MAX_TOKENS,
                return_tensors="pt"
            )
            if features["input_ids"].shape[1] > MAX_TOKENS:
                raise RuntimeError(f"Tokenization exceeded MAX_TOKENS: {features['input_ids'].shape[1]}")

            features = {k: v.to(device) for k, v in features.items()}

            out = transformer(features)
            out = pooling(out)
            batch_emb = out["sentence_embedding"]
            batch_emb = torch.nn.functional.normalize(batch_emb, p=2, dim=1)
            batch_emb = batch_emb.detach().cpu().numpy().astype(np.float32)

        embeddings[start_idx:start_idx + len(batch_texts)] = batch_emb

        if (start_idx // BATCH_SIZE) % 200 == 0:
            done = start_idx + len(batch_texts)
            print(f" Embedded {done}/{len(texts)}")

    # Save embeddings
    np.save(os.path.join(OUTPUT_DIR, "qwen_embeddings.npy"), embeddings)
    print(f"\nFile Saved: {OUTPUT_DIR}/qwen_embeddings.npy  shape={embeddings.shape}")

    # Clusters
    umap_model = umap.UMAP(n_components=5, n_neighbors=15, metric="cosine", random_state=42)
    emb_red = umap_model.fit_transform(embeddings)

    clusterer = hdbscan.HDBSCAN(min_cluster_size=MIN_CLUSTER_SIZE)
    raw_labels = clusterer.fit_predict(emb_red)
    raw_probs = np.where(raw_labels == -1, 0.0, clusterer.probabilities_)

    #  Remap topics by size (BERTopic-like ordering) 
    labels, remap_dict, counts_dict = remap_topics_by_size(raw_labels)

    # probs stay aligned per-conversation (no change needed)
    probs = raw_probs

    # Saves cluster models and remap
    joblib.dump(
        {"umap": umap_model, "hdbscan": clusterer, "topic_remap": remap_dict},
        os.path.join(OUTPUT_DIR, "cluster_models.joblib")
    )
    print(f"💾 Saved: {OUTPUT_DIR}/cluster_models.joblib (includes topic_remap)")

    # conversation_topic_mapping_qwen.csv 
    pd.DataFrame({
        "conversationId": df["conversationId"],
        "topic": labels,
        "topic_prob": probs
    }).to_csv(MAPPING_OUT, index=False, encoding="utf-8-sig")
    print(f"File Saved: {MAPPING_OUT}")

    # topic_annotaion_prdered_qwen.csv 
    topic_to_indices = defaultdict(list)
    for i, t in enumerate(labels):
        topic_to_indices[int(t)].append(i)

    rows = []
    for t, idxs in topic_to_indices.items():
        if t == -1:
            continue

        cluster_texts = [texts[i] for i in idxs if texts[i]]
        keywords = extract_keywords(cluster_texts, k=TOP10_KEYWORDS)

        topic_name = f"{t}: " + "_".join(keywords[:5]) if keywords else f"{t}:"
        best_i = max(idxs, key=lambda i: probs[i])
        rep_doc = texts[best_i]

        rows.append({
            "Topics": t,  
            "Counts": len(idxs),
            "Topic_name": topic_name,
            "keywords_top10": ", ".join(keywords),
            "Representative_Docs": rep_doc
        })

    ann_df = pd.DataFrame(rows)
    ann_df = ann_df.sort_values(["Counts", "Topics"], ascending=[False, True])
    ann_df.to_csv(ANNOTATIONS_OUT, index=False, encoding="utf-8-sig")
    print(f"File Saved: {ANNOTATIONS_OUT}")

    print("\n DONE (topics remapped by size; ordered outputs saved).")
