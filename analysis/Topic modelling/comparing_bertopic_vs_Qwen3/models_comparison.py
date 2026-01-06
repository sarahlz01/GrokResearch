"""

Topic Analysis Comparison Script


This script performs the following tasks:

- Loads topic annotations for two topic modeling approaches:
   - BERTopic
   - Qwen

- Loads conversation-to-topic mappings and conversation embeddings.

- Produces summary statistics for topic sizes in each model and L2-normalized topic centroids and aligns BERTopic topics
   to Qwen topics using cosine similarity of embeddings.
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from numpy.linalg import norm


# Annotation CSVs 
BERT_ANN_CSV = "outputs/topics_annotations_gemini_no_outliers.csv"
QWEN_ANN_CSV = "qwen/topic_annotations_qwen_gemini.csv"

# Conversation to topic mapping CSVs
BERT_MAP_CSV = "outputs/conversation_topic_assignment.csv"
QWEN_MAP_CSV = "qwen/conversation_topic_mapping_qwen.csv"

# Saved conversation embeddings 
QWEN_EMB_NPY = "qwen/qwen_embeddings.npy"

# Outputs
OUT_STATS_CSV = "outputs/topic_size_summary_stats.csv"
OUT_ALIGN_CSV = "outputs/bertopic_qwen_alignment.csv"

# Visualization outputs
OUT_LOG_HIST_PNG   = "outputs/topic_size_log_histogram.png"
OUT_ECDF_PNG       = "outputs/topic_size_ecdf.png"
OUT_BOXPLOT_PNG    = "outputs/topic_size_boxplot.png"
OUT_STATS_TABLE_PNG = "outputs/topic_size_summary_table.png"

# Alignment settings
TOP_BERTOPIC_THEMES = 5  # Number of top BERT topics to align
TOP_QWEN_MATCHES = 3     # Top matching Qwen topics per BERT topic

# HELPER FUNCTIONS
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Strip whitespace from column names."""
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    return df

def pick_col(df: pd.DataFrame, candidates):
    """Select the first column from candidates that exists in df."""
    for c in candidates:
        if c in df.columns:
            return c
    return None

def compute_size_stats(df: pd.DataFrame, counts_col: str) -> dict:
    """Compute summary statistics for topic sizes."""
    x = pd.to_numeric(df[counts_col], errors="coerce").fillna(0).astype(float)
    return {
        "#topics": int(len(x)),
        "mean": float(x.mean()),
        "median": float(x.median()),
        "std": float(x.std(ddof=1)) if len(x) > 1 else 0.0,
        "min": float(x.min()),
        "max": float(x.max()),
        "p90": float(x.quantile(0.90)),
        "p99": float(x.quantile(0.99)),
    }

def l2_normalize_rows(X: np.ndarray) -> np.ndarray:
    """L2-normalize each row in a matrix to unit length."""
    denom = np.maximum(norm(X, axis=1, keepdims=True), 1e-12)
    return X / denom

def topic_centroids(embeddings: np.ndarray, topic_ids: np.ndarray) -> dict:
    """Compute L2-normalized centroid vector for each topic."""
    centroids = {}
    for t in np.unique(topic_ids):
        t = int(t)
        if t == -1:
            continue
        idx = np.where(topic_ids == t)[0]
        if idx.size == 0:
            continue
        c = embeddings[idx].mean(axis=0)
        c = c / max(norm(c), 1e-12)
        centroids[t] = c
    return centroids

def cosine(a: np.ndarray, b: np.ndarray) -> float:
    """Compute cosine similarity between two vectors."""
    return float(np.dot(a, b) / max(norm(a) * norm(b), 1e-12))

def ecdf(x: np.ndarray):
    """Compute empirical cumulative distribution function."""
    x = np.sort(x)
    y = np.arange(1, len(x) + 1) / len(x)
    return x, y

def summary_stats(x: np.ndarray) -> dict:
    """Compute summary stats for numeric array."""
    return {
        "topics": int(len(x)),
        "mean": float(np.mean(x)),
        "median": float(np.median(x)),
        "p90": float(np.quantile(x, 0.90)),
        "p99": float(np.quantile(x, 0.99)),
        "max": float(np.max(x)),
        "min": float(np.min(x)),
    }


# Loading and unifying all annotaions
bert_ann = normalize_columns(pd.read_csv(BERT_ANN_CSV, encoding="utf-8-sig"))
qwen_ann = normalize_columns(pd.read_csv(QWEN_ANN_CSV, encoding="utf-8-sig"))

def unify_ann(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize annotation columns across different sources."""
    topic_col  = pick_col(df, ["topic", "Topics", "Topic"])
    counts_col = pick_col(df, ["Counts", "count", "counts", "Count", "Nr_Documents"])
    name_col   = pick_col(df, ["Topic_name", "topic_name", "Name", "name", "label_k5"])
    kw_col     = pick_col(df, ["keywords_top10", "Keywords", "keywords", "Top_Keywords"])
    rep_col    = pick_col(df, ["Representative_Docs", "Representative_Doc", "example_doc", "example", "rep_doc"])

    missing = [("topic", topic_col), ("Counts", counts_col), ("Topic_name", name_col),
               ("keywords_top10", kw_col), ("Representative_Docs", rep_col)]
    missing = [k for k, v in missing if v is None]
    if missing:
        raise RuntimeError(f"Missing columns in annotations: {missing}\nFound: {df.columns.tolist()}")

    out = pd.DataFrame({
        "topic": df[topic_col],
        "Counts": df[counts_col],
        "Topic_name": df[name_col],
        "keywords_top10": df[kw_col],
        "Representative_Docs": df[rep_col],
    })
    out["topic"] = pd.to_numeric(out["topic"], errors="coerce").fillna(-1).astype(int)
    out["Counts"] = pd.to_numeric(out["Counts"], errors="coerce").fillna(0).astype(int)
    return out

bert_ann_u = unify_ann(bert_ann)
qwen_ann_u = unify_ann(qwen_ann)


# topic size stats 
stats_df = pd.DataFrame([
    {"model": "BERTopic", **compute_size_stats(bert_ann_u, "Counts")},
    {"model": "Qwen", **compute_size_stats(qwen_ann_u, "Counts")},
])
stats_df.to_csv(OUT_STATS_CSV, index=False, encoding="utf-8-sig")
print("\n=== Topic Size Stats ===")
print(stats_df.to_string(index=False))
print(f"Saved topic size stats: {OUT_STATS_CSV}")

#mapping and embeddings to be loaded
bert_map = normalize_columns(pd.read_csv(BERT_MAP_CSV, encoding="utf-8-sig"))
qwen_map = normalize_columns(pd.read_csv(QWEN_MAP_CSV, encoding="utf-8-sig"))

b_cid   = pick_col(bert_map, ["conversationId", "conversation_id", "ConversationId"])
b_topic = pick_col(bert_map, ["topic", "Topics", "Topic"])
q_cid   = pick_col(qwen_map, ["conversationId", "conversation_id", "ConversationId"])
q_topic = pick_col(qwen_map, ["topic", "Topics", "Topic"])

if any(v is None for v in [b_cid, b_topic, q_cid, q_topic]):
    raise RuntimeError(
        "Mapping CSVs must contain conversationId and topic.\n"
        f"BERTopic cols found: {bert_map.columns.tolist()}\n"
        f"Qwen cols found: {qwen_map.columns.tolist()}"
    )

bert_map_u = pd.DataFrame({
    "conversationId": bert_map[b_cid].astype(str),
    "bert_topic": pd.to_numeric(bert_map[b_topic], errors="coerce").fillna(-1).astype(int)
})

qwen_map_u = pd.DataFrame({
    "conversationId": qwen_map[q_cid].astype(str),
    "qwen_topic": pd.to_numeric(qwen_map[q_topic], errors="coerce").fillna(-1).astype(int)
})

emb = np.load(QWEN_EMB_NPY).astype(np.float32)
if emb.shape[0] != len(qwen_map_u):
    raise RuntimeError(
        f"Embeddings rows ({emb.shape[0]}) do not match Qwen mapping rows ({len(qwen_map_u)})."
    )
emb = l2_normalize_rows(emb)

# Merging datasets fr comaprison
joined = qwen_map_u.copy()
joined["row_idx"] = np.arange(len(joined))
joined = joined.merge(bert_map_u, on="conversationId", how="inner")
joined_emb = emb[joined["row_idx"].to_numpy()]

bert_centroids = topic_centroids(joined_emb, joined["bert_topic"].to_numpy())
qwen_centroids = topic_centroids(joined_emb, joined["qwen_topic"].to_numpy())


# BERTopic and  Qwen alignment via centroid cosine similarity

top_bert_topics = (
    bert_ann_u[bert_ann_u["topic"] != -1]
    .sort_values("Counts", ascending=False)
    .head(TOP_BERTOPIC_THEMES)["topic"]
    .tolist()
)

bert_name   = dict(zip(bert_ann_u["topic"], bert_ann_u["Topic_name"]))
qwen_name   = dict(zip(qwen_ann_u["topic"], qwen_ann_u["Topic_name"]))
bert_counts = dict(zip(bert_ann_u["topic"], bert_ann_u["Counts"]))
qwen_counts = dict(zip(qwen_ann_u["topic"], qwen_ann_u["Counts"]))

rows = []
qwen_topics_list = sorted(qwen_centroids.keys())

for bt in top_bert_topics:
    if bt not in bert_centroids:
        continue

    bc = bert_centroids[bt]
    sims = [(qt, cosine(bc, qwen_centroids[qt])) for qt in qwen_topics_list]
    sims.sort(key=lambda x: x[1], reverse=True)
    best = sims[:TOP_QWEN_MATCHES]

    rows.append({
        "BERTopic_topic": bt,
        "BERTopic_Counts": int(bert_counts.get(bt, 0)),
        "BERTopic_Topic_name": str(bert_name.get(bt, "")),
        "Top_Qwen_matches": " | ".join([f"{qt} (cos={sim:.3f}, n={int(qwen_counts.get(qt,0))})" for qt, sim in best]),
        "Top_Qwen_names": " | ".join([str(qwen_name.get(qt, "")) for qt, _ in best]),
    })

align_df = pd.DataFrame(rows)
align_df.to_csv(OUT_ALIGN_CSV, index=False, encoding="utf-8-sig")
print("\n=== BERTopic → Qwen Alignment ===")
print(align_df.to_string(index=False))
print(f"Saved alignment table: {OUT_ALIGN_CSV}")


#Vizualisations
B_PLOT = bert_ann_u.loc[bert_ann_u["topic"] != -1, "Counts"].astype(float).to_numpy()
Q_PLOT = qwen_ann_u.loc[qwen_ann_u["topic"] != -1, "Counts"].astype(float).to_numpy()
B_PLOT = B_PLOT[B_PLOT > 0]
Q_PLOT = Q_PLOT[Q_PLOT > 0]

# Log-scale histogram
plt.figure(figsize=(10, 6))
bins = np.logspace(np.log10(max(1, min(B_PLOT.min(), Q_PLOT.min()))),
                   np.log10(max(B_PLOT.max(), Q_PLOT.max())), 45)
plt.hist(B_PLOT, bins=bins, alpha=0.6, label="BERTopic")
plt.hist(Q_PLOT, bins=bins, alpha=0.6, label="Qwen")
plt.xscale("log")
plt.xlabel("Conversations per topic (log scale)")
plt.ylabel("Number of topics")
plt.title("Topic Size Distribution (Log-Scaled Histogram)")
plt.legend()
plt.grid(True, which="both", alpha=0.25)
plt.tight_layout()
plt.savefig(OUT_LOG_HIST_PNG, dpi=220)
print(f"Saved plot: {OUT_LOG_HIST_PNG}")

#  ECDF plot
bx, by = ecdf(B_PLOT)
qx, qy = ecdf(Q_PLOT)
plt.figure(figsize=(10, 6))
plt.plot(bx, by, label="BERTopic")
plt.plot(qx, qy, label="Qwen")
plt.xscale("log")
plt.xlabel("Conversations per topic (log scale)")
plt.ylabel("Fraction of topics ≤ x")
plt.title("ECDF of Topic Sizes")
plt.legend()
plt.grid(True, which="both", alpha=0.25)
plt.tight_layout()
plt.savefig(OUT_ECDF_PNG, dpi=220)
print(f"Saved plot: {OUT_ECDF_PNG}")

# 4.3 Boxplot
plt.figure(figsize=(8, 6))
plt.boxplot([B_PLOT, Q_PLOT], labels=["BERTopic", "Qwen"], showfliers=False)
plt.yscale("log")
plt.ylabel("Conversations per topic (log scale)")
plt.title("Topic Size Comparison (Boxplot, Outliers Hidden)")
plt.grid(True, which="both", axis="y", alpha=0.25)
plt.tight_layout()
plt.savefig(OUT_BOXPLOT_PNG, dpi=220)
print(f"Saved plot: {OUT_BOXPLOT_PNG}")

#Summary statistics table
bS = summary_stats(B_PLOT)
qS = summary_stats(Q_PLOT)

plt.figure(figsize=(10, 3.8))
plt.axis("off")
rows = [
    ["# topics", f"{bS['topics']:,}", f"{qS['topics']:,}"],
    ["min", f"{bS['min']:.0f}", f"{qS['min']:.0f}"],
    ["median", f"{bS['median']:.1f}", f"{qS['median']:.1f}"],
    ["mean", f"{bS['mean']:.1f}", f"{qS['mean']:.1f}"],
    ["p90", f"{bS['p90']:.1f}", f"{qS['p90']:.1f}"],
    ["p99", f"{bS['p99']:.1f}", f"{qS['p99']:.1f}"],
    ["max", f"{bS['max']:.0f}", f"{qS['max']:.0f}"],
]

table = plt.table(
    cellText=rows,
    colLabels=["Metric", "BERTopic", "Qwen"],
    cellLoc="left",
    colLoc="left",
    loc="center",
)
table.auto_set_font_size(False)
table.set_fontsize(11)
table.scale(1, 1.5)
plt.title("Topic Size Summary Statistics", pad=16)
plt.tight_layout()
plt.savefig(OUT_STATS_TABLE_PNG, dpi=220)
print(f"Saved summary table: {OUT_STATS_TABLE_PNG}")
