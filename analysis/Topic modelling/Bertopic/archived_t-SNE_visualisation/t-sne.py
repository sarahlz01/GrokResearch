"""
visualize_topics_tsne.py

This script generates a 2D t-SNE visualization of document-level topic
assignments produced by a previously trained BERTopic model.

Design choices:
- Visualization is kept separate from training to keep the pipeline modular.
- We reuse stored embeddings from the BERTopic model to ensure consistency.
- t-SNE is used instead of UMAP for a more classic, interpretable layout
  that HR reviewers are often familiar with.

Expected inputs:
- A saved BERTopic model directory (from topic_model.save)
- The conversation-topic assignment CSV for reference

Outputs:
- A static PNG scatter plot showing topic structure in embedding space
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from bertopic import BERTopic
from sklearn.manifold import TSNE


#config
MODEL_DIR = "outputs/bertopic_model"
ASSIGNMENTS_PATH = "outputs/conversation_topic_assignments.csv"
OUTPUT_DIR = "outputs"

TSNE_RANDOM_STATE = 42
TSNE_PERPLEXITY = 30
TSNE_ITER = 1000

os.makedirs(OUTPUT_DIR, exist_ok=True)


# -----------------------------
# Load model and data
# -----------------------------
print("Loading BERTopic model...")
topic_model = BERTopic.load(MODEL_DIR)

print("Loading topic assignments...")
assignments_df = pd.read_csv(ASSIGNMENTS_PATH)

# BERTopic stores document embeddings internally after fit_transform
embeddings = topic_model.embeddings_

if embeddings is None:
    raise ValueError(
        "No embeddings found in the BERTopic model. "
        "Make sure the model was trained with an embedding model."
    )

topics = assignments_df["topic"].values


#Running T-sne
print("Running t-SNE dimensionality reduction...")
tsne = TSNE(
    n_components=2,
    perplexity=TSNE_PERPLEXITY,
    n_iter=TSNE_ITER,
    random_state=TSNE_RANDOM_STATE,
    init="pca",
    learning_rate="auto"
)

embeddings_2d = tsne.fit_transform(embeddings)


#plot
print("Generating plot...")
plt.figure(figsize=(12, 8))

scatter = plt.scatter(
    embeddings_2d[:, 0],
    embeddings_2d[:, 1],
    c=topics,
    s=10,
    alpha=0.7
)

plt.title("t-SNE Visualization of BERTopic Document Clusters")
plt.xlabel("t-SNE Dimension 1")
plt.ylabel("t-SNE Dimension 2")

# Creates a compact legend for topics (excluding outliers)
unique_topics = sorted(t for t in np.unique(topics) if t != -1)
handles = [
    plt.Line2D([0], [0], marker='o', linestyle='', markersize=6)
    for _ in unique_topics
]
labels = [f"Topic {t}" for t in unique_topics]

plt.legend(
    handles,
    labels,
    title="Topics",
    bbox_to_anchor=(1.05, 1),
    loc="upper left",
    borderaxespad=0.0
)

plt.tight_layout()

output_path = os.path.join(OUTPUT_DIR, "tsne_topic_visualization.png")
plt.savefig(output_path, dpi=300)
plt.close()

print(f"t-SNE plot saved to: {output_path}")
