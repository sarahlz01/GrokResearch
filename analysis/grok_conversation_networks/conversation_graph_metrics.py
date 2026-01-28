import os
import json
import ijson
import networkx as nx
import pandas as pd
from typing import Dict, Any, Iterable, Tuple, Optional


DATA_PATH = "dataset.json"
OUTPUT_DIR = "outputs_graph_metrics"
MIN_WEIGHT_FOR_CONSISTENT = 2

# Identifying grok nodes
ASSISTANT_AUTHOR_NAMES = {"grok"}          
ASSISTANT_FLAG_FIELD = "isAssistant"       

os.makedirs(OUTPUT_DIR, exist_ok=True)



def stream_conversations(filepath: str) -> Iterable[Dict[str, Any]]:
   
    with open(filepath, "r", encoding="utf-8") as f:
        for conv in ijson.items(f, "item"):
            yield conv



def node_id_from_tweet(tweet: Dict[str, Any]) -> str:
    aid = str(tweet.get("authorId", "")).strip()
    if not aid:
        # fall back to authorName 
        return f"name:{str(tweet.get('authorName', '')).strip()}"
    return f"user:{aid}"

#Identifies grok either via isAssistant or authorName

def is_grok(tweet: Dict[str, Any]) -> bool:
    if tweet.get(ASSISTANT_FLAG_FIELD) is True:
        return True
    name = str(tweet.get("authorName", "")).strip().lower()
    return name in ASSISTANT_AUTHOR_NAMES



#Graphs 
# For a single conversation:
    #Undirected graph: who interacted with whom (ignores direction & counts)
    #Directed weighted graph: A->B weight = number of adjacency-reply steps
# edges are infered from adjacency within each thread: tweet[i] replies to tweet[i-1]
def build_graphs_for_conversation(conv: Dict[str, Any]) -> Tuple[nx.Graph, nx.DiGraph, Dict[str, Any]]:
    G_u = nx.Graph()
    G_d = nx.DiGraph()

    meta = {
        "conversationId": conv.get("conversationId"),
        "threads": 0,
        "tweets": 0,
        "reply_steps": 0,
        "missing_authorId": 0,
        "grok_nodes": set(),  # node ids that are grok in this conversation
    }

    threads = conv.get("threads", [])
    if not isinstance(threads, list):
        return G_u, G_d, meta

    for thread in threads:
        meta["threads"] += 1
        tweets = thread.get("tweets", [])
        if not isinstance(tweets, list) or len(tweets) == 0:
            continue

        meta["tweets"] += len(tweets)

        prev_node: Optional[str] = None

        for tw in tweets:
            if not isinstance(tw, dict):
                continue

            # Tracks which node(s) are grok in this conversation
            if is_grok(tw):
                meta["grok_nodes"].add(node_id_from_tweet(tw))

            node = node_id_from_tweet(tw)
            if node.startswith("name:") and node == "name:":
                meta["missing_authorId"] += 1
                continue

            # adds node so isolated speakers are counted
            G_u.add_node(node)
            G_d.add_node(node)

            # adjacency reply: current replies to previous
            if prev_node and prev_node != node:
                meta["reply_steps"] += 1

                # undirected interaction exists
                G_u.add_edge(node, prev_node)

                # directed weighted edge (node = prev_node)
                if G_d.has_edge(node, prev_node):
                    G_d[node][prev_node]["weight"] += 1
                else:
                    G_d.add_edge(node, prev_node, weight=1)

            prev_node = node

    meta["grok_nodes"] = list(meta["grok_nodes"])
    return G_u, G_d, meta

 # metrics 
# Average degree centrality (undirected): the mean node degree normalized by the maximum
# possible degree (n-1). Equivalent to (sum(deg(v))/n) / (n-1) Returns 0.0 if n <= 1
def avg_degree_centrality(G_u: nx.Graph) -> float:
    n = G_u.number_of_nodes()
    if n <= 1:
        return 0.0
    avg_deg = sum(dict(G_u.degree()).values()) / n
    return avg_deg / (n - 1)