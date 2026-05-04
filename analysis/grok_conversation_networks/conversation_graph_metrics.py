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

# Average out-degree centrality (directed): the mean out-degree normalized by the maximum
# possible out-degree (n-1)
#  Equivalent to (sum(out_deg(v))/n) / (n-1). Returns 0.0 if n <= 1
def avg_out_degree(G_d: nx.DiGraph) -> float:
    n = G_d.number_of_nodes()
    if n <= 1:
        return 0.0
    avg_out = sum(dict(G_d.out_degree()).values()) / n
    return avg_out / (n - 1)

# Unweighted reciprocity (directed): fraction of directed edges (u->v) that have the reverse
# edge (v->u) present. Counts edges (not unordered pairs), so mutual dyads contribute twice
# (once for each direction) Returns 0.0 if the graph has no edges

def reciprocity(G_d: nx.DiGraph) -> float:
    m = G_d.number_of_edges()
    if m == 0:
        return 0.0
    mutual = 0
    for u, v in G_d.edges():
        if G_d.has_edge(v, u):
            mutual += 1
    return mutual / m


# Consistent reciprocity (directed, weighted): consider each unordered pair {u,v} and count it
# as "bidirectional" if both u->v and v->u exist. Among bidirectional pairs, count it as
# "consistent" if both edge weights are >= min_weight. The metric is:
#  consistent_pairs / bidirectional_pairs
# where bidirectional_pairs counts unordered pairs with both directions present at all.
# Returns 0.0 if there are no bidirectional pairs
def consistent_reciprocity(G_d: nx.DiGraph, min_weight: int = 2) -> float:
    mutual_pairs = 0
    bidir_pairs = 0
    seen = set()

    for u, v in G_d.edges():
        if u == v:
            continue
        key = tuple(sorted((u, v)))
        if key in seen:
            continue
        seen.add(key)

        if G_d.has_edge(u, v) and G_d.has_edge(v, u):
            bidir_pairs += 1
            w_uv = G_d[u][v].get("weight", 1)
            w_vu = G_d[v][u].get("weight", 1)
            if w_uv >= min_weight and w_vu >= min_weight:
                mutual_pairs += 1

    if bidir_pairs == 0:
        return 0.0
    return mutual_pairs / bidir_pair

"""
Transitivity (undirected): global clustering coefficient defined as
3 * (# of triangles) / (# of connected triples)
(equivalently, fraction of connected triples that are closed) Uses networkx.transitivity.
Returns 0.0 if the graph has fewer than 3 nodes
"""
def transitivity(G_u: nx.Graph) -> float:
    if G_u.number_of_nodes() < 3:
        return 0.0
    return nx.transitivity(G_u)
    

"""
Grok-focused back-and-forth per conversation
For conversations that include Grok, it measures how much users go back-and-forth with Grok.
all grok nodes are aggregated  in this conversation (usually 1)
"""
def grok_back_and_forth_stats(G_d: nx.DiGraph, grok_nodes: Iterable[str], min_weight: int = 2) -> Dict[str, Any]:
    grok_nodes = list(grok_nodes)
    if not grok_nodes:
        return {
            "has_grok": False,
            "users_interacting_with_grok": 0,
            "mutual_users_with_grok": 0,
            "mutual_rate_with_grok": 0.0,
            "consistent_mutual_users_with_grok": 0,
            "consistent_mutual_rate_with_grok": 0.0,
        }

    counterparts: Dict[str, Dict[str, int]] = {}

    for g in grok_nodes:
        if g not in G_d:
            continue

        # users to grok
        for u in G_d.predecessors(g):
            w = G_d[u][g].get("weight", 1)
            counterparts.setdefault(u, {"to_grok": 0, "from_grok": 0})
            counterparts[u]["to_grok"] += w

        # grok to users
        for v in G_d.successors(g):
            w = G_d[g][v].get("weight", 1)
            counterparts.setdefault(v, {"to_grok": 0, "from_grok": 0})
            counterparts[v]["from_grok"] += w

    # Removing grok nodes from counterparts if they got included
    for g in grok_nodes:
        counterparts.pop(g, None)

    total = len(counterparts)
    mutual = sum(1 for u, d in counterparts.items() if d["to_grok"] > 0 and d["from_grok"] > 0)
    consistent = sum(1 for u, d in counterparts.items() if d["to_grok"] >= min_weight and d["from_grok"] >= min_weight)

    return {
        "has_grok": True,
        "users_interacting_with_grok": total,
        "mutual_users_with_grok": mutual,
        "mutual_rate_with_grok": (mutual / total) if total else 0.0,
        "consistent_mutual_users_with_grok": consistent,
        "consistent_mutual_rate_with_grok": (consistent / total) if total else 0.0,
    }

def avg_degree_centrality_grok(G_u: nx.Graph, grok_nodes: Iterable[str]) -> float:
    """
    Average degree centrality for Grok nodes only

    For each Grok node g:
        degree_centrality(g) = deg(g) / (n - 1)
    Then average across Grok nodes present in the graph

    Returns 0.0 if no grok nodes or n <= 1
    """
    grok_nodes = [g for g in grok_nodes if g in G_u]
    n = G_u.number_of_nodes()
    if n <= 1 or not grok_nodes:
        return 0.0

    return sum(G_u.degree(g) / (n - 1) for g in grok_nodes) / len(grok_nodes)

def main():
    rows = []
    processed = 0

    for conv in stream_conversations(DATA_PATH):
        G_u, G_d, meta = build_graphs_for_conversation(conv)

        #  5 metrics per conversation
        m = {
            "conversationId": meta["conversationId"],
            "n_nodes": G_u.number_of_nodes(),
            "n_edges_undirected": G_u.number_of_edges(),
            "n_edges_directed": G_d.number_of_edges(),
            "avg_degree_centrality": avg_degree_centrality(G_u),
            "avg_degree_centrality_grok": avg_degree_centrality_grok(G_u, meta.get("grok_nodes", [])),
            "avg_out_degree": avg_out_degree(G_d),
            "reciprocity": reciprocity(G_d),
            "consistent_reciprocity": consistent_reciprocity(
                G_d,
                min_weight=MIN_WEIGHT_FOR_CONSISTENT
            ),
            "transitivity": transitivity(G_u),
            "threads": meta["threads"],
            "tweets": meta["tweets"],
            "reply_steps": meta["reply_steps"],
            "missing_authorId": meta["missing_authorId"],
        }

        # Grok-specific back and forth
        grok_stats = grok_back_and_forth_stats(
            G_d,
            meta.get("grok_nodes", []),
            min_weight=MIN_WEIGHT_FOR_CONSISTENT
        )
        m.update(grok_stats)

        rows.append(m)
        processed += 1

        if processed % 500 == 0:
            print(f"Processed {processed} conversations...")

    df = pd.DataFrame(rows)
    out_csv = os.path.join(OUTPUT_DIR, "conversation_metrics.csv")
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"Saved per-conversation metrics to {out_csv}")

    # Dataset-level summary 
    summary = df[
        [
            "avg_degree_centrality",
            "avg_out_degree",
            "reciprocity",
            "consistent_reciprocity",
            "transitivity",
            "mutual_rate_with_grok",
            "consistent_mutual_rate_with_grok",
        ]
    ].describe().to_dict()

    out_json = os.path.join(OUTPUT_DIR, "dataset_summary.json")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"Saved dataset summary stats to {out_json}")


if __name__ == "__main__":
    main()