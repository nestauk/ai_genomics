"""
Util functions to create subgraphs by timeslice, cluster timeslices and propagate
cluster labels across timeslices to make communities comparable across time.
"""
####
import networkx as nx
from typing import Dict, List
import leidenalg as la
import igraph as ig
import numpy as np
import random
import statistics
from collections import Counter
import itertools
from sklearn.feature_extraction.text import TfidfVectorizer


def get_tfidf_top_features(documents: list, n_top: int):
    """get top n features using tfidf."""
    tfidf_vectorizer = TfidfVectorizer(stop_words="english")
    tfidf = tfidf_vectorizer.fit_transform(documents)
    importance = np.argsort(np.asarray(tfidf.sum(axis=0)).ravel())[::-1]
    tfidf_feature_names = np.array(tfidf_vectorizer.get_feature_names_out())
    return tfidf_feature_names[importance[:n_top]]


def get_subgraph_cluster_nodes(G_timeslice, cluster: str) -> List:
    """helper function to get cluster nodes per subgraph."""
    return [
        node
        for node, node_info in G_timeslice.nodes(data=True)
        if node_info["timeslice cluster number"] == cluster
    ]


def get_subgraph_clusters(G_timeslice) -> List:
    """helper function to get ordered list of all subgraph
    clusters from largest to smallest."""
    subgraph_cluster = [
        node[1] for node in G_timeslice.nodes(data="timeslice cluster number")
    ]
    return [node[0] for node in Counter(subgraph_cluster).most_common()]


def jaccard_similarity(list1: List, list2: List) -> float:
    """helper function to calculate the jaccard similarity between
    two lists."""
    s1 = set(list1)
    s2 = set(list2)
    return float(len(s1.intersection(s2)) / len(s1.union(s2)))


def add_cluster_names(subgraph_communities: dict, n_top: int) -> Dict:
    """
    Generate tf-idf of each cluster at latest time point
    and assign cluster name across all time slices for consistency.
    Args:
        subgraph_communities (Dict): A dictionary where the keys refer to timeslices
        and the values are undirected networkx subgraphs with timeslice cluster
        group node attribute.
    Returns:
        subgraph_communities (Dict): A dictionary where the keys refer to timeslices
        and the values are undirected networkx subgraphs with timeslice cluster
        group and cluster name attributes.
    """
    community_names = dict()
    for subgraph in subgraph_communities.values():
        communities = get_subgraph_clusters(subgraph)
        for community in communities:
            node_names = [
                x
                for x, y in subgraph.nodes(data=True)
                if y["timeslice cluster number"] == community
            ]
            community_names[community] = "-".join(get_tfidf_top_features(node_names, 3))

    for subgraph in subgraph_communities.values():
        node_cluster_names = dict(
            zip(
                list(subgraph.nodes()),
                [
                    community_names[subgraph.nodes[n]["timeslice cluster number"]]
                    for n in subgraph.nodes
                ],
            )
        )
        nx.set_node_attributes(subgraph, node_cluster_names, "timeslice cluster name")

    return subgraph_communities


def add_cluster_colors(subgraph_communities: dict) -> Dict:
    """Generates 6 digit HEX color codes per cluster per subgraph and appends HEX colors
    as 'cluster color' node attribute to subgraphs.
    Args:
        subgraph_communities (Dict): A dictionary where the keys refer to timeslices
        and the values are undirected networkx subgraphs with timeslice cluster
        group node attributes.
    Returns:
           subgraph_communities (Dict): A dictionary where the keys refer to timeslices
        and the values are undirected networkx subgraphs with timeslice cluster
        group and cluster color node attributes.
    """
    cluster_numbers = [
        list(
            set(
                [
                    node_info["timeslice cluster number"]
                    for node, node_info in subgraph.nodes(data=True)
                ]
            )
        )
        for subgraph in subgraph_communities.values()
    ]
    all_clusters = list(set(itertools.chain(*cluster_numbers)))
    hex_colors = [
        "#%06x" % random.randint(0, 0xFFFFFF) for _ in range(len(all_clusters))
    ]
    cluster_colors = dict(zip(all_clusters, hex_colors))

    for subgraph in subgraph_communities.values():
        node_cluster_colors = dict(
            zip(
                list(subgraph.nodes()),
                [
                    cluster_colors[subgraph.nodes[n]["timeslice cluster number"]]
                    for n in subgraph.nodes
                ],
            )
        )

        nx.set_node_attributes(subgraph, node_cluster_colors, "timeslice cluster color")

    return subgraph_communities


def timeslice_pair_coo_graph(G, timeslice_interval: int, min_timeslice: int) -> Dict:
    """
    Creates timesliced ent-pair co-occurance subgraphs every X year interval.
    Args:
        G (Graph): ent pair cooccurance graph with time based edge attributes.
        timeslice_interval (int): timeslice interval in years.
    Returns:
        G_timeslices (Dict): A dictionary where keys refer to timeslices and
        the values refer to ent pair cooccurance subgraphs.
    """
    pairs_first_published = [e["first_introduced"] for u, v, e in G.edges(data=True)]

    G_timeslices = dict()
    for i, timeslice in enumerate(
        range(
            min_timeslice,
            max(pairs_first_published) + timeslice_interval,
            timeslice_interval,
        )
    ):
        subgraph_edges = [
            (u, v)
            for u, v, e in G.edges(data=True)
            if e["first_introduced"] <= timeslice
        ]

        # subgraph induced by specified edges
        G_timeslices["G_timeslice_" + str(i)] = nx.Graph(
            nx.edge_subgraph(G, subgraph_edges)
        )

    return G_timeslices


def cluster_timeslice_pair_coo_graph(G_timeslices: dict):
    """
    Clusters timesliced ent-pair co-occurance subgraphs every X year interval
    using the leiden algorithm.
    Args:
        G_timeslices (Dict): A dictionary where keys refer to timeslices and
        the values refer to ent pair cooccurance subgraphs.
    Returns:
        subgraph_communities (Dict): A dictionary where keys refer to timeslices and
        the values refer to ent pair cooccurance subgraphs w/
        a node cluster attributes.
    """
    subgraph_communities = dict()

    for timeslice, subgraph in G_timeslices.items():
        subgraph_igraph = ig.Graph.from_networkx(subgraph)
        partitions = la.find_partition(subgraph_igraph, la.ModularityVertexPartition)

        for node in range(len(subgraph_igraph.vs)):
            subgraph_igraph.vs["cluster number"] = partitions.membership
        subgraph_communities[timeslice] = subgraph_igraph.to_networkx()

    # add timeslice cluster node attribute per subgraph
    for timestamp, subgraph_community in subgraph_communities.items():
        for node in subgraph_community.nodes(data=True):
            node[1]["timeslice cluster number"] = (
                timestamp + "_" + str(node[1]["cluster number"])
            )

    return subgraph_communities


def sanitise_clusters(timeslice_x, timeslice_y, min_jaccard_score):
    """
    Enforces cluster label consistency across timeslices greedily
    based on maximum jaccard similarity above threshold.
    Args:
        timeslice_x (Graph): ent pair cooccurance subgraph at timeslice x
        timeslice_y (Graph): ent pair cooccurance subgraphs at timeslice y (x + 1)
    """
    subgraph_clusters = [
        get_subgraph_clusters(subgraph) for subgraph in (timeslice_x, timeslice_y)
    ]
    cluster_perms = list(itertools.product(subgraph_clusters[0], subgraph_clusters[1]))

    perm_dists = []
    for cluster_perm in cluster_perms:
        timeslice_x_nodes, timeslice_y_nodes = (
            get_subgraph_cluster_nodes(timeslice_x, cluster_perm[0]),
            get_subgraph_cluster_nodes(timeslice_y, cluster_perm[1]),
        )
        dists = jaccard_similarity(timeslice_x_nodes, timeslice_y_nodes)
        if (dists != 0) & (dists > min_jaccard_score):
            perm_dists.append((cluster_perm, dists))

    sorted_perm_dists = sorted(perm_dists, key=lambda x: x[1], reverse=True)

    while len(sorted_perm_dists) > 0:
        most_similar_clusts = sorted_perm_dists[0]
        # update labels in timeslice y
        for node in timeslice_y.nodes(data=True):
            if node[1]["timeslice cluster number"] == most_similar_clusts[0][1]:
                node[1]["timeslice cluster number"] = most_similar_clusts[0][0]

        # remove perms
        clusters_to_remove = list(most_similar_clusts[0])
        for i, perm_dist in enumerate(sorted_perm_dists):
            if (
                perm_dist[0][0] in clusters_to_remove
                or perm_dist[0][1] in clusters_to_remove
            ):
                sorted_perm_dists.pop(i)
