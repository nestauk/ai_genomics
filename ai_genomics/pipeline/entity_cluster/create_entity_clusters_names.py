"""This script generates look ups for entity clusters 
between their entity cluster IDs and their top n TF-IDF name. 

Insures name uniquenss by adding a number at the end of any duplicate
cluster name. 

python ai_genomics/pipeline/entity_cluster/create_entity_clusters_names.py
"""
from ai_genomics.utils.text import get_top_terms
from ai_genomics.getters.entities import (
    get_entity_cluster_lookup,
    get_evolved_entity_cluster_lookup,
)
from ai_genomics.getters.data_getters import save_to_s3

from collections import defaultdict, Counter
from ai_genomics import get_yaml_config, PROJECT_DIR, bucket_name
from typing import Dict, List

CONFIG = get_yaml_config(PROJECT_DIR / "ai_genomics/config/entity_cluster.yaml")
EVOLVED_CLUSTERS = get_evolved_entity_cluster_lookup()


def get_final_entity_list(
    cluster: str, evolved_clusters: Dict[str, List[str]] = EVOLVED_CLUSTERS
):
    """Helper function get entities list at latest timestamp for a given cluster"""
    final_year = max(
        [
            int(year)
            for year, clust_info in evolved_clusters.items()
            if clust_info.get(cluster)
        ]
    )
    return evolved_clusters[str(final_year)][cluster]


def make_cluster_names_unique(cluster_name_lookup: Dict[str, str]) -> Dict[str, str]:
    """Insures uniqueness of name in cluster name lookup where
        key is cluster ID and value is cluster name using top 3 TF-IDF
        terms by adding number after duplicate TF-IDF name
    
    Returns unique cluster name lookup 
    """
    unique_cluster_name_lookup = dict()
    cluster_name_counts = Counter(cluster_name_lookup.values()).most_common()
    for cluster_name, cluster_count in cluster_name_counts:
        if cluster_count > 1:
            name_mapper_indxs = [
                list(cluster_name_lookup.keys())[i]
                for i, j in enumerate(list(cluster_name_lookup.values()))
                if j == cluster_name
            ]
            unique_names = [f"{cluster_name}-{str(i)}" for i in range(cluster_count)]
            unique_cluster_name_lookup.update(
                (dict(zip(name_mapper_indxs, unique_names)))
            )
        unique_cluster_name_lookup[
            list(cluster_name_lookup.keys())[
                list(cluster_name_lookup.values()).index(cluster_name)
            ]
        ] = cluster_name
    return unique_cluster_name_lookup


if __name__ == "__main__":

    entity_cluster_lookup_agg = defaultdict(list)
    for k in "100", "200", "500", "1000":
        entity_cluster_lookup = get_entity_cluster_lookup(
            CONFIG["cluster"][f"k_{k}"]["n_clusters"]
        )
        for key, value in sorted(entity_cluster_lookup.items()):
            entity_cluster_lookup_agg[value].append(key)

        cluster_name_lookup = dict()
        for cluster_id, entity_list in dict(entity_cluster_lookup_agg).items():
            cluster_name_lookup[cluster_id] = get_top_terms(entity_list)
        unique_cluster_name_lookup = make_cluster_names_unique(cluster_name_lookup)
        save_to_s3(
            bucket_name,
            unique_cluster_name_lookup,
            f"inputs/entities/entity_groups_names_k_{k}.json",
        )

    # do the same for evolved clusters
    all_clusters = []
    for timestamped_cluster in list(EVOLVED_CLUSTERS.values()):
        all_clusters.extend(list(timestamped_cluster.keys()))
    all_clusters = list(set(all_clusters))

    evolved_cluster_name_mapper = dict()
    for cluster in all_clusters:
        final_entity_list = get_final_entity_list(cluster)
        cluster_name = get_top_terms(final_entity_list)
        evolved_cluster_name_mapper[cluster] = cluster_name

    unique_evolved_cluster_name_mapper = make_cluster_names_unique(
        evolved_cluster_name_mapper
    )
    save_to_s3(
        bucket_name,
        unique_evolved_cluster_name_mapper,
        "outputs/analysis/tag_evolution/dbpedia_clusters_timeslice_names.json",
    )
