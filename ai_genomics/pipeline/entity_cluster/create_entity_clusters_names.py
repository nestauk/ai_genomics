"""This script generates look ups for entity clusters 
between their entity cluster IDs and their top n TF-IDF name. 

Insures name uniquenss by adding a number at the end of any duplicate
cluster name. 
"""
from ai_genomics.utils.text import get_top_terms
from ai_genomics.getters.entities import get_entity_cluster_lookup
from ai_genomics.getters.data_getters import save_to_s3

from collections import defaultdict, Counter
from ai_genomics import get_yaml_config, PROJECT_DIR, bucket_name

CONFIG = get_yaml_config(PROJECT_DIR / "ai_genomics/config/entity_cluster.yaml")

if __name__ == "__main__":

    entity_cluster_lookup_agg = defaultdict(list)
    unique_cluster_name_lookup = dict()

    for k in "100", "200", "500", "1000":
        entity_cluster_lookup = get_entity_cluster_lookup(
            CONFIG["cluster"][f"k_{k}"]["n_clusters"]
        )
        for key, value in sorted(entity_cluster_lookup.items()):
            entity_cluster_lookup_agg[value].append(key)

        cluster_name_lookup = dict()
        for cluster_id, entity_list in dict(entity_cluster_lookup_agg).items():
            cluster_name_lookup[cluster_id] = get_top_terms(entity_list)
        cluster_name_counts = Counter(cluster_name_lookup.values()).most_common()
        # insure uniqueness of cluster name
        for cluster_name, cluster_count in cluster_name_counts:
            if cluster_count > 1:
                name_mapper_indxs = [
                    list(cluster_name_lookup.keys())[i]
                    for i, j in enumerate(list(cluster_name_lookup.values()))
                    if j == cluster_name
                ]
                unique_names = [
                    f"{cluster_name}-{str(i)}" for i in range(cluster_count)
                ]
                unique_cluster_name_lookup.update(
                    (dict(zip(name_mapper_indxs, unique_names)))
                )
            unique_cluster_name_lookup[
                list(cluster_name_lookup.keys())[
                    list(cluster_name_lookup.values()).index(cluster_name)
                ]
            ] = cluster_name
        save_to_s3(
            bucket_name,
            unique_cluster_name_lookup,
            f"inputs/entities/entity_groups_names_k_{k}.json",
        )
