from typing import Dict

from ai_genomics import bucket_name
from ai_genomics.getters.data_getters import load_s3_data


def get_entity_cluster_lookup(k: int = 500) -> Dict[str, int]:
    """Gets a lookup between DBpedia entities and their entity cluster IDs.

    Args:
        k (int, optional): The number of clusters. Defaults to 500.

    Returns:
        Dict[str, int]: A lookup where keys are entity names and values are
            cluster IDs
    """
    fname = f"inputs/entities/entity_groups_k_{k}.json"
    return load_s3_data(bucket_name, fname)
