from typing import Dict, List

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

def get_entity_cluster_name_lookup(k: int = 500) -> Dict[str, str]:
    """Gets a lookup between DBpedia entity cluster IDs and their
        cluster name.

    Args:
        k (int, optional): The number of clusters. Defaults to 500.

    Returns:
        Dict[str, str]: A lookup where keys are cluster IDs and
            values are the cluster name
    """
    fname = f"inputs/entities/entity_groups_names_k_{k}.json"
    return load_s3_data(bucket_name, fname)

def get_evolved_entity_cluster_name_lookup() -> Dict[str, str]:
    """Gets a lookup between timestamped cluster IDs and their cluster
        name.

    Returns:
        Dict[str, str]: A lookup where keys are timestamped cluster IDs and
            values are the cluster name
    """
    return load_s3_data(bucket_name, NAME)

#add look up for evolved clusters
def get_evolved_entity_cluster_lookup() -> Dict[str, List[str]]:
    """Gets a lookup between timestamped cluster IDs and their
        DBpedia entities.

    Returns:
        Dict[str, List[str]]: A lookup where keys are timestamped 
            cluster IDs and values are a list of DPedia entities associated to the cluster.
    """
    return load_s3_data(bucket_name, NAME)

