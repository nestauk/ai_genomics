from typing import Dict

from ai_genomics import bucket_name
from ai_genomics.getters.data_getters import load_s3_data


def get_entity_cluster_lookup(ai_only: bool = False) -> Dict[str, Dict[int, str]]:
    """Get patent and OpenAlex document clusters.

    Args:
        ai_only (bool, optional): If True, loads clusters for a narrow subset of
        AI papers only. Defaults to False.

    Returns:
        Dict[str, Dict[int, str]]: Mapping of data sources ('oa' or 'pat') to a sub
            mapping of cluster IDs to lists of document IDs belonging to that cluster.
    """
    subset = "ai" if ai_only else "all"
    fname = f"outputs/data/cluser/doc_{subset}_clusters.json"
    return load_s3_data(bucket_name, fname)
