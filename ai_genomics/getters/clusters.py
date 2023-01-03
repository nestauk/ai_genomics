from pandas import read_csv
from turtle import pd
from typing import Dict
from botocore.exceptions import ClientError

from ai_genomics import PROJECT_DIR
from ai_genomics import bucket_name
from ai_genomics.getters.data_getters import load_s3_data, get_s3_dir_files


def get_doc_cluster_lookup(
    ai_only: bool = False, min_year: int = 2012, max_year: int = 2021,
) -> Dict[str, Dict[int, str]]:
    """Get patent and OpenAlex document clusters.

    Args:
        ai_only (bool, optional): If True, loads clusters for a narrow subset of
            AI papers only. Defaults to False.
        min_year (int): Load clusters with docs from, and including, this year.
        max_year (int): Load clusters with docs up to, and including, this year.

    Note: by setting the arguments above, you assume or know that there must be an
    exact cluster file matching those parameters.

    Returns:
        Dict[str, Dict[int, str]]: Mapping of data sources ('oa' or 'pat') to a sub
            mapping of cluster IDs to lists of document IDs belonging to that cluster.
    """
    subset = "ai" if ai_only else "all"
    fname = f"outputs/data/cluster/doc_{subset}_{min_year}_{max_year}_clusters.json"

    try:
        return load_s3_data(bucket_name, fname)
    except ClientError as ex:
        code = ex.response["Error"]["Code"]
        if code == "NoSuchKey":
            files = get_s3_dir_files(bucket_name, "outputs/cluster")
            print(f"{code}: Files available are:")
            for f in files:
                print(f)
        else:
            raise


def get_doc_cluster_names(
    ai_only: bool = False, min_year: int = 2012, max_year: int = 2021,
) -> Dict[int, str]:
    """Get document cluster names generated through tf-idf of entity counts.

    Args:
        ai_only (bool, optional): If True, loads clusters for a narrow subset of
            AI papers only. Defaults to False.
        min_year (int): Load clusters with docs from, and including, this year.
        max_year (int): Load clusters with docs up to, and including, this year.

    Returns:
        Dict[int, str]: Lookup between cluster ID and document names. (Top 25 tf-idf terms)
    """
    subset = "ai" if ai_only else "all"
    fname = (
        f"outputs/data/cluster/doc_{subset}_{min_year}_{max_year}_clusters_names.json"
    )
    return load_s3_data(bucket_name, fname)


def get_doc_cluster_interp() -> Dict[int, str]:
    """Returns interpretable doc cluster names"""

    cluster_names = "inputs/data/cluster_names.csv"

    if (PROJECT_DIR / cluster_names).exists():
        data = read_csv(f"{PROJECT_DIR}/{cluster_names}")
    data = load_s3_data(bucket_name, cluster_names)

    return data.set_index("cluster")["name"].to_dict()


def get_id_cluster_lookup() -> Dict[int, str]:
    """Parses the cluster lookup so each ID is mapped to a cluster name" """

    cl_names = get_doc_cluster_interp()

    return {
        str(_id): cl_names[int(cl)]
        for source, cl_assgn in get_doc_cluster_lookup().items()
        for cl, id_list in cl_assgn.items()
        for _id in id_list
    }
