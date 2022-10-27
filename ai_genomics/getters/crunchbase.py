from ai_genomics import PROJECT_DIR, logger, bucket_name

import pandas as pd
from typing import Mapping, Union

from ai_genomics.pipeline.crunchbase_data.make_crunchbase_data import (
    CB_COMP_PATH,
    CB_COMP_NAME,
)

from ai_genomics.getters.data_getters import load_s3_data


def get_ai_genomics_crunchbase_org_ids() -> pd.DataFrame:
    """Returns dataframe of AI and Genomics crunchbase organisation ids"""
    try:
        return pd.read_csv(
            PROJECT_DIR / "inputs/data/crunchbase/ai_genomics_org_ids.csv",
        )
    except FileNotFoundError as e:
        logger.error(
            "FileNotFoundError: To create the missing file, run ai_genomics/analysis/crunchbase_definitions.py"
        )
        raise e


def get_ai_genomics_crunchbase_orgs(local: bool = True) -> pd.DataFrame:
    """Reads a table with information about AI, Genomics or AI and genomics companies

    Args:
        local: If True, read from local file. If False, read from s3 bucket.

    """

    if local:
        return pd.read_csv(CB_COMP_PATH)
    else:
        return load_s3_data("ai-genomics", f"outputs/crunchbase/{CB_COMP_NAME}")


def get_crunchbase_entities() -> Mapping[str, Mapping[str, Union[str, str]]]:
    """From S3 loads ai genomics cb entities"""
    return load_s3_data(
        bucket_name,
        "outputs/entity_extraction/cb_lookup_clean.json",
    )


def get_crunchbase_ai_genomics_entity_groups(k: int = 500) -> pd.DataFrame:
    """Gets a dataframe of vectors representing the presence of DBpedia entity
    clusters in each document.

    Args:
        k (int, optional): The number of clusters. Defaults to 500.

    Returns:
        pd.DataFrame: A sparse dataframe where the index is company IDs and
            the columns are vector dimensions (entity cluster IDs).
    """
    fname = f"inputs/entities/crunchbase_entity_group_vectors_k_{k}.csv"
    return load_s3_data(bucket_name, fname)


def get_crunchbase_ai_genomics_description_embeddings() -> pd.DataFrame:
    """Gets an array of description embeddings and the associated company IDs.

    Returns:
       pd.DataFrame: Description embeddings and the associated company IDs.
    """
    fname = "inputs/embedding/pat_embeddings.csv"
    return load_s3_data(bucket_name, fname)
