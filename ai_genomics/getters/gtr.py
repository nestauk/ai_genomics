from ai_genomics import PROJECT_DIR, bucket_name as BUCKET_NAME, logger
from ai_genomics.getters.data_getters import load_s3_data
from ai_genomics.pipeline.gtr.make_gtr_projects import GTR_OUTPUTS_DIR, GTR_PROJ_NAME
from typing import Mapping, Union
import pandas as pd

GTR_INPUTS_DIR = PROJECT_DIR / "inputs/data/gtr"


def get_gtr_from_s3(entity_type: str) -> pd.DataFrame:
    """Get Gateway to Research data from S3

    Args:
        entity_type: Which GtR dataset to load,
            for example "organisations" or "persons"

    Returns:
        Dataframe of specified GtR data
    """
    return pd.DataFrame.from_dict(
        load_s3_data(BUCKET_NAME, f"inputs/gtr/gtr_projects-{entity_type}.json")
    )


def get_ai_genomics_gtr_data(entity_type: str) -> pd.DataFrame:
    """Get AI and Genomics GtR data

    Args:
        entity_type: "projects" or "publications"

    Returns:
        Dataframe of AI and Genomics GtR projects or publications
    """
    if entity_type not in ["projects", "publications"]:
        raise ValueError("entity_type should be 'projects' or 'publications'")

    try:
        return pd.read_json(GTR_INPUTS_DIR / f"gtr_ai_genomics_{entity_type}.json")
    except ValueError as e:
        logger.error(
            "ValueError: To create the missing file, run ai_genomics/analysis/gtr_definitions.py"
        )
        raise e


def get_ai_genomics_project_table(local: bool = True) -> pd.DataFrame:
    """Get AI and Genomics GtR project information

    Args:
        local: if the data is stored locally

    Returns:
        Dataframe of AI and Genomics GtR projects
    """

    if local:
        return pd.read_csv(GTR_OUTPUTS_DIR / GTR_PROJ_NAME)

    else:
        return load_s3_data("ai-genomics", f"outputs/gtr/{GTR_PROJ_NAME}")


def get_gtr_entities() -> Mapping[str, Mapping[str, Union[str, str]]]:
    """From S3 loads ai genomics gtr entities"""
    return load_s3_data(
        BUCKET_NAME,
        "outputs/entity_extraction/gtr_lookup_clean.json",
    )
