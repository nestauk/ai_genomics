from ai_genomics import PROJECT_DIR, logger
import os
import pandas as pd

from ai_genomics.pipeline.crunchbase_data.crunchbase_data import (
    CB_COMP_PATH,
    CB_COMP_NAME,
)

from ai_genomics.utils.reading import fetch_s3


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


def get_ai_genomics_crunchbase_companies(local: bool = True) -> pd.DataFrame:
    """Reads a table with information about AI, Genomics or AI and genomics companies

    Args:
        local: If True, read from local file. If False, read from s3 bucket.

    """

    if local:
        return pd.read_csv(CB_COMP_PATH)
    else:
        return fetch_s3(f"outputs/crunchbase/{CB_COMP_NAME}")
