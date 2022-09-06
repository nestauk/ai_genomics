import json
import pandas as pd
from typing import Dict

from ai_genomics import bucket_name as BUCKET_NAME
from ai_genomics.getters.data_getters import load_s3_data
from ai_genomics.pipeline.patent_data.cpc_lookup import CPC_LOOKUP_PATH



def get_ai_genomics_patents() -> pd.DataFrame:
    """From S3 loads dataframe of AI in genomics patents
    with columns such as:
        - application_number
        - publication_number
        - full list of cpc codes
        - full list of ipc codes
        - abstract_text
        - publication_date
        - inventor
        - assignee
    """
    return load_s3_data(
        BUCKET_NAME,
        "inputs/patent_data/processed_patent_data/ai_genomics_patents_cpc_ipc_codes.csv",
    )


def get_cpc_lookup() -> Dict:
    """Loads a lookup between CPC codes and their descriptions and parent
    codes.
    """
    with open(CPC_LOOKUP_PATH, "r") as f:
        return json.load(f)
