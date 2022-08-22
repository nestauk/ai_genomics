from ai_genomics import bucket_name as BUCKET_NAME
import pandas as pd
from ai_genomics.getters.data_getters import load_s3_data


def get_ai_genomics_patents() -> pd.DataFrame:
    """From S3 loads dataframe of AI in genomics patents
    with columns such as:
        - application_number
        - publication_number
        - abstract_text
        - publication_date
        - inventor
        - assignee
    """
    return load_s3_data(
        BUCKET_NAME,
        "inputs/patent_data/processed_patent_data/ai_genomics_patents_cpc_ipc_codes.csv",
    )
