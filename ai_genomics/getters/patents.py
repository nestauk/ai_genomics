from ai_genomics import bucket_name
from ai_genomics.getters.data_getters import load_s3_data
import pandas as pd
from typing import Mapping, Union


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
        bucket_name,
        "inputs/patent_data/processed_patent_data/ai_genomics_patents_cpc_ipc_codes.csv",
    )

def get_ai_genomics_cpc_codes() -> Mapping[str, Mapping[str, Union[str, str]]]:
    """From S3 loads AI in genomics cpc codes"""
    return load_s3_data(
        bucket_name, "outputs/patent_data/class_codes/cpc_with_definitions.json"
    )


def get_ai_genomics_ipc_codes() -> Mapping[str, Mapping[str, Union[str, str]]]:
    """From S3 loads AI in genomics ipc codes without Google BigQuery ipc code formatting"""
    return load_s3_data(
        bucket_name, "outputs/patent_data/class_codes/ipc_with_definitions.json"
    )


def get_ai_genomics_ipc_codes_formatted() -> Mapping[
    str, Mapping[str, Union[str, str]]
]:
    """From S3 loads AI in genomics ipc codes with Google BigQuery ipc code formatting"""
    return load_s3_data(
        bucket_name,
        "outputs/patent_data/class_codes/ipc_formatted_with_definitions.json",
    )
