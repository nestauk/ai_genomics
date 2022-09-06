from ai_genomics import bucket_name
from ai_genomics.getters.data_getters import load_s3_data
from ai_genomics.pipeline.patent_data.cpc_lookup import CPC_LOOKUP_PATH
import pandas as pd
from typing import Mapping, Union, Dict
import json



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


def get_ai_sample_patents() -> pd.DataFrame:
    """From S3 loads dataframe of a sample of AI patents (random 10%)
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
        "inputs/patent_data/processed_patent_data/ai_sample_patents_cpc_ipc_codes.csv",
    )


def get_genomics_sample_patents() -> pd.DataFrame:
    """From S3 loads dataframe of a sample of genomics patents (random 3%)
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
        "inputs/patent_data/processed_patent_data/genomics_sample_patents_cpc_ipc_codes.csv",
    )


def get_ai_genomics_cpc_codes() -> Mapping[str, Mapping[str, Union[str, str]]]:
    """From S3 loads AI in genomics cpc codes"""
    return load_s3_data(
        bucket_name, "outputs/patent_data/class_codes/cpc_with_definitions.json"
    )


def get_ai_genomics_ipc_codes() -> Mapping[str, Mapping[str, Union[str, str]]]:
    """From S3 loads AI in genomics ipc codes WITHOUT Google BigQuery ipc code formatting"""
    return load_s3_data(
        bucket_name, "outputs/patent_data/class_codes/ipc_with_definitions.json"
    )


def get_ai_genomics_ipc_codes_formatted() -> Mapping[
    str, Mapping[str, Union[str, str]]
]:
    """From S3 loads AI in genomics ipc codes WITH Google BigQuery ipc code formatting"""
    return load_s3_data(
        bucket_name,
        "outputs/patent_data/class_codes/ipc_formatted_with_definitions.json",
    )


def get_cpc_lookup() -> Dict:
    """Loads a lookup between CPC codes and their descriptions and parent
    codes.
    """
    with open(CPC_LOOKUP_PATH, "r") as f:
        return json.load(f)
