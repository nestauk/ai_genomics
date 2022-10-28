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


def get_ai_genomics_patents_entities() -> Mapping[str, Mapping[str, Union[str, str]]]:
    """From S3 loads AI in genomics patents entities"""
    return load_s3_data(
        bucket_name,
        "outputs/entity_extraction/ai_genomics_patents_lookup_clean.json",
    )


def get_ai_patents_entities() -> Mapping[str, Mapping[str, Union[str, str]]]:
    """From S3 loads AI patents entities"""
    return load_s3_data(
        bucket_name,
        "outputs/entity_extraction/ai_patents_lookup_clean.json",
    )


def get_genomics_patents_entities() -> Mapping[str, Mapping[str, Union[str, str]]]:
    """From S3 loads genomics patents entities"""
    return load_s3_data(
        bucket_name,
        "outputs/entity_extraction/genomics_patents_lookup_clean.json",
    )


def get_patent_ai_genomics_entity_groups(k: int = 500) -> pd.DataFrame:
    """Gets a dataframe of vectors representing the presence of DBpedia entity
    clusters in each document.

    Args:
        k (int, optional): The number of clusters. Defaults to 500.

    Returns:
        pd.DataFrame: A sparse dataframe where the index is patent IDs and
            the columns are vector dimensions (entity cluster IDs).
    """
    fname = f"inputs/entities/patent_entity_group_vectors_k_{k}.csv"
    return load_s3_data(bucket_name, fname)


def get_patent_ai_genomics_abstract_embeddings() -> pd.DataFrame:
    """Gets an array of abstract embeddings and the associated publication IDs.

    Returns:
       pd.DataFrame: Abstract embeddings and the associated publication IDs.
    """
    fname = "inputs/embedding/pat_ai_genomics_embeddings.csv"
    return load_s3_data(bucket_name, fname)
