"""Functions to instantiate Google BigQuery client and
clean BigQuery results.
"""
from google.oauth2.service_account import Credentials
from google.cloud import bigquery
import os
from ai_genomics import logger
from typing import List
import pandas as pd
import numpy as np
import string

GENOMICS_AI_FIELDS = (
    "publication_number, application_number, cpc.code as cpc_code, "
    "title_localized.text as title_text, title_localized.language as title_language, "
    "abstract_localized.text as abstract_text, abstract_localized.language as abstract_language, "
    "publication_date, filing_date, grant_date, priority_date, inventor, assignee, entity_status "
)

DATE_COLS = ["publication_date", "grant_date", "filing_date", "priority_date"]

AI_KEYWORDS = ["machine learning", "artificial intelligence", "neural network"]

GOOD_AI_CPC_CODES = [
    "Y10S706/902",
    "Y10S706/908",
    "Y10S706/916",
    "Y10S706/919",
    "Y10S706/92",
    "Y10S706/921",
    "Y10S706/922",
    "Y10S706/923",
    "Y10S706/932",
    "Y10S706/934",
    "G16B40/20",
    "G16B40/30",
    "G06V10/762",
    "G06V10/7635",
    "G06V10/764",
    "G06V10/77",
    "G06V10/86",
]

def clean_code_definition(code_text: str) -> str:
    """Cleans CPC code definitions by:
    - lowercasing;
    - replacing values;
    - removing {};
    """
    return code_text.replace("\r", "").lower().translate(str.maketrans("", "", "{}"))


def make_keywords_regex_pattern(keywords: List[str]) -> str:
    """Makes regex pattern given a list of keywords or phrases

    Example:
        make_keywords_regex_pattern(['genome', 'dna']) -> '\\bgenome\\b|\\bdna\\b'
    """
    return "|".join(f"\\b{k}\\b" for k in keywords)


def convert_list_of_codes_to_string(list_of_codes: List[str]) -> str:
    """Converts list of relevant CPC codes to BigQuery-compliant
    string.
    """
    return "'" + "', '".join(list_of_codes) + "'"


def est_conn():
    """Instantiate Google BigQuery client to query patent data."""

    if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        google_creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

        credentials = Credentials.from_service_account_file(google_creds)

        client = bigquery.Client(credentials=credentials)

        return client

    else:
        logger.exception(
            "export GOOGLE_APPLICATION_CREDENTIALS directory path as global variable."
        )


def replace_missing_values_with_nans(ai_genomics_patents: pd.DataFrame) -> pd.DataFrame:
    """Replace missing values in the AI in
    genomics patents dataset with NaNs"""
    return ai_genomics_patents.replace(
        {date_col: 0 for date_col in DATE_COLS},
        np.nan,
    ).mask(ai_genomics_patents.applymap(str).eq("[]"))


def convert_date_columns_to_datetime(ai_genomics_patents: pd.DataFrame) -> pd.DataFrame:
    """Convert date columns to datetime format
    in the AI in genomics patents dataset"""
    for col in DATE_COLS:
        ai_genomics_patents[col] = pd.to_datetime(
            ai_genomics_patents[col], format="%Y%m%d", errors="ignore"
        )
    return ai_genomics_patents
