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

GENOMICS_AI_FIELDS = (
    "publication_number, application_number, cpc.code as cpc_code, ipc.code as ipc_code, "
    "title_localized.text as title_text, title_localized.language as title_language, "
    "abstract_localized.text as abstract_text, abstract_localized.language as abstract_language, "
    "publication_date, filing_date, grant_date, priority_date, inventor, assignee, entity_status "
)

DATE_COLS = ["publication_date", "grant_date", "filing_date", "priority_date"]

AI_KEYWORDS = ["machine learning", "artificial intelligence", "neural network"]

GENOMICS_KEYWORDS = ["genome", "dna", "gene", "genetic"]

GOOD_GENOMICS_CPC_CODES = ["C12Q1/6869", "G16B20/20", "G16B5/20"]

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

BAD_GENOMICS_CPC_CODES = [
    "H01J49/0409",
    "H04L9/0866",
    "C12P",
    "G10H2220/386",
    "C12C2200/01",
    "A01H1/06",
    "A01K11/003",
    "A01K2217/15",
    "A01K2267/0306",
    "A23C19/0326",
    "A23C2220/202",
    "A23V2300/21",
    "C40B10/00",
    "C40B40/08",
    "G05B23/0229",
    "G05B2219/32091",
    "G05B2219/32333",
    "G05B2219/33041",
    "G05B2219/35041",
    "G05B2219/40384",
    "G05B2219/40473",
    "G05B2219/42145",
    "G05B2219/42147",
    "G06F2111/06",
    "G06K7/1482",
    "G06K9/6229",
    "G06N3/086",
    "G06N3/126",
    "G10H2250/011",
    "G10K2210/3029",
    "G10L25/39",
    "G01N23/20",
    "G06N3/12",
    "G06N3/123",
    "G06N3/00",
    "G06N3/002",
    "C12N2015/8536",
    "C12N15/00",
    "C12M35/00" "C12N15/8241",
    "C12N15/8213",
    "C12N15/8201",
    "C12N15/113",
    "C12N15/8241",
    "C12M35/00",
    "C12",
    "A61K2800/86",
    "C07K",
]

BAD_GENOMICS_IPC_CODES = [
    "G06N0003120000",
    "G10L0025390000",
    "G10L0025390000",
    "G06F0111060000",
    "C12",
]


def clean_code_definition(code_text: str) -> str:
    """Cleans IPC/CPC code definitions by:
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
    """Converts list of relevant IPC and CPC codes to BigQuery-compliant
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
        {date_col: 0 for date_col in DATE_COLS}, np.nan,
    ).mask(ai_genomics_patents.applymap(str).eq("[]"))


def convert_date_columns_to_datetime(ai_genomics_patents: pd.DataFrame) -> pd.DataFrame:
    """Convert date columns to datetime format
    in the AI in genomics patents dataset"""
    for col in DATE_COLS:
        ai_genomics_patents[col] = pd.to_datetime(
            ai_genomics_patents[col], format="%Y%m%d", errors="ignore"
        )
    return ai_genomics_patents


def clean_ipc_codes(genomics_codes: List[str]) -> List[str]:
    """Prepares IPC codes by replacing '000' with '/' to match Google BigQuery format.

    Args:
        genomics_codes (list): List of patent classification codes relevant to genomics.

    Returns:
        ipc_codes_clean (list): List of clean IPC codes relevant to genomics.
    """
    ipc_codes_clean = []
    for ipc_code in genomics_codes:
        if len(ipc_code) == 14:
            if ipc_code[6] == "0":
                if ipc_code[10:] != "0000":
                    ipc_codes_clean.append(
                        ipc_code[:4]
                        + ipc_code[7:8]
                        + "/"
                        + ipc_code[8 : len(ipc_code) - 1]
                    )
                else:
                    ipc_codes_clean.append(
                        ipc_code[:4] + ipc_code[7:8] + "/" + ipc_code[8:10]
                    )
            else:
                ipc_codes_clean.append(
                    ipc_code[:4] + ipc_code[6:8] + "/" + ipc_code[8:10]
                )
        else:
            ipc_codes_clean.append(ipc_code + "/")

    return ipc_codes_clean
