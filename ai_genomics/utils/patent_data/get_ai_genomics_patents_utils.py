"""Functions to instantiate Google BigQuery client and
create bespoke genomics patents Google BigQuery
tables in local project.
"""
from google.oauth2.service_account import Credentials
from google.cloud import bigquery
import os

from ai_genomics.utils.error_utils import Error
from typing import List
from collections.abc import Iterable
import pandas as pd
import numpy as np

DATE_COLS = ["publication_date", "grant_date", "filing_date", "priority_date"]


def est_conn():
    """Instantiate Google BigQuery client to query patent data."""

    if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        google_creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

        credentials = Credentials.from_service_account_file(google_creds)

        client = bigquery.Client(credentials=credentials)

        return client

    else:
        raise Error(
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
            ai_genomics_patents[col], format="%Y%m%d"
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
