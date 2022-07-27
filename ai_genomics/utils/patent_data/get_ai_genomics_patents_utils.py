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

def clean_ipc_codes(
    genomics_codes: List[str]
    ) -> List[str]:
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

def make_table_query(
    classification_codes: List[str], 
    class_sys: str
    ) -> str:
    """Generates queries for BigQuery console to generate new tables.

    Args:
        classification_codes (list): list of classification codes.
        class_sys: classification system to query.

    Returns:
        q (str): string to query BigQuery console with.
    """

    class_str = "'" + "', '".join(classification_codes) + "'"

    q = (
        f"SELECT publication_number, application_number, {clas_sys}__u.code "
        f"FROM `patents-public-data.patents.publications`, UNNEST({class_sys}) AS {class_sys}__u "
        f"WHERE {class_sys}__u.code IN ({class_str})"
    )

    return q