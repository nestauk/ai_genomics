"""Script to instantiate Google BigQuery client and
functions to create bespoke genomics patents Google BigQuery
tables in local project.
"""
######################################
from google.cloud import bigquery
from google.oauth2 import service_account
import os

######################################


def est_conn():
    """Instantiate Google BigQuery client
    to query patent data.
    """
    if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        google_creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

        credentials = service_account.Credentials.from_service_account_file(
            google_creds
        )

        client = bigquery.Client(credentials=credentials)

        return client

    else:
        print(
            "export GOOGLE_APPLICATION_CREDENTIALS directory path as global variable."
        )


def clean_ipc_codes(genomics_codes: dict) -> list:
    """Prepares IPC codes in format to query Google BigQuery.

    Inputs:
        genomics_codes (dict): Dictionary of patent classification codes relevant to genomics.
    Outputs:
        ipc_codes_clean (list): List of clean IPC codes relevant to genomics.
    """
    ipc_codes_clean = []

    if "ipc" in genomics_codes.keys():
        for ipc_code in genomics_codes["ipc"].keys():
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
    else:
        print("no IPC code keys in genomics_codes dict!")


def make_table_query(
    class_sys: str, classification_codes: list
) -> str:  # used to create bespoke genomics patent BigQuery tables
    """Queries for BigQuery console to generate new tables.

    Inputs:
        classification_codes (list): list of classification codes.
        class_sys: classification system to query.

    Outputs:
        q (str): string to query BigQuery console with.
    """

    class_str = "'" + "', '".join(classification_codes) + "'"

    q = (
        f"SELECT application_number "
        f"FROM `patents-public-data.patents.publications`, UNNEST({class_sys}) AS {class_sys}__u "
        f"WHERE {class_sys}__u.code IN ({class_str})"
    )

    return q
