"""
This script adds the full list of CPC and IPC codes per patent number to patents datasets in S3.
"""
import re
from ai_genomics.getters.data_getters import save_to_s3
from ai_genomics import logger, bucket_name
import pandas as pd
from typing import List
from ai_genomics.getters.patents import (
    get_ai_genomics_patents,
    get_ai_sample_patents,
    get_genomics_sample_patents,
)
from ai_genomics.utils.patents import est_conn, convert_list_of_codes_to_string

AI_GENOMICS_PATENTS = get_ai_genomics_patents()
AI_SAMPLE_PATENTS = get_ai_sample_patents()
GENOMICS_SAMPLE_PATENTS = get_genomics_sample_patents()


def add_full_cpc_ipc_codes(
    conn,
    patents_df: pd.DataFrame,
) -> pd.DataFrame:
    """Adds full list of CPC and IPC codes per patent publication number.
    Args:
        patents_df: The patents dataframe to add full list of CPC and IPC codes to
        conn: Google BigQuery coonection
    Returns:
        full_cpc_ipc: The patents dataframe with a full list of CPC and IPC codes
    """
    publication_numbers_formatted = convert_list_of_codes_to_string(
        patents_df.publication_number
    )

    full_cpc_ipc_q = (
        "select publication_number, cpc.code as cpc_code, ipc.code as ipc_code "
        "from patents-public-data.patents.publications, "
        "UNNEST(cpc) cpc, UNNEST(ipc) ipc "
        f"WHERE publication_number in ({publication_numbers_formatted})"
    )

    full_cpc_ipc_codes = conn.query(full_cpc_ipc_q).to_dataframe()

    logger.info(
        "pulled full cpc and ipc codes per patent publication number from Google BigQuery."
    )

    full_cpc_ipc_codes_agg = (
        full_cpc_ipc_codes.groupby("publication_number").agg(set).reset_index()
    )

    patents_full_codes = (
        pd.merge(full_cpc_ipc_codes_agg, patents_df, on="publication_number")
        .drop(["cpc_code_y", "ipc_code_y"], axis=1)
        .rename(columns={"cpc_code_x": "cpc_codes", "ipc_code_x": "ipc_codes"})
    )

    return patents_full_codes


if __name__ == "__main__":

    # establish conn
    conn = est_conn()

    ai_genomics_full_codes = add_full_cpc_ipc_codes(conn, AI_GENOMICS_PATENTS)
    ai_sample_full_codes = add_full_cpc_ipc_codes(conn, AI_SAMPLE_PATENTS)
    genomics_sample_full_codes = add_full_cpc_ipc_codes(conn, GENOMICS_SAMPLE_PATENTS)

    for table_name, table in zip(
        ("ai_genomics", "ai_sample", "genomics_sample"),
        (ai_genomics_full_codes, ai_sample_full_codes, genomics_sample_full_codes),
    ):
        full_table_name = f"inputs/patent_data/processed_patent_data/{table_name}_patents_cpc_ipc_codes.csv"
        save_to_s3(bucket_name, table, full_table_name)
