"""
This script adds the full list of CPC and IPC codes per patent number to the AI and Genomics patents dataset saved in S3.
"""
from ai_genomics.getters.data_getters import load_s3_data, save_to_s3
from ai_genomics import logger, bucket_name as BUCKET_NAME
import pandas as pd
from typing import List

from ai_genomics.utils.patent_data.get_ai_genomics_patents_utils import est_conn
from ai_genomics.pipeline.patent_data.get_ai_genomics_patents import (
    covert_list_of_codes_to_string,
)

AI_GENOMICS_PATENTS = load_s3_data(
    BUCKET_NAME,
    "inputs/patent_data/processed_patent_data/ai_genomics_patents_cpc_ipc_codes.csv",
)


def get_full_cpc_ipc_codes_query(
    ai_genomics_publication_numbers: List[
        str
    ] = AI_GENOMICS_PATENTS.publication_number.to_list(),
) -> str:
    """Generates query to retrieve full list of CPC and IPC codes per patent publication number.
    Args:
        ai_genomics_publication_numbers (list): a list of AI and Genomics publication numbers
    Returns:
        full_cpc_ipc_q (str): The BigQuery query to retrive a full list of CPC and IPC codes
    """
    ai_genomics_publication_numbers_formatted = covert_list_of_codes_to_string(
        ai_genomics_publication_numbers
    )

    full_cpc_ipc_q = (
        "select publication_number, cpc.code as cpc_code, ipc.code as ipc_code "
        "from patents-public-data.patents.publications, "
        "UNNEST(cpc) cpc, UNNEST(ipc) ipc "
        f"WHERE publication_number in ({ai_genomics_publication_numbers_formatted})"
    )

    return full_cpc_ipc_q


def add_full_cpc_ipc_codes(
    full_cpc_ipc_df: pd.DataFrame, ai_genomics_df: pd.DataFrame = AI_GENOMICS_PATENTS
) -> pd.DataFrame:
    """Adds full list of CPC and IPC codes per patent publication number to ai genomics dataset.
    Args:
        full_cpc_ipc_df (pd.DataFrame): Dataframe of ai genomics patent numbers, cpc and ipc codes
        ai_genomics_df (pd.DataFrame): Dataframe of ai genomics patents
    Returns:
        ai_genomics_df_full_codes (pd.DataFrame): DataFrame of ai genomics patents with full list of cpc and ipc codes
    """

    full_cpc_ipc_codes_agg = (
        full_cpc_ipc_df.groupby("publication_number").agg(set).reset_index()
    )
    ai_genomics_full_codes = pd.merge(
        full_cpc_ipc_codes_agg, ai_genomics_df, on="publication_number"
    )

    return ai_genomics_full_codes.drop(["cpc_code_y", "ipc_code_y"], axis=1).rename(
        columns={"cpc_code_x": "cpc_codes", "ipc_code_x": "ipc_codes"}
    )


if __name__ == "__main__":

    # establish conn
    conn = est_conn()

    full_cpc_ipc_q = get_full_cpc_ipc_codes_query()
    full_cpc_ipc_codes = conn.query(full_cpc_ipc_q).to_dataframe()
    logger.info(
        "pulled full cpc and ipc codes per AI Genomics publication number from Google BigQuery."
    )

    ai_genomics_full_codes = add_full_cpc_ipc_codes(full_cpc_ipc_codes)
    logger.info("merged AI genomics dataframe with full cpc and ipc codes.")

    save_to_s3(
        BUCKET_NAME,
        ai_genomics_full_codes,
        "inputs/patent_data/processed_patent_data/ai_genomics_patents_full_cpc_ipc_codes.csv",
    )
