"""
This script adds the full list of CPC and IPC codes per patent number to the AI and Genomics patents dataset saved in S3.
"""
from re import A
from ai_genomics.getters.data_getters import load_s3_data, save_to_s3
from ai_genomics import logger, bucket_name
import pandas as pd
from collections.abc import Sequence

from ai_genomics.utils.patents import est_conn, convert_list_of_codes_to_string

ai_genomics_patents = load_s3_data(
    bucket_name,
    "inputs/patent_data/processed_patent_data/ai_genomics_patents_cpc_ipc_codes.csv",
)
ai_genomics_publicaion_number = ai_genomics_patents.publicaiton_number


def get_full_cpc_ipc_codes_query(
    ai_genomics_publication_numbers: Sequence[str] = ai_genomics_patents,
) -> str:
    """Generates query to retrieve full list of CPC and IPC codes per patent publication number.
    Args:
        ai_genomics_publication_numbers: a list of AI and Genomics publication numbers
    Returns:
        full_cpc_ipc_q: The BigQuery query to retrive a full list of CPC and IPC codes
    """

    ai_genomics_publication_numbers_formatted = convert_list_of_codes_to_string(
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
    full_cpc_ipc_df: pd.DataFrame, ai_genomics_df: pd.DataFrame = ai_genomics_patents
) -> pd.DataFrame:
    """Adds full list of CPC and IPC codes per patent publication number to ai genomics dataset.
        Returns DataFrame of ai genomics patents with full list of cpc and ipc codes.
    Args:
        full_cpc_ipc_df: Dataframe of ai genomics patent numbers, cpc and ipc codes
        ai_genomics_df: Dataframe of ai genomics patents
    """

    full_cpc_ipc_codes_agg = (
        full_cpc_ipc_df.groupby("publication_number").agg(set).reset_index()
    )
    ai_genomics_full_codes = pd.merge(
        full_cpc_ipc_codes_agg, ai_genomics_df, on="publication_number"
    )

    return (
        pd.merge(full_cpc_ipc_codes_agg, ai_genomics_df, on="publication_number")
        .drop(["cpc_code_y", "ipc_code_y"], axis=1)
        .rename(columns={"cpc_code_x": "cpc_codes", "ipc_code_x": "ipc_codes"})
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
        bucket_name,
        ai_genomics_full_codes,
        "inputs/patent_data/processed_patent_data/ai_genomics_patents_cpc_ipc_codes.csv",
    )
