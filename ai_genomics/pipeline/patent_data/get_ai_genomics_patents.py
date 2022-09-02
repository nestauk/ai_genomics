"""Script to query BigQuery based on genomics related and AI related cpc/ipc codes."""
from ai_genomics import bucket_name, logger
from ai_genomics.getters.patents import (
    get_ai_genomics_cpc_codes,
    get_ai_genomics_ipc_codes_formatted,
)
from ai_genomics.utils.patents import (
    est_conn,
    replace_missing_values_with_nans,
    convert_date_columns_to_datetime,
)
from ai_genomics.getters.data_getters import load_s3_data, save_to_s3
from google.api_core.exceptions import Forbidden
from google.cloud import bigquery
from typing import Dict, List
from argparse import ArgumentParser

S3_SAVE_FILENAME = (
    "inputs/patent_data/processed_patent_data/ai_genomics_patents_cpc_ipc_codes.csv"
)
DATASET_NAME = "golden-shine-355915.genomics"

GENOMICS_AI_FIELDS = (
    "publication_number, application_number, cpc.code as cpc_code, ipc.code as ipc_code, "
    "title_localized.text as title_text, title_localized.language as title_language, "
    "abstract_localized.text as abstract_text, abstract_localized.language as abstract_language, "
    "publication_date, filing_date, grant_date, priority_date, inventor, assignee, entity_status "
)

CPC_CODES = get_ai_genomics_cpc_codes()
IPC_CODES = get_ai_genomics_ipc_codes_formatted()


def genomics_ai_query(
    cpc_codes: Dict[str, list] = CPC_CODES,
    ipc_codes: Dict[str, list] = IPC_CODES,
) -> str:
    """Generates query to create bespoke genomics ai table
            based on cpc and ipc codes.

    Args:
        cpc_codes: dictionary of genomics and ai related cpc codes.
        ipc_codes: dictionary of genomics and ai related ipc codes.

    Returns:
        BigQuery query to select genomics and ai related patents.
    """
    cpc_ai_ids, ipc_ai_ids = (
        covert_list_of_codes_to_string(list(cpc_codes["ai"].keys())),
        covert_list_of_codes_to_string(list(ipc_codes["ai"].keys())),
    )
    cpc_genomics_ids, ipc_genomics_ids = (
        covert_list_of_codes_to_string(list(cpc_codes["genomics"]).keys()),
        covert_list_of_codes_to_string(list(ipc_codes["genomics"].keys())),
    )

    genomics_q = (
        f"SELECT DISTINCT publication_number "
        "FROM `patents-public-data.patents.publications`, UNNEST(cpc) AS cpc__u, UNNEST(ipc) AS ipc__u "
        f"WHERE cpc__u.code IN ({cpc_genomics_ids}) OR "
        f"ipc__u.code IN ({ipc_genomics_ids})"
    )

    genomics_ai_q = (
        f"WITH "
        f"genomics_ids AS ({genomics_q}) "
        "SELECT "
        f"{GENOMICS_AI_FIELDS}"
        "FROM `patents-public-data.patents.publications`, UNNEST(cpc) AS cpc, UNNEST(ipc) AS ipc, "
        "UNNEST(title_localized) AS title_localized, UNNEST(abstract_localized) AS abstract_localized "
        "INNER JOIN genomics_ids USING(publication_number) "
        f"WHERE cpc.code in ({cpc_ai_ids}) OR ipc.code in ({ipc_ai_ids});"
    )

    return genomics_ai_q


def select_unique_ai_genomics_patents(
    full_table_name: str = "golden-shine-355915.genomics.ai_genomics",
) -> str:
    """Returns BigQuery query to select unique ai-genomics patents
    based on publication_number from specified full_table_name
    """
    unique_ai_genomics_patents = (
        f"with english_ai_genomics as (select * from {full_table_name} "
        "WHERE title_language = 'en' AND abstract_language = 'en') "
        "SELECT * FROM ("
        "SELECT *, ROW_NUMBER() OVER (PARTITION BY publication_number) row_number "
        f"FROM english_ai_genomics) "
        "WHERE row_number = 1"
    )

    return unique_ai_genomics_patents


if __name__ == "__main__":

    parser = ArgumentParser()

    parser.add_argument(
        "--table_name",
        help="the name of the table to pull ai genomics patents from",
        default="ai_genomics",
    )

    args = parser.parse_args()
    table_name = args.table_name
    full_table_name = f"{DATASET_NAME}.{table_name}"

    conn = est_conn()
    tables = conn.list_tables(DATASET_NAME)
    table_names = [
        f"{table.project}.{table.dataset_id}.{table.table_id}" for table in tables
    ]

    unique_ai_genomics_patents_q = select_unique_ai_genomics_patents(
        full_table_name=full_table_name
    )

    if full_table_name not in table_names:
        try:
            ai_genomics_table_q = genomics_ai_query()

            job_config = bigquery.QueryJobConfig(destination=full_table_name)
            query_job = conn.query(ai_genomics_table_q, job_config=job_config)
            query_job.result()
            logger.info(f"Query results loaded to the table {full_table_name}")
        except Forbidden:
            logger.exception("Time out error. Try again in 2-3 hours.")

    try:
        genomics_ai_df = conn.query(unique_ai_genomics_patents_q).to_dataframe()
        genomics_ai_df = (
            genomics_ai_df.drop(columns="row_number")
            .pipe(replace_missing_values_with_nans)
            .pipe(convert_date_columns_to_datetime)
        )
        # save to s3
        save_to_s3(bucket_name, genomics_ai_df, S3_SAVE_FILENAME)
    except Forbidden:
        logger.exception("Time out error. Try again in 2-3 hours.")
