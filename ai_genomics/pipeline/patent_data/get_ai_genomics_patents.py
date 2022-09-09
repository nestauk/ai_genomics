"""Script to query BigQuery based on genomics related and AI related cpc/ipc codes."""
from ai_genomics import bucket_name
from ai_genomics.getters.patents import (
    get_ai_genomics_cpc_codes,
    get_ai_genomics_ipc_codes_formatted,
)
from ai_genomics.utils.patents import (
    est_conn,
    replace_missing_values_with_nans,
    convert_date_columns_to_datetime,
    convert_list_of_codes_to_string,
)
from ai_genomics.getters.data_getters import load_s3_data, save_to_s3
from typing import Dict
import pandas as pd

GENOMICS_AI_FIELDS = (
    "publication_number, application_number, cpc.code as cpc_code, ipc.code as ipc_code, "
    "title_localized.text as title_text, title_localized.language as title_language, "
    "abstract_localized.text as abstract_text, abstract_localized.language as abstract_language, "
    "publication_date, filing_date, grant_date, priority_date, inventor, assignee, entity_status "
)

CPC_CODES = get_ai_genomics_cpc_codes()
IPC_CODES = get_ai_genomics_ipc_codes_formatted()


def code_query(
    cpc_codes: Dict[str, list] = CPC_CODES,
    ipc_codes: Dict[str, list] = IPC_CODES,
    topic: str = "ai",
    sample: int = 10,
) -> str:
    """Generates query to get sample of patents based on cpc/ipc codes.

    Args:
        cpc_codes: dictionary of cpc codes.
        ipc_codes: dictionary of ipc codes.

    Returns:
        BigQuery query to select related patents.
    """
    cpc_ids, ipc_ids = (
        convert_list_of_codes_to_string(list(cpc_codes[topic].keys())),
        convert_list_of_codes_to_string(list(ipc_codes[topic].keys())),
    )

    topic_q = (
        f"SELECT {GENOMICS_AI_FIELDS} "
        f"FROM `patents-public-data.patents.publications` TABLESAMPLE SYSTEM ({sample} PERCENT), "
        " UNNEST(cpc) AS cpc, UNNEST(ipc) AS ipc, "
        "UNNEST(title_localized) AS title_localized, UNNEST(abstract_localized) AS abstract_localized "
        f"WHERE cpc.code IN ({cpc_ids}) OR "
        f"ipc.code IN ({ipc_ids})"
    )

    return topic_q


def genomics_ai_query(
    cpc_codes: Dict[str, list] = CPC_CODES,
    ipc_codes: Dict[str, list] = IPC_CODES,
) -> str:
    """Generates query to identify genomics ai patents
            based on cpc and ipc codes.

    Args:
        cpc_codes: dictionary of genomics and ai related cpc codes.
        ipc_codes: dictionary of genomics and ai related ipc codes.

    Returns:
        BigQuery query to select genomics and ai related patents.
    """

    cpc_ai_ids, ipc_ai_ids = (
        convert_list_of_codes_to_string(list(cpc_codes["ai"].keys())),
        convert_list_of_codes_to_string(list(ipc_codes["ai"].keys())),
    )
    cpc_genomics_ids, ipc_genomics_ids = (
        convert_list_of_codes_to_string(list(cpc_codes["genomics"].keys())),
        convert_list_of_codes_to_string(list(ipc_codes["genomics"].keys())),
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


def get_patents(
    conn,
    table_name: str = "ai_genomics",
) -> pd.DataFrame:
    """Returns DataFrame to select unique patents
    based on publication_number from specified table name
    """
    unique_patents = (
        "SELECT * FROM ("
        "SELECT *, ROW_NUMBER() OVER (PARTITION BY publication_number) row_number "
        f"FROM golden-shine-355915.genomics.{table_name}) "
        "WHERE row_number = 1"
    )

    df = conn.query(unique_patents).to_dataframe()
    df = (
        df.drop(columns="row_number")
        .pipe(replace_missing_values_with_nans)
        .pipe(convert_date_columns_to_datetime)
    )

    return df


if __name__ == "__main__":

    conn = est_conn()

    ai_genomics_patents = get_patents(conn)
    ai_patents_sample = get_patents(conn, table_name="ai_sample")
    genomics_patents_sample = get_patents(conn, table_name="genomics_sample")

    for table_name, table in zip(
        ("ai_genomics", "ai_sample", "genomics_sample"),
        (ai_genomics_patents, ai_patents_sample, genomics_patents_sample),
    ):
        full_table_name = f"inputs/patent_data/processed_patent_data/{table_name}_patents_cpc_ipc_codes.csv"
        save_to_s3(bucket_name, table, full_table_name)
