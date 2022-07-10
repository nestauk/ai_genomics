"""Script to query google patent data with USPTO
AI patent ids and identify patent ids that have
genomics-related classification codes."""
#################################################
from ai_genomics import bucket_name, config
from ai_genomics.utils.patent_data.get_ai_genomics_patents_utils import est_conn
from ai_genomics.getters.data_getters import s3, load_s3_data, save_to_s3

import pandas as pd
import numpy as np
import time

#################################################


def make_query_chunks(uspto_patent_ids, n_chunks: int, table: str) -> list:
    """Generate BigQuery query chunks based on USPTO AI patent IDS.

    Args:
        uspto_patent_ids: List of patent IDs.
        n_chunks (int): The number of chunks generated from uspto_patent_ids.

    Returns:
        uspto_queries (list): List of n_chunk size of BigQuery queries.
    """
    uspto_queries = []
    uspto_patent_chunks = np.array_split(
        uspto_patent_ids, n_chunks
    )  # split based on query string limits

    for uspto_patent_chunk in uspto_patent_chunks:
        ids = "'" + "', '".join([str(i) for i in uspto_patent_chunk]) + "'"
        q = (
            f"SELECT application_number "
            f"FROM `{table}` "
            f"WHERE REGEXP_EXTRACT(application_number, r'[0-9]+') IN ({ids})"
        )

        uspto_queries.append(q)

    return uspto_queries


def query_patent_data(conn, query_chunks: list) -> pd.DataFrame:
    """Queries genomics tables in query chunks.
    Args:
        conn: Google BigQuery connection.
        query_chunks (list): List of sql queries.

    Returns:
        ai_genomics_patents (list): list of AI patent IDs in genomics-related
        sql table.
    """
    ai_genomics_patents = []
    for uspto_indx, uspto_query in enumerate(query_chunks):
        data = (
            conn.query(uspto_query).to_dataframe().drop_duplicates("application_number")
        )
        print(f"got query chunk {uspto_indx}")
        ai_genomics_patents.append(data)

    all_ai_genomics_patents = pd.concat(ai_genomics_patents).reset_index(drop=True)

    return all_ai_genomics_patents


if __name__ == "__main__":
    # load data
    start = time.time()

    uspto_data = load_s3_data(bucket_name, config["uspto_file"])
    uspto_patent_ids = uspto_data[uspto_data.flag_patent == 1]["doc_id"]
    print("loaded data")
    ## Class codes
    # Make query chunks
    query_chunks = make_query_chunks(
        uspto_patent_ids, config["n_chunks"], config["sql_table"]
    )
    # est BigQuery connection
    google_conn = est_conn()
    # query BigQuery
    ai_patents = query_patent_data(google_conn, query_chunks)
    print("queried genomics related patents")
    # save output
    save_to_s3(s3, bucket_name, ai_patents, config["ai_genomics_patents_file"])

    print("It took", time.time() - start, "seconds.")
