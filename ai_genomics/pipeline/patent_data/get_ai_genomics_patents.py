"""Script to query google patent data with USPTO
AI patent ids and identify patent ids that have
genomics-related classification codes.

To run script,
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/credentials.json"
python get_ai_genomics_patents.py
"""
#################################################
from ai_genomics import bucket_name, config
from ai_genomics.utils.patent_data.get_ai_genomics_patents_utils import est_conn
from ai_genomics.getters.data_getters import (
    s3,
    load_s3_data,
    save_to_s3,
    get_s3_dir_files,
)

import pandas as pd
import numpy as np

import time
from google.api_core.exceptions import Forbidden
import uuid

#################################################


def make_query_chunks(uspto_patent_ids, n_chunks: int, table: str) -> list:
    """Generate BigQuery query chunks based on USPTO AI patent IDS.

    Args:
        uspto_patent_ids: List of patent IDs.
        n_chunks (int): The number of chunks generated from uspto_patent_ids.
                        Chunk string size must be less than 1024.00K characters,
                        including comments and white space characters. 

    Returns:
        uspto_queries (list): List of n_chunk size of BigQuery queries.
    """
    uspto_queries = []
    uspto_patent_chunks = np.array_split(
        uspto_patent_ids, n_chunks
    )  # split based on query string limits

    for uspto_patent_chunk in uspto_patent_chunks:
        if len(uspto_patent_chunk) < 1024000:
            ids = "'" + "', '".join([str(i) for i in uspto_patent_chunk]) + "'"
            q = (
                f"SELECT application_number "
                f"FROM `{table}` "
                f"WHERE REGEXP_EXTRACT(application_number, r'[0-9]+') IN ({ids})"
            )

            uspto_queries.append(q)
        else:
            print('chunk size too large - choose a larger n_chunks int')

    return uspto_queries


def query_patent_data(conn, query_chunks: list, chunk_indx: int) -> pd.DataFrame:
    """Queries genomics tables in query chunks. Saves each query chunk
    as a CSV and prints chunk_indx query fails at due to Time out error.
    Args:
        conn: Google BigQuery connection.
        query_chunks (list): List of sql queries.
        chunk_indx (int): query chunks indx to start querying from.

    Returns:
        ai_genomics_patents (list): list of AI patent IDs in genomics-related
        sql tables.
    """
    ai_genomics_patents = []

    if query_chunks[chunk_indx:] != []:
        for uspto_indx, uspto_query in enumerate(query_chunks[chunk_indx:]):
            try:
                data = (
                    conn.query(uspto_query)
                    .to_dataframe()
                    .drop_duplicates("application_number")
                )
                print(f"got query chunk {chunk_indx + uspto_indx + 1}")
                save_to_s3(
                    s3,
                    bucket_name,
                    data,
                    f"/outputs/patent_data/ai_genomics_id_chunks/ai_genomics_patent_ids_{chunk_indx + uspto_indx + 1}_chunksize_{len(query_chunks)}_{str(uuid.uuid4())}.csv",
                )
            except Forbidden:
                print(
                    f"Time out error at {chunk_indx + uspto_indx + 1} patent chunk. Try again later."
                )
                break
    else:
        print("Queried all patent id chunks.")


if __name__ == "__main__":
    start = time.time()
    # est BigQuery connection
    google_conn = est_conn()
    # load data
    uspto_data = load_s3_data(bucket_name, config["uspto_file"])
    uspto_patent_ids = uspto_data[uspto_data.flag_patent == 1]["doc_id"]
    print("loaded data")
    # Make query chunks
    query_chunks = make_query_chunks(
        uspto_patent_ids, config["n_chunks"], config["sql_table"]
    )
    # get query chunk indx based chunks already queried in s3
    query_chunks_indxs = [
        indx
        for indx in get_s3_dir_files(
            s3, bucket_name, "outputs/patent_data/ai_genomics_id_chunks/"
        )
        if "csv" in indx
    ]
    if query_chunks_indxs != []:
        for query_chunk_indx in query_chunks_indxs:
            indx = query_chunk_indx.split("/")[-1].split("_")[4]
    else:
        indx = 0
    # query BigQuery
    ai_patents = query_patent_data(google_conn, query_chunks, int(indx))
    print("It took", time.time() - start, "seconds.")
