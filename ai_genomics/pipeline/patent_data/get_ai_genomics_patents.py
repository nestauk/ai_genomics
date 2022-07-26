"""Script to query google patent data with USPTO
AI patent ids and identify patent ids that have
genomics-related classification codes.

To run script,
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/credentials.json"
python get_ai_genomics_patents.py
"""

from ai_genomics import bucket_name, config, logger
from ai_genomics.utils.patent_data.get_ai_genomics_patents_utils import est_conn
from ai_genomics.getters.data_getters import (
    Error,
    load_s3_data,
    save_to_s3,
    get_s3_dir_files,
)

import pandas as pd
import numpy as np

from google.cloud import bigquery
from google.api_core.exceptions import Forbidden
import uuid
from toolz.itertoolz import partition_all
from typing import List

def make_query_chunks(
    uspto_patent_ids: List[str], 
    table: str) -> List[str]:
    """Generate BigQuery query chunks based on USPTO AI patent IDS.

    Args:
        uspto_patent_ids: List of patent IDs.
    Returns:
        uspto_queries (list): List of BigQuery queries.
    """
    uspto_queries = []
    uspto_patent_chunks = list(partition_all(8520, uspto_patent_ids))  # split based on query string limits

    for uspto_patent_chunk in uspto_patent_chunks:
        ids = "'" + "', '".join([str(i) for i in uspto_patent_chunk]) + "'"
        q = (
            "SELECT publication_number "
            f"FROM `{table}` "
            f"WHERE REGEXP_EXTRACT(publication_number, r'[0-9]+') IN ({ids}) AND STARTS_WITH(publication_number, 'US-')"
        )

        if len(q) > 102400: #resulting query must be less than 1024000 chars
            raise Error("Query too large - make int smaller.")
        else:
            uspto_queries.append(q)

    return uspto_queries

def query_patent_data(
    conn, query_chunks: list, chunk_indx: int, table: str
) -> pd.DataFrame:
    """Queries genomics tables in query chunks. Saves each query chunk
    as a CSV and prints chunk_indx when Time out error.
    
    Args:
        conn: Google BigQuery connection.
        query_chunks (list): List of sql queries.
        chunk_indx (int): query chunks indx to start querying from.
    """
    chunks = get_s3_dir_files(
            bucket_name,
            f"outputs/patent_data/ai_genomics_id_chunks/{table}_{len(query_chunks)}_chunksize/",
        )
    ai_genomics_patent_id_chunk_csvs = [
        chunk
        for chunk in chunks
        if chunk.endswith("csv")
    ]
    if len(ai_genomics_patent_id_chunk_csvs) != len(query_chunks):  # if there are more chunks than chunk files...
        for uspto_indx, uspto_query in enumerate(query_chunks[chunk_indx:]):
            try:
                data = (
                    conn.query(uspto_query).to_dataframe()
                    # .drop_duplicates("doc_id")
                )
                logger.info(f"got query chunk {chunk_indx + uspto_indx + 1}")
                save_to_s3(
                    bucket_name,
                    data,
                    f"/outputs/patent_data/ai_genomics_id_chunks/test/{table}{len(query_chunks)}_chunksize/ai_genomics_patent_ids_{chunk_indx + uspto_indx + 1}_{str(uuid.uuid4())}.csv",
                )
            except Forbidden:
                raise Error(
                    f"Time out error at {chunk_indx + uspto_indx + 1} patent chunk. Try again later."
                )
    else:
        logger.info("Queried all patent id chunks.")

if __name__ == "__main__":

    # clean up BigQuery table name
    table = config["sql_table"].replace("*", "").replace(".", "_")

    # est BigQuery connection
    google_conn = est_conn()
    # load data
    uspto_data = load_s3_data(bucket_name, config["uspto_file"])
    uspto_data = uspto_data[uspto_data["predict50_any_ai"] > 0]
    uspto_patent_ids = uspto_data[uspto_data.flag_patent == 1]["doc_id"].tolist()
    print("loaded data")

    # Make query chunks
    query_chunks = make_query_chunks(
        uspto_patent_ids, config["sql_table"]
    )

    chunks = get_s3_dir_files(bucket_name, f"outputs/patent_data/ai_genomics_id_chunks/{table}{len(query_chunks)}_chunksize/")
    query_chunks_indxs = [chunk for chunk in chunks if chunk.endswith("csv")]
        # get query chunk indx based chunks already queried in s3
    if query_chunks_indxs != []:  # get last chunk
        indx = len(query_chunks_indxs)
    else:
        indx = 0
    # query BigQuery
    query_patent_data(google_conn, query_chunks, int(indx), table)