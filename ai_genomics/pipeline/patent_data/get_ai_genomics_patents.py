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

from google.api_core.exceptions import Forbidden
import uuid

#################################################

AI_SUBDOMAIN_PREDICTIONS = [
    "ai_score_ml",
    "ai_score_evo",
    "ai_score_nlp",
    "ai_score_speech",
    "ai_score_vision",
]
AI_THRESHOLD_LOWER = 0.95
AI_THRESHOLD_UPPER = 1


def make_query_chunks(uspto_patent_ids, n_chunks: int, table: str) -> list:
    """Generate BigQuery query chunks based on USPTO AI patent IDS.

    Args:
        uspto_patent_ids: List of patent IDs.
        n_chunks (int): The number of chunks generated from uspto_patent_ids.
                        Chunk string size must be less than 1024.00K characters,
                        including comments and white space characters. This means
                        there needs to be approx. 85 query chunks to accomodate
                        string size limits.

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
            f"SELECT publication_number "
            f"FROM `{table}` "
            f"WHERE REGEXP_EXTRACT(publication_number, r'[0-9]+') IN ({ids}) AND STARTS_WITH(publication_number, 'US-')"
        )

        uspto_queries.append(q)

    if [
        len(uspto_query_chunk)
        for uspto_query_chunk in uspto_queries
        if len(uspto_patent_chunk) > 1024000
    ] != []:
        print("A query chunk(s) too large - make n_chunk int larger.")
    else:
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

    Returns:
        ai_genomics_patents (list): list of AI patent IDs in genomics-related
        sql tables.
    """
    ai_genomics_patents = []
    queries = [
        chunk
        for chunk in get_s3_dir_files(
            s3,
            bucket_name,
            f"outputs/patent_data/ai_genomics_id_chunks/{table}_{len(query_chunks)}_chunksize/",
        )
        if "csv" in chunk
    ]
    if len(queries) != len(
        query_chunks
    ):  # if there are more chunks than chunk files...
        for uspto_indx, uspto_query in enumerate(query_chunks[chunk_indx:]):
            try:
                data = (
                    conn.query(uspto_query).to_dataframe()
                    # .drop_duplicates("doc_id")
                )
                print(f"got query chunk {chunk_indx + uspto_indx + 1}")
                save_to_s3(
                    s3,
                    bucket_name,
                    data,
                    f"/outputs/patent_data/ai_genomics_id_chunks/{table}{len(query_chunks)}_chunksize/ai_genomics_patent_ids_{chunk_indx + uspto_indx + 1}_{str(uuid.uuid4())}.csv",
                )
            except Forbidden:
                print(
                    f"Time out error at {chunk_indx + uspto_indx + 1} patent chunk. Try again later."
                )
                break
    else:
        print("Queried all patent id chunks.")

    return queries


def make_list_of_ai_publication_ids(
    threshold_upper: float = AI_THRESHOLD_UPPER,
    threshold_lower: float = AI_THRESHOLD_LOWER,
    ai_subdomain_predictions: list = AI_SUBDOMAIN_PREDICTIONS,
) -> list:
    """Loads USPTO patents that have been assigned probabilties of
    being in different AI subdomains. Select for patents that have
    probabilities with a range of specified values for a specified
    set of subdomains.

    Args:
        threshold_upper: Upper prediction threshold for AI predictions
        threshold_lower: Lower prediction threshold for AI predictions
        ai_subdomain_predictions: List of AI subdomains to check thresholds
            for

    Returns:
        A list of patent publication ids that have been classified
            as being AI patents.
    """
    uspto_data = load_s3_data(bucket_name, config["uspto_file"])
    max_ai_subdomain_pred = uspto_data[ai_subdomain_predictions].max(axis=1)
    ai_subdomain_pred_in_range = (threshold_upper >= max_ai_subdomain_pred) & (
        threshold_lower < max_ai_subdomain_pred
    )
    is_patent = uspto_data.flag_patent == 1
    combined_conditions = ai_subdomain_pred_in_range & is_patent
    return uspto_data[combined_conditions].doc_id.tolist()


if __name__ == "__main__":

    # clean up BigQuery table name
    table = config["sql_table"].replace("*", "").replace(".", "_")

    # est BigQuery connection
    google_conn = est_conn()
    # load data
    uspto_patent_ids = make_list_of_ai_publication_ids()
    print("loaded data")

    # Make query chunks
    query_chunks = make_query_chunks(
        uspto_patent_ids, config["n_chunks"], config["sql_table"]
    )
    if query_chunks:
        query_chunks_indxs = [
            chunk
            for chunk in get_s3_dir_files(
                s3,
                bucket_name,
                f"outputs/patent_data/ai_genomics_id_chunks/{table}{len(query_chunks)}_chunksize/",
            )
            if "csv" in chunk
        ]
        # get query chunk indx based chunks already queried in s3
        if query_chunks_indxs != []:  # get last chunk
            indx = len(query_chunks_indxs)
        else:
            indx = 0
        # query BigQuery
        ai_patents = query_patent_data(google_conn, query_chunks, int(indx), table)
    else:
        pass
