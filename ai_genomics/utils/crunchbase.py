import logging
from io import StringIO
import boto3
import pandas as pd
from toolz import pipe


def fetch_crunchbase(
    table_name: str,
) -> pd.DataFrame:
    """Fetches and returns a crunchbase table from S3"""
    s3 = boto3.resource("s3")
    ai_genomics_bucket = s3.Bucket("ai-genomics")

    logging.info(f"Fetching crunchbase {table_name}")

    return pipe(
        f"inputs/crunchbase/{table_name}.csv",
        ai_genomics_bucket.Object,
        lambda _object: _object.get(),
    )


def parse_s3_table(s3_object) -> pd.DataFrame:
    """Parses an s3 object into a pandas dataframe"""
    return pipe(
        s3_object,
        lambda _object: _object["Body"].read().decode("utf-8"),
        StringIO,
        pd.read_csv,
    )
