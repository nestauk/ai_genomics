import logging
from io import StringIO
import boto3
import pandas as pd
from toolz import pipe


KEEP_CB_COLS = [
    "id",
    "name",
    "type",
    "created_at",
    "updated_at",
    "roles",
    "homepage_url",
    "country_code",
    "state_code",
    "region",
    "city",
    "address",
    "num_funding_rounds",
    "total_funding_usd",
    "founded_on",
    "employee_count",
    "num_exits",
    "location_id",
    "short_description",
    "long_description",
    "description_combined",
    "ai",
    "genom",
    "ai_genom",
]


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


def parse_s3_table(
    s3_object,
) -> pd.DataFrame:
    """Parses an s3 object into a pandas dataframe"""
    return pipe(
        s3_object,
        lambda _object: _object["Body"].read().decode("utf-8"),
        StringIO,
        pd.read_csv,
    )
