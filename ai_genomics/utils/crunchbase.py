# Functions to fetch and process OpenAlex data
import json
import logging
from collections import Counter
from io import StringIO
from itertools import chain
from typing import Dict, List, Any, Union
import boto3
import pandas as pd
from toolz import pipe, partial


from ai_genomics import config


def fetch_crunchbase(
    table_name: str,
) -> pd.DataFrame:
    """Fetch a crunchbase table
    Args:
        s3_bucket where we store the data
        table_name: name of the table to fetch
    Returns:
    """
    s3 = boto3.resource("s3")
    ai_genomics_bucket = s3.Bucket("ai-genomics")

    logging.info(f"Fetching crunchbase {table_name}")

    return pipe(
        f"inputs/crunchbase/{table_name}.csv",
        ai_genomics_bucket.Object,
        lambda _object: _object.get(),
        # pd.DataFrame,
    )


def parse_s3_table(s3_object) -> pd.DataFrame:
    """Parses a s3 object into a pandas dataframe
    Args:
        s3_object: s3 object to parse
    Returns:
        pandas dataframe
    """
    return pipe(
        s3_object,
        lambda _object: _object["Body"].read().decode("utf-8"),
        StringIO,
        pd.read_csv,
    )
