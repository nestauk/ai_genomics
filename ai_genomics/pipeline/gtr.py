# Scripts to explore definitions in the GtR data
import json
import logging
import boto3
from typing import List, Dict

from toolz import pipe


def fetch_gtr(table_name: str) -> List[Dict]:
    """Fetch a json gtr object"""
    s3 = boto3.resource("s3")
    ai_genomics_bucket = s3.Bucket("ai-genomics")

    logging.info(f"Fetching table {table_name}")

    return pipe(
        f"inputs/gtr/{table_name}.json",
        ai_genomics_bucket.Object,
        lambda _object: _object.get()["Body"].read().decode(),
        json.loads,
    )
