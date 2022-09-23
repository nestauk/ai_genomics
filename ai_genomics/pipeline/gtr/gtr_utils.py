# Scripts to explore definitions in the GtR data
import logging
from typing import List, Dict

from ai_genomics.getters.data_getters import load_s3_data


def fetch_gtr(table_name: str) -> List[Dict]:
    """Fetch a json gtr object"""

    logging.info(f"Fetching table {table_name}")

    return load_s3_data("ai-genomics", f"inputs/gtr/{table_name}.json")
