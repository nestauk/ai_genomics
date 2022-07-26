"""ai_genomics/pipeline/patent_data/cleaning.py

Functions for cleaning patent data from Google Patents.
"""

import json
from toolz.functoolz import pipe

from typing import List, Union

from ai_genomics.utils.text import contains_non_ascii


def parse_patent_record(record_raw: str) -> dict:
    """Parses a JSON-like string from patent data fields that have NDJSON
    format. Includes fields like `title_localized`, `abstract_localized` and
    `inventor_harmonized`.

    Args:
        record: JSON-like string of a single record from a patent field.

    Returns:
        Dictionary of record.
    """
    if len(record_raw) > 5:  # handle empty records
        return pipe(
            record_raw,
            lambda r: r.replace("'", '"'),
            lambda r: r.replace("False", '"False"'),
            lambda r: r.replace("True", '"True"'),
            json.loads,
        )
    else:
        return None


def preprocess_records(records_raw: str) -> List[str]:
    """Splits ndjson-like field (e.g. abstract_localized, title_localized) into
    list of dict-like strings.
    """
    return pipe(records_raw, lambda rs: rs[1:-1], lambda rs: rs.split("\n"))


def extract_english_record(records_raw: str) -> Union[str, None]:
    """Extracts English text from a record if present. Where possible, texts
    with a full character set are selected, otherwise the text returned is
    ASCII only.

    Args:
        records_raw: NDJSON-like field from Google Patents that contains identical
            entries in different languages. This might include:
                - title_localized
                - abstract_localized

    Returns:
        text: The English text. If none available, then None is returned.
    """

    text = None

    records = preprocess_records(records_raw)
    for record in records:
        record = parse_patent_record(record)
        if record is None:
            return record

        if (record["language"] == "en") and (contains_non_ascii(record["text"])):
            return record["text"]
        elif (record["language"] == "en") and not (contains_non_ascii(record["text"])):
            text = record["text"]

    return text
