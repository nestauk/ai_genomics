from dataclasses import dataclass
import datetime
import gzip
import json
from toolz import pipe

import numpy as np
import pandas as pd
from pandas import DataFrame

from ai_genomics import PROJECT_DIR

DATA_PATH = f"{PROJECT_DIR}/inputs/data/papers_with_code"


def make_year(datetime: datetime.datetime) -> int:
    """Extracts year from a datetime.datetime object"""

    return int(datetime.year) if pd.isnull(datetime) is False else np.nan


def read_parse(file_name: str) -> dict:
    """Reads, decompresses and parses a pwc file"""
    with gzip.open(f"{DATA_PATH}/{file_name}", "rb") as f:
        file_content = f.read()

    return json.loads(file_content)


def parse_date_string(date_string: str, _format: str = "%Y-%m-%d") -> datetime.datetime:
    """Parses a date string"""

    return (
        datetime.datetime.strptime(date_string, _format)
        if pd.isnull(date_string) is False
        else np.nan
    )


def make_empty_list_na(df: DataFrame, variables: list):
    """Remove empty lists with np.nans in a dataframe"""

    df_ = df.copy()
    for v in variables:

        df_[v] = df[v].apply(lambda x: x if len(x) > 0 else np.nan)

    return df_


def read_pwc_papers() -> DataFrame:
    """Get papers table"""
    # Read and parse the data
    return (
        pipe(
            "papers-with-abstracts.json.gz",
            read_parse,
            DataFrame,
            lambda df: make_empty_list_na(df, ["tasks", "methods"]),
        )
        .replace({None: np.nan, "": np.nan})
        .assign(date=lambda df: df["date"].apply(parse_date_string))
        .assign(year=lambda df: df["date"].apply(make_year))
        .assign(
            methods_name=lambda df: [
                [meth["name"] for meth in meth_list]
                if type(meth_list) is list
                else np.nan
                for meth_list in df["methods"]
            ]
        )
    )
