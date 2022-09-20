"""Script to generate look up tables across datasets (ai and genomics, ai baseline, genomics baseline)
where output is {id: abstract} for DBpedia tagging."""

import pandas as pd
from datetime import datetime as date
import os

from ai_genomics import bucket_name
from ai_genomics.getters.data_getters import save_to_s3

from ai_genomics.getters.gtr import get_gtr_from_s3
from ai_genomics.getters.openalex import (
    _get_openalex_ai_genomics_abstracts,
    get_openalex_baseline,
)
from ai_genomics.getters.patents import get_ai_genomics_patents
from ai_genomics.getters.crunchbase import get_ai_genomics_orgs
from ai_genomics.utils.crunchbase import fetch_crunchbase, parse_s3_table

from typing import Dict

CB_DATA = load_s3_data(
    bucket_name, "outputs/crunchbase/crunchbase_ai_genom_comps.csv"
).rename(columns={"genom": "genomics", "ai_genom": "ai_genomics"})
GTR_DATA = load_s3_data(bucket_name, "outputs/gtr/gtr_ai_genomics_projects.csv")
OA_DATA = load_s3_data(bucket_name, "outputs/openalex/openalex_abstracts.json")

LOOKUP_TABLE_PATH = "inputs/lookup_tables/"
VALID_DF_TYPES = ["ai", "genomics", "ai_genomics"]


def get_lookup(
    df: pd.DataFrame, id_col: str, text_col: str, df_type: str = ""
) -> Dict[str, str]:
    """Converts pandas dataframe to lookup table where
    key is id and value is text."""

    assert pd.Series(df[id_col]).is_unique == True, "not every id is unique"

    if df_type != "":
        assert df_type in VALID_DF_TYPES, "invalid df_type"

        return df.query(f"{df_type} == True").set_index(id_col)[text_col].to_dict()
    else:

        return df.set_index(id_col)[text_col].to_dict()


def get_save_lookup(
    df: pd.DataFrame,
    ds: str,
    separate_dfs: False,
    ai_df: pd.DataFrame = get_ai_sample_patents(),
    genomics_df: pd.DataFrame = get_genomics_sample_patents(),
    ai_genomics_df: pd.DataFrame = get_ai_genomics_patents(),
):
    """generates lookup and saves lookup table to s3"""

    if separate_dfs:
        for name, df in zip(VALID_DF_TYPES, (ai_df, genomics_df, ai_genomics_df)):
            df_lookup = get_lookup(df=df, df_type=name)
            lookup_table_name = os.path.join(
                LOOKUP_TABLE_PATH, f"{name}_{ds}_lookup.json"
            )
            save_to_s3(bucket_name, df_lookup, lookup_table_name)
    else:
        for name in VALID_DF_TYPES:
            df_lookup = get_lookup(df=df, df_type=name)
            lookup_table_name = os.path.join(
                LOOKUP_TABLE_PATH, f"{name}_{ds}_lookup.json"
            )
            save_to_s3(bucket_name, df_lookup, lookup_table_name)


if __name__ == "__main__":

    # generate patents look ups across ai + genomics, ai baseline and genomics baseline patents
    get_save_lookup(separate_dfs=True, ds="patents")

    # generate and save cb look ups across ai + genomics, ai baseline and genomics baseline
    get_save_lookup(df=CB_DATA, ds="cb")

    # generate and save gtr look ups across ai + genomics, ai baseline and genomics baseline gtr
    get_save_lookup(df=GTR_DATA, ds="gtr")

    # generate openalex look ups across ai + genomics, ai baseline and genomics baseline openalex
    # TO DO: for openalex - where does the baseline live again??
    ai_genomics_openalex_lookup = _get_openalex_ai_genomics_abstracts()
