# Script to generate CrunchBase data for analysis

import logging
import os
from typing import Set, Union

import boto3
import pandas as pd
import numpy as np
from toolz import pipe

from ai_genomics.utils.crunchbase import fetch_crunchbase, parse_s3_table

from ai_genomics import PROJECT_DIR, config

CB_INPUTS_DATA_DIR = PROJECT_DIR / "inputs/data/crunchbase/"
CB_OUTPUTS_DATA_DIR = PROJECT_DIR / "outputs/data/crunchbase"

CB_COMP_NAME = "crunchbase_ai_genom_comps.csv"

CB_COMP_PATH = CB_OUTPUTS_DATA_DIR / CB_COMP_NAME

os.makedirs(CB_INPUTS_DATA_DIR, exist_ok=True)
os.makedirs(CB_OUTPUTS_DATA_DIR, exist_ok=True)

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


def search_terms(abstract: str, terms: Set) -> Union[bool, float]:
    """Checks if a string contains any of the terms in a set.

    Args:
        abstract: string to check
        terms: set of terms to check for

    Returns:
        True if any of the terms are found, False otherwise (and
        nan if we don't have an abstract)
    """

    if pd.isnull(abstract) is False:

        return any(t in abstract.lower() for t in terms)

    else:
        return np.nan


def send_output_to_s3(file_path: str, s3_destination: str):
    """ """
    s3 = boto3.resource("s3")
    (s3.Bucket("ai-genomics").upload_file(file_path, f"outputs/{s3_destination}"))


if __name__ == "__main__":
    logging.info("Check organisations in relevant categories")

    cb_cat_comps = pipe(fetch_crunchbase("org_cats"), parse_s3_table)

    # Get set of organisations in each of the categories
    gen_cats, ai_cats = [
        set(cb_cat_comps.query(f"category_name=='{cat}'")["organization_id"])
        for cat in ["genetics", "artificial intelligence"]
    ]

    logging.info(f"Genetics category organisations: {len(gen_cats)}")
    logging.info(f"Artificial intelligence category organisations: {len(ai_cats)}")
    logging.info(f"organisations in both categories:{len(gen_cats & ai_cats)}")

    logging.info("Check organisations with relevant abstracts")
    cb_comps = pipe(fetch_crunchbase("orgs"), parse_s3_table)

    logging.info(f"total organisations: {len(cb_comps)/1e6} M")

    ai_terms, genom_terms = [
        config[category] for category in ["ai_cb_terms", "genom_cb_terms"]
    ]

    cb_comps["description_combined"] = [
        f"{str(descr_short)} {str(descr_long)}"
        for descr_short, descr_long in zip(
            cb_comps["short_description"], cb_comps["long_description"]
        )
    ]

    cb_comps["has_ai"], cb_comps["has_genom"] = [
        [search_terms(descr, terms) for descr in cb_comps["description_combined"]]
        for terms in [ai_terms, genom_terms]
    ]

    logging.info(f"Genomics terms organisations: {sum(cb_comps['has_genom'])}")
    logging.info(
        f"Artificial intelligence terms organisations: {sum(cb_comps['has_ai'])}"
    )
    logging.info(
        f"organisations with terms in both categories:{sum(cb_comps['has_ai'] & cb_comps['has_genom'])}"
    )

    # Extract ids for ai / genom description organisations
    ai_descr, genom_descr = [
        set(cb_comps[cb_comps[f"has_{var}"] == True]["id"]) for var in ["ai", "genom"]
    ]

    ai_combined = ai_cats.union(ai_descr)
    gen_combined = gen_cats.union(genom_descr)
    ai_gen_combined = ai_combined & gen_combined

    # Create and save table tagged with AI, Genom and AI genom columns
    cb_comps["ai"], cb_comps["genom"], cb_comps["ai_genom"] = [
        cb_comps["id"].isin(x) for x in [ai_combined, gen_combined, ai_gen_combined]
    ]

    cb_comps.loc[
        cb_comps[["ai", "genom", "ai_genom"]].values.sum(axis=1) > 0, :
    ].reset_index(drop=True)[KEEP_CB_COLS].to_csv(CB_COMP_PATH, index=False)

    send_output_to_s3(str(CB_COMP_PATH), f"crunchbase/{CB_COMP_NAME}")
