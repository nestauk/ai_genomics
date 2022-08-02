# Script to test definitions in the crunchbase data

import io
import logging
from typing import Set, Union

import pandas as pd
import numpy as np
from toolz import pipe

from ai_genomics.utils.crunchbase import fetch_crunchbase, parse_s3_table

from ai_genomics import PROJECT_DIR, config


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
        str(descr_short) + "_" + str(descr_long)
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

    logging.info(f"Genomics terms combined: {len(gen_combined)}")
    logging.info(f"Artificial intelligence terms organisations: {len(ai_combined)}")
    logging.info(f"organisations with terms in both categories:{len(ai_gen_combined)}")

    logging.info("Get 5 random examples")
    store_df = []

    for cat, name in zip(
        [ai_combined, gen_combined, ai_gen_combined],
        ["ai_combined", "gen_combined", "ai_gen_combined"],
    ):

        relevant = cb_comps.loc[cb_comps["id"].isin(cat)]

        sampled = relevant.sample(5)

        for _, s in sampled.iterrows():

            store_df.append(
                {
                    **{
                        k: v
                        for k, v in dict(s).items()
                        if k in ["name", "description_combined"]
                    },
                    **{"category": name},
                }
            )

    logging.info(pd.DataFrame(store_df).head())

    (
        pd.DataFrame(store_df)
        .assign(
            description_combined=lambda df: df["description_combined"].str[:400] + "..."
        )
        .to_markdown(
            f"{PROJECT_DIR}/outputs/crunchbase_examples_reproduce.md", index=False
        )
    )
