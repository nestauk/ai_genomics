# Script to find influential openalex researchers

import logging
import os
import pandas as pd
import numpy as np
from ai_genomics import PROJECT_DIR
from typing import List

import ai_genomics.getters.openalex as oalex

TARGET_PATH = f"{PROJECT_DIR}/outputs/data/experts"

VARS_TO_KEEP = [
    "auth_display_name",
    "auth_id",
    "auth_orcid",
    "display_name",
    "num_pubs",
]


def ai_genomics_table() -> pd.DataFrame:
    """Placeholder getter for the AI genomics data"""
    return pd.read_csv(f"{PROJECT_DIR}/outputs/ai_genomics_provisional_dataset.csv")


def high_cited(cit_distr: pd.Series, q: float) -> List[bool]:
    """Return a list of booleans indicating
    whether each author has high cited papers
    """

    thres = np.quantile(cit_distr, q)

    return [c > thres for c in cit_distr]


def load_all_oalex(oalex_getter, id_name: str):
    """Reads, concatenates and returns all OpenAlex data"""
    return (
        pd.concat(
            [
                oalex_getter(disc, year_list=range(2012, 2022))
                for disc in ["artificial_intelligence", "genetics"]
            ]
        )
        .drop_duplicates(id_name)
        .reset_index(drop=True)
    )


def get_top_authors(auths: pd.DataFrame, ids: set, country="all"):
    """Returns top authors in a dataset"""

    auths_out = (
        auths.loc[auths["id"].isin(ids)][
            [
                "auth_id",
                "auth_display_name",
                "auth_orcid",
                "inst_id",
                "display_name",
                "country_code",
            ]
        ]
        .value_counts(ascending=False)
        .reset_index(name="num_pubs")
    )

    # Filter by country if needed
    return (
        auths_out
        if country == "all"
        else (
            auths_out.loc[auths_out["country_code"] == country].reset_index(drop=True)
        )
    )


if __name__ == "__main__":

    logging.info("Loading data")
    # AI genomics papers including position in the
    # citation distribution in each year (to control for the
    # fact that older papers have more time to accumulate citations)
    ai_proc = (
        ai_genomics_table()
        .groupby("publication_year")
        .apply(
            lambda df: df.assign(
                high_cit=lambda df_2: high_cited(df_2["cited_by_count"], 0.5)
            )
        )
        .reset_index(drop=True)
    )
    # We implement a convoluted method to ensure we are only recommending
    # authors who according to the most recent openalex affiliation data are based
    # in the UK

    # Lookup between Openalex and publication date
    work_date_lu = (
        load_all_oalex(oalex.work_metadata, "work_id")
        .set_index("work_id")["publication_date"]
        .to_dict()
    )

    # All openalex authors
    oalex_author = (
        load_all_oalex(oalex.work_authorship, "id")
        .assign(pub_date=lambda df: df["id"].map(work_date_lu))
        .assign(pub_date=lambda df: pd.to_datetime(df["pub_date"]))
    )

    instits = oalex.instit_metadata().rename(columns={"id": "inst_id"})

    logging.info("Finding the most recent institution for each author")

    # Last publication date for each author
    oalex_last_date = oalex_author.groupby("auth_id")["pub_date"].max()

    # Last institution for each author (NB the inner merge)
    oalex_last_inst = (
        oalex_author.merge(
            oalex_last_date.reset_index(name="last_date"),
            left_on=["auth_id", "pub_date"],
            right_on=["auth_id", "last_date"],
            how="inner",
        )
        .reset_index(drop=True)
        .merge(instits, left_on="inst_id", right_on="inst_id")
    )[["auth_id", "inst_id", "display_name", "country_code"]]

    # This combines author paper data with an author's last institution
    oalex_author_2 = oalex_author.drop(axis=1, labels=["inst_id"]).merge(
        oalex_last_inst, left_on="auth_id", right_on="auth_id", how="left"
    )

    # IDS for all "AI genomics papers" and all highly cited "AI genomics" papers
    ai_genom_ids = set(ai_proc["work_id"])
    ai_genom_ids_cited = set(ai_proc.query("high_cit==True")["work_id"])

    # Save files
    os.makedirs(TARGET_PATH, exist_ok=True)
    for _ids, title in zip(
        [ai_genom_ids, ai_genom_ids_cited],
        ["top_openalex_uk_authors", "top_openalex_uk_authors_cited"],
    ):
        tab = get_top_authors(oalex_author_2, _ids, country="GB").head(n=50)[
            VARS_TO_KEEP
        ]
        logging.info(tab.head())

        tab.to_csv(f"{TARGET_PATH}/{title}.csv", index=False)
