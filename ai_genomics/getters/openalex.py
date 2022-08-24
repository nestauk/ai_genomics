import json

import pandas as pd
from typing import List, Dict, Any
from functools import reduce
from toolz import pipe

from ai_genomics.utils.reading import read_json
from ai_genomics import PROJECT_DIR, logger
import pandas as pd

OALEX_PATH = f"{PROJECT_DIR}/inputs/data/openalex"


def get_openalex_works() -> List[Dict[Any, Any]]:
    """Reads OpenAlex works (papers)

    Returns:
        A list of dicts. Every element is a paper with metadata.
        See here for more info: https://docs.openalex.org/about-the-data/work
    """

    return read_json(
        f"{PROJECT_DIR}/inputs/openalex/openalex_works_production-True.json"
    )


def get_openalex_instits() -> list:
    """Reads OpenAlex institutions

    Returns:
        A list of dicts. Every element is an institution with metadata.
        See here for more info: https://docs.openalex.org/about-the-data/institution
    """

    return read_json(f"{PROJECT_DIR}/inputs/openalex/institutions.json")


def get_openalex_concepts() -> list:
    """Reads OpenAlex concepts

    Returns:
        A list of dicts. Every element is a concept with metadata.
        See here for more info: https://docs.openalex.org/about-the-data/concept
    """

    return read_json(f"{PROJECT_DIR}/inputs/openalex/concepts.json")


def get_concepts_df() -> pd.DataFrame:
    """Parses open alex concepts as a dataframe"""

    KEEP_KEYS = ["id", "display_name", "level", "works_count"]

    return pipe(
        get_openalex_concepts(),
        lambda list_dict: [
            {k: v for k, v in _dict.items() if k in KEEP_KEYS} for _dict in list_dict
        ],
        pd.DataFrame,
    )


def work_metadata(discipline: str, year_list: list) -> pd.DataFrame:
    """Reads metadata about openalex works

    Args:
        discipline: The discipline of the work (AI or genetics)
        year_list: publication years

    Returns:
        A df with the metadata
    """

    return pd.concat(
        [
            pd.read_csv(f"{OALEX_PATH}/works_{discipline}_{year}_augmented.csv")
            for year in year_list
        ]
    ).reset_index(drop=True)


def work_concepts(discipline: str, concept: str, year_list: list) -> pd.DataFrame:
    """Reads the concepts associated to openalex works

    Args:
        discipline: The discipline of the work (AI or genetics)
        concept: whether we are collecting OpenAlex concepts or mesh terms
        year_list: publication years

    Returns:
        A dataframe looking up works and concepts

    """

    return pd.concat(
        [
            pd.read_csv(f"{OALEX_PATH}/{concept}_{discipline}_{year}.csv")
            for year in year_list
        ]
    ).reset_index(drop=True)


def work_authorship(discipline: str, year_list: list) -> pd.DataFrame:
    """
    Reads the authors and institutions associated with an openalex work

    Args:
        discipline: The discipline of the work (AI or genetics)
        year_list: publication years

    Returns:
        A dataframe with authors and institution ids
    """

    return pd.concat(
        [
            pd.read_csv(f"{OALEX_PATH}/authorships_{discipline}_{year}.csv")
            for year in year_list
        ]
    ).reset_index(drop=True)


def instit_metadata() -> pd.DataFrame:
    """Read institution metadata"""

    return pd.read_csv(f"{OALEX_PATH}/oalex_institutions_meta.csv")


def work_abstracts(discipline: str, years: List) -> Dict:
    """Reads the abstracts for a list of years"""

    return reduce(
        lambda a, b: dict(a, **b),
        [
            read_json(f"{OALEX_PATH}/abstracts_{discipline}_{year}.json")
            for year in years
        ],
    )


def get_openalex_ai_genomics_works() -> pd.DataFrame:
    """Returns dataframe of AI in genomics OpenAlex works"""
    try:
        return pd.read_csv(
            f"{PROJECT_DIR}/outputs/ai_genomics_provisional_dataset.csv", index_col=0
        ).drop(columns=["index"])
    except FileNotFoundError:
        logger.error(
            "FileNotFoundError: To create the missing file, run ai_genomics/analysis/openalex_definition.py"
        )


def get_openalex_abstracts(field: str, year: int) -> pd.DataFrame:
    """Returns dataframe of abstracts of specified field and year

    Args:
        field: "artificial_intelligence" or "genetics"
        year: Year e.g 2017

    Returns:
        Dataframe with columns work_id, abstract
    """
    return (
        pd.read_json(
            f"{PROJECT_DIR}/inputs/data/openalex/abstracts_{field}_{year}.json",
            orient="index",
        )
        .reset_index()
        .rename(columns={"index": "work_id", 0: "abstract"})
    )
