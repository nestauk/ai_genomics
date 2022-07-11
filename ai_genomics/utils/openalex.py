# Functions to process OpenAlex data
from itertools import chain
from typing import Dict, List

from pandas import DataFrame
from toolz import pipe, partial

from ai_genomics import get_yaml_config, _base_config_path

CONCEPT_THRES = get_yaml_config(_base_config_path)["concept_threshold"]


def deinvert_abstract(inverted_abstract: Dict[str, List]) -> str:
    """Convert inverted abstract into normal abstract

    Args:
        inverted_abstract: a dict where the keys are words
        and the values lists of positions

    Returns:
        A str that reconstitutes the abstrat

    """

    abstr_empty = (max(chain(*inverted_abstract.values())) + 1) * [None]

    for word, pos in inverted_abstract.items():
        for p in pos:
            abstr_empty[p] = word

    return " ".join(abstr_empty)


def get_institutions(doc: dict) -> list:
    """
    Create a list of all institutions involved in a work
    """

    return [
        inst["id"]
        for auth in doc["authorships"]
        for inst in auth["institutions"]
        if len(inst) > 0
    ]


def enrich_institutions(doc_inst: list, insts_dict: dict, variable: str):
    """
    Create institutional variables for a list of institutions
    involved in a work
    """

    return [insts_dict[inst][variable] if inst != None else None for inst in doc_inst]


def get_concepts(doc: dict, thres: float = CONCEPT_THRES) -> list:
    """Create a list of concepts associated with a work

    Args:
        doc: OA work
        thres: minimum threshold for inclusion

    """

    return [
        concept["display_name"]
        for concept in doc["concepts"]
        if float(concept["score"]) > thres
    ]


def make_inst_lookup(inst_dict: dict, variable: str):
    """Create a lookup between inst id and variable

    Args:
        inst_dict: a lookup between inst ids and their metadata
        variable: the variable to look up

    """
    return {key: value[variable] for key, value in inst_dict.items()}


def work_metadata_df(
    work_list: list,
    inst_list: list,
    meta_vars: list = ["country_code", "type", "display_name"],
) -> DataFrame:
    """
    Creates a dataframe with work metadata

    Args:
        work_list: a list of OA works
        inst_list: a list of OA institutions
        meta_vars: list of metadata variables to include in the dataframe

    Returns:
        inst_df: A dataframe where rows are works and columns are metadata
            of interest
    """

    inst_dict = {inst["id"]: inst for inst in inst_list}

    inst_df = (
        pipe(
            [
                (doc["id"], doc["publication_year"], get_institutions(doc))
                for doc in work_list
            ],
            partial(DataFrame, columns=["work_id", "year", "institution"]),
        )
        .explode("institution")
        .reset_index(drop=True)
    )

    # We create a column for every new institutional metadata variable
    for var in meta_vars:
        inst_df[var] = inst_df["institution"].map(make_inst_lookup(inst_dict, var))

    return inst_df


if __name__ == "__main__":
    import logging
    from ai_genomics.getters.openalex import (
        get_openalex_concepts,
        get_openalex_works,
        get_openalex_instits,
    )
    from ai_genomics.utils.openalex import deinvert_abstract, work_metadata_df

    logging.info("Checking functions")

    logging.info("Checking getters")
    conc = get_openalex_concepts()
    insts = get_openalex_instits()
    works = get_openalex_works()

    logging.info("Checking function to produce metadata df \n")

    work_meta = work_metadata_df(works, insts)
    logging.info(work_meta.head(n=30))

    logging.info("Checking deinvert abstracts function \n")
    logging.info(deinvert_abstract(works[0]["abstract_inverted_index"]))
