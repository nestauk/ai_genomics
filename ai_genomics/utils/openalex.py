# Functions to fetch and process OpenAlex data
import json
import logging
from collections import Counter
from itertools import chain
from typing import Dict, List, Any
from pandas import DataFrame
from toolz import pipe, partial


import ai_genomics.utils.openalex as oalex
from ai_genomics import get_yaml_config, _base_config_path

CONCEPT_THRES = get_yaml_config(_base_config_path)["concept_threshold"]

OA_NAME_ID_LOOKUP = {"artificial_intelligence": "C154945302", "genetics": "C54355233"}


def fetch_openalex(
    s3_bucket,
    concept_name: str = "artificial_intelligence",
    year: int = 2007,
) -> List[Dict]:
    """Fetch a json object
    Args:
        s3_bucket where we store the data
        concept_name: The name of the concept
        year: the year

    Returns:
    """
    logging.info(f"Fetching {concept_name} for year {year}")

    return pipe(
        f"inputs/openalex/{concept_name}/openalex-works_production-True_concept-{OA_NAME_ID_LOOKUP[concept_name]}_year-{year}.json",
        s3_bucket.Object,
        lambda _object: _object.get()["Body"].read().decode(),
        json.loads,
    )


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


def make_work_metadata(
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

    logging.info("Checking functions")

    logging.info("Checking getters")
    conc = get_openalex_concepts()
    insts = get_openalex_instits()
    works = get_openalex_works()

    logging.info("Checking function to produce metadata df \n")

    work_meta = make_work_metadata(works, insts)
    logging.info(work_meta.head(n=30))

    logging.info("Checking deinvert abstracts function \n")
    logging.info(deinvert_abstract(works[0]["abstract_inverted_index"]))


def year_summary(docs: list, inst_dict: dict) -> Dict[str, Any]:
    """Produces a summary of document activity in a year
    Args:
        docs: list of documents published in the year
        inst_dict: lookup between institution ids and their metadata

    Returns:
        A dict with metadata about the papers published in the year
    """

    meta_dict = {}  # type: Dict[str, Any]

    meta_dict["n"] = len(docs)

    meta_dict["ids"] = list([work["id"] for work in docs])

    meta_dict["concepts"] = Counter(chain(*[oalex.get_concepts(work) for work in docs]))

    institution_list = list(chain(*[oalex.get_institutions(work) for work in docs]))

    meta_dict["institution_names"] = Counter(
        oalex.enrich_institutions(institution_list, inst_dict, "display_name")
    )

    meta_dict["institution_countries"] = Counter(
        oalex.enrich_institutions(institution_list, inst_dict, "country_code")
    )

    return meta_dict
