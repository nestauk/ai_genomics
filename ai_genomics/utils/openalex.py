# Functions to fetch and process OpenAlex data
import json
import logging
from collections import Counter
from itertools import chain
from typing import Dict, List, Any, Union
import boto3
import pandas as pd
from toolz import pipe, partial


from ai_genomics import config

CONCEPT_THRES = config["concept_threshold"]

INST_META_VARS, WORK_META_VARS, VENUE_META_VARS = [
    config[var_list]
    for var_list in ["inst_meta_vars", "work_meta_vars", "venue_meta_vars"]
]

MESH_VARS = ["descriptor_ui", "descriptor_name", "qualifier_name"]

OA_NAME_ID_LOOKUP = {"artificial_intelligence": "C154945302", "genetics": "C54355233"}


def fetch_openalex(
    concept_name: str,
    year: int,
) -> List[Dict]:
    """Fetch a json object
    Args:
        s3_bucket where we store the data
        concept_name: The name of the concept
        year: the year

    Returns:
    """
    s3 = boto3.resource("s3")
    ai_genomics_bucket = s3.Bucket("ai-genomics")

    logging.info(f"Fetching {concept_name} for year {year}")

    return pipe(
        f"inputs/openalex/{concept_name}/openalex-works_production-True_concept-{OA_NAME_ID_LOOKUP[concept_name]}_year-{year}.json",
        ai_genomics_bucket.Object,
        lambda _object: _object.get()["Body"].read().decode(),
        json.loads,
    )


def deinvert_abstract(inverted_abstract: Dict[str, List]) -> Union[str, None]:
    """Convert inverted abstract into normal abstract

    Args:
        inverted_abstract: a dict where the keys are words
        and the values lists of positions

    Returns:
        A str that reconstitutes the abstract or None if the deinvered abstract
        is empty

    """

    if len(inverted_abstract) == 0:
        return None
    else:

        abstr_empty = (max(chain(*inverted_abstract.values())) + 1) * [""]

        for word, pos in inverted_abstract.items():
            for p in pos:
                abstr_empty[p] = word

        return " ".join(abstr_empty)


def extract_obj_meta(oalex_object: Dict, meta_vars: List) -> Dict:
    """Extracts variables of interest from an OpenAlex object (eg work, insitution...)

    Args:
        oalex_object: an OpenAlex object
        meta_vars: a list of variables to extract

    Returns:
        A dict with the variables of interest
    """

    return {var: val for var, val in oalex_object.items() if var in meta_vars}


def extract_work_venue(work: Dict, venue_vars: List) -> Dict:
    """Extracts nested metadata about a publication venue

    Args:
        work: an OpenAlex work
        venue_vars: a list of variables to extract

    Returns:
        A dict with the variables of interest
    """

    return {
        f"venue_{var}": val
        for var, val in work["host_venue"].items()
        if var in venue_vars
    }


def make_inst_metadata(inst_list: List, meta_vars: List) -> pd.DataFrame:
    """Makes a df with metadata about oalex institutions

    Args:
        doc_list: list of oalex institutions
        meta_vars: list of variables to extract

    Returns
        A df with instit-level metadata
    """

    return pipe(
        inst_list,
        lambda list_of_dicts: [
            extract_obj_meta(
                d,
                meta_vars=meta_vars,
            )
            for d in list_of_dicts
        ],
        pd.DataFrame,
    )


def make_work_metadata(work: Dict) -> Dict:
    """Extracts metadata about a work"""

    return {
        **extract_obj_meta(work, meta_vars=WORK_META_VARS),
        **extract_work_venue(work, venue_vars=VENUE_META_VARS),
    }


def make_work_corpus_metadata(works_list: List) -> pd.DataFrame:
    """Makes a df with work metadata

    Args:
        work_list: list of oalex works

    Returns:
        A df with work-level metadata
    """

    return pipe(
        works_list,
        lambda list_of_dicts: [make_work_metadata(pd.Series(d)) for d in list_of_dicts],
        pd.DataFrame,
    ).rename(columns={"id": "work_id"})


def get_nested_vars(work: Dict, variable: str, keys_to_keep: List) -> Union[None, List]:
    """Extracts nested variables from a document

    Args:
        doc: an open alex work
        nested_variable: the nested variable to extract
        keys_to_keep: the keys to keep in the nested variable

    Returns:
        A list of dicts with the nested variables
    """

    if variable not in work.keys():
        return None
    else:
        return [
            {
                **{"doc_id": work["id"]},
                **{k: v for k, v in conc.items() if k in keys_to_keep},
            }
            for conc in work[variable]
        ]


def make_work_concepts(
    works_list: List,
    variable: str = "concepts",
    keys_to_keep: List = ["id", "display_name", "score"],
) -> pd.DataFrame:
    """
    Extracts concepts from work (could be openalex or mesh)

    Args:
        doc_list: list of openalex
        variable: concept variable to extract
        keys_to_keep: keys to keep in the concept

    Returns:
        A df with work-level concepts

    """

    return pipe(
        works_list,
        lambda doc_list: [
            get_nested_vars(doc, variable=variable, keys_to_keep=keys_to_keep)
            for doc in doc_list
        ],
        lambda dict_list: pd.DataFrame(chain(*dict_list)),
    )


def get_authorships(work: Dict) -> List:
    """
    Extract authorships from a document

    Args:
        work: an openalex

    Returns:
        A list of parsed dicts with the authors and their affiliations
    """
    return list(
        chain(
            *[
                [
                    {
                        **{"id": work["id"]},
                        **{f"auth_{k}": v for k, v in auth["author"].items()},
                        **{"affiliation_string": auth["raw_affiliation_string"]},
                        **{f"inst_{k}": v for k, v in inst.items() if k == "id"},
                    }
                    for inst in auth[
                        "institutions"
                    ]  # Some authors are affiliated to more than
                    # one institution.
                ]
                for auth in work["authorships"]
            ]
        )
    )


def make_work_authorships(works_list: List) -> pd.DataFrame:
    """
    Creates a df with authors and institutions per works

    Args:
        works_list: list of openalex works
    """

    return pipe(
        works_list,
        lambda list_of_docs: [get_authorships(doc) for doc in list_of_docs],
        lambda extracted: pd.DataFrame(chain(*extracted)),
    )


def make_citations(work_list: List) -> Dict:
    """Dict with the papers cited by each work"""

    return {doc["id"]: doc["referenced_works"] for doc in work_list}


def make_deinverted_abstracts(work_list: List) -> Dict:
    """Dict with the deinverted abstracts of each work (where available"""

    return {
        doc["id"]: deinvert_abstract(doc["abstract_inverted_index"])
        if (type(doc["abstract_inverted_index"]) == dict)
        else None
        for doc in work_list
    }


if __name__ == "__main__":
    import logging

    from ai_genomics.getters.openalex import get_openalex_instits

    logging.info("getting data")
    instits = get_openalex_instits()
    works = fetch_openalex("artificial_intelligence", 2007)

    logging.info("Checking institutions")
    inst = make_inst_metadata(instits, INST_META_VARS)

    logging.info(inst.head())

    logging.info("Checking works")
    works_df = make_work_corpus_metadata(works)

    logging.info(works_df.head())

    logging.info("Checking authorships")
    authorships = make_work_authorships(works)
    logging.info(authorships.head())

    logging.info("Checking concepts")
    oa_concepts = make_work_concepts(works, variable="concepts")

    logging.info(oa_concepts.head())

    mesh_subjects = make_work_concepts(works, variable="mesh", keys_to_keep=MESH_VARS)

    logging.info(mesh_subjects.head())

    logging.info("Checking citations")
    logging.info(list(make_citations(works).values())[0])

    logging.info("checking deinverted abstracts")
    logging.info(list(make_deinverted_abstracts(works).values())[0])
