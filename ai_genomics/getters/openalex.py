import json

import pandas as pd
from typing import List, Dict, Any, Mapping, Union
from functools import reduce
from toolz import pipe

from ai_genomics.utils.reading import read_json
from ai_genomics.getters.data_getters import load_s3_data
from ai_genomics import PROJECT_DIR, logger, bucket_name

OALEX_PATH = f"{PROJECT_DIR}/inputs/data/openalex"
OALEX_OUT_PATH = f"{PROJECT_DIR}/outputs/openalex"


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


def ai_genom_getter(
    filename: str, format: str = "csv", local: bool = True
) -> pd.DataFrame:
    """Returns dataframe of AI in genomics OpenAlex works"""

    if local:
        if format == "csv":
            return pd.read_csv(f"{OALEX_OUT_PATH}/{filename}.csv")
        else:
            with open(f"{OALEX_OUT_PATH}/{filename}.json") as infile:
                return json.load(infile)
    else:
        return load_s3_data("ai-genomics", f"outputs/openalex/{filename}.{format}")


def get_openalex_ai_genomics_works(local: bool = True) -> pd.DataFrame:
    """Returns dataframe of in scope AI in genomics OpenAlex works."""

    return ai_genom_getter("ai_genomics_openalex_works", "csv", local)


def get_openalex_works(local: bool = True) -> pd.DataFrame:
    """Returns dataframe of AI in genomics OpenAlex works."""

    return ai_genom_getter("openalex_works", "csv", local)


def get_openalex_ai_genomics_concepts(local: bool = True) -> pd.DataFrame:
    """Returns dataframe of AI in genomics OpenAlex concepts"""

    return ai_genom_getter("openalex_concepts", "csv", local)


def get_openalex_ai_genomics_mesh(local: bool = True) -> pd.DataFrame:
    """Returns dataframe of AI in genomics OpenAlex MeSH terms"""

    return ai_genom_getter("openalex_mesh", "csv", local)


def get_openalex_ai_genomics_institutes(local: bool = True) -> pd.DataFrame:
    """Returns dataframe of AI in genomics OpenAlex MeSH institutes/authorships"""

    return ai_genom_getter("openalex_institutes", "csv", local)


def get_openalex_ai_genomics_abstracts(local: bool = True) -> Dict:
    """Returns dataframe of in scope AI in genomics OpenAlex abstracts"""

    return ai_genom_getter("ai_genomics_openalex_abstracts", "json", local)


def get_openalex_abstracts(local: bool = True) -> Dict:
    """Returns dataframe of all OpenAlex abstracts"""

    return ai_genom_getter("openalex_abstracts", "json", local)


def get_openalex_disc_influence() -> pd.DataFrame:
    """Returns dict with discipline influence scores for each paper"""

    return (
        pd.DataFrame(
            load_s3_data(
                "ai-genomics", "outputs/analysis/openalex_influence_score.json"
            )
        )
        .T.stack()
        .reset_index(name="disc_influence")
        .rename(columns={"level_0": "work_id", "level_1": "category"})
    )


def get_openalex_entities() -> Mapping[str, Mapping[str, Union[str, str]]]:
    """From S3 loads ai genomics oa entities"""
    return load_s3_data(
        bucket_name,
        "outputs/entity_extraction/oa_lookup_clean.json",
    )


def get_openalex_ai_entities() -> Mapping[str, Mapping[str, Union[str, str]]]:
    """From S3 loads ai oa entities"""
    return load_s3_data(
        bucket_name,
        "outputs/entity_extraction/oa_ai_lookup_clean.json",
    )


def get_openalex_ai_genomics_entities() -> Mapping[str, Mapping[str, Union[str, str]]]:
    """From S3 loads ai genomics oa entities"""
    return load_s3_data(
        bucket_name,
        "outputs/entity_extraction/ai_genomics_oa_lookup_clean.json",
    )


def get_openalex_entities_sample() -> Mapping[str, Mapping[str, Union[str, str]]]:
    """From S3 loads oa entities sample"""
    return load_s3_data(
        bucket_name,
        "outputs/entity_extraction/oa_lookup_clean_sample.json",
    )


def get_openalex_ai_genomics_works_entity_groups(k: int = 500) -> pd.DataFrame:
    """Gets a dataframe of vectors representing the presence of DBpedia entity
    clusters in each document.

    Args:
        k (int, optional): The number of clusters. Defaults to 500.

    Returns:
        pd.DataFrame: A sparse dataframe where the index is work IDs and
            the columns are vector dimensions (entity cluster IDs).
    """
    fname = f"inputs/entities/openalex_entity_group_vectors_k_{k}.csv"
    return load_s3_data(bucket_name, fname)


def get_openalex_ai_genomics_works_embeddings() -> pd.DataFrame:
    """Gets an array of abstract embeddings and the associated work IDs.

    Returns:
        pd.DataFrame: Abstract embeddings and the associated work IDs.
    """
    fname = "inputs/embedding/oa_ai_genomics_embeddings.csv"
    return load_s3_data(bucket_name, fname)
