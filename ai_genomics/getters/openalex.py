import json
from typing import List, Dict, Any

from ai_genomics.utils.reading import read_json
from ai_genomics import PROJECT_DIR


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


def get_year_summary(
    subject: str = "artificial_intelligence",
) -> Dict[int, Dict[str, Any]]:
    """Reads a summary of results for a year

    Args:
        subject: The subject of the docs (AI or genetics)

    Returns:
        A list where every element is a dict with results for a year

    """

    with open(
        f"{PROJECT_DIR}/outputs/data/openalex/{subject}_year_summaries.json", "r"
    ) as infile:
        return json.load(infile)
