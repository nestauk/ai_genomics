"""Script to generate look up tables across datasets (ai and genomics, ai baseline, genomics baseline)
where output is {id: abstract} for DBpedia tagging.

python ai_genomics/pipeline/entity_extraction/generate_lookups.py
"""
import pandas as pd
import os
from typing import Dict, List

from ai_genomics import bucket_name
from ai_genomics.getters.data_getters import load_s3_data, save_to_s3
from ai_genomics.getters.patents import (
    get_ai_sample_patents,
    get_genomics_sample_patents,
    get_ai_genomics_patents,
)

CB_DATA = load_s3_data(bucket_name, "outputs/crunchbase/crunchbase_ai_genom_comps.csv")
GTR_DATA = load_s3_data(bucket_name, "outputs/gtr/gtr_ai_genomics_projects.csv")

OA_ABSTRACTS = load_s3_data(bucket_name, "outputs/openalex/openalex_abstracts.json")

AI_PATENTS, GENOMICS_PATENTS, AI_GENOMICS_PATENTS = (
    get_ai_sample_patents(),
    get_genomics_sample_patents(),
    get_ai_genomics_patents(),
)

LOOKUP_TABLE_PATH = "inputs/lookup_tables/"
VALID_DF_TYPES = ["ai", "genomics", "ai_genomics"]


def save_lookups(lookups: List[Dict]):
    """For a given list of lookups per data source (ai, genomics, ai_genomics), save lookups
    to s3."""
    for name, lookup in zip(VALID_DF_TYPES, lookups):
        lookup_table_name = os.path.join(
            LOOKUP_TABLE_PATH, f"{name}_patents_lookup.json"
        )
        save_to_s3(bucket_name, lookup, lookup_table_name)


if __name__ == "__main__":

    # generate patents look ups across ai + genomics, ai baseline and genomics baseline patents
    patent_lookups = []
    for lookup in (AI_PATENTS, GENOMICS_PATENTS, AI_GENOMICS_PATENTS):
        assert (
            pd.Series(lookup.publication_number).is_unique == True
        ), "not every id is unique"
        patent_lookup = (
            lookup.query("abstract_language == 'en'")
            .set_index("publication_number")["abstract_text"]
            .to_dict()
        )
        patent_lookups.append(patent_lookup)

    save_lookups(patent_lookups)

    # generate and save cb look ups across ai + genomics, ai baseline and genomics baseline
    assert pd.Series(CB_DATA.id).is_unique == True, "not every id is unique"
    cb_lookup = CB_DATA.set_index("id")["description_combined"].to_dict()
    save_to_s3(
        bucket_name, cb_lookup, os.path.join(LOOKUP_TABLE_PATH, "cb_lookup.json")
    )

    # generate and save gtr look ups across ai + genomics, ai baseline and genomics baseline gtr
    assert pd.Series(GTR_DATA.id).is_unique == True, "not every id is unique"
    cb_lookup = GTR_DATA.set_index("id")["abstract_text"].to_dict()
    save_to_s3(
        bucket_name, cb_lookup, os.path.join(LOOKUP_TABLE_PATH, "gtr_lookup.json")
    )

    # generate openalex look ups across ai + genomics, ai baseline and genomics baseline openalex
    assert len(OA_ABSTRACTS.keys()) == len(
        set(OA_ABSTRACTS.keys())
    ), "not every id is unique"
    save_to_s3(
        bucket_name, OA_ABSTRACTS, os.path.join(LOOKUP_TABLE_PATH, "oa_lookup.json")
    )
