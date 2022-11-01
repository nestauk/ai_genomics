"""Script to generate look up tables across datasets (ai and genomics, ai baseline, genomics baseline)
where output is [{id: X, abstract: Y}] for DBpedia tagging.

python ai_genomics/pipeline/entity_extraction/generate_lookups.py
"""
import pandas as pd
import os
from typing import Dict, List

from ai_genomics import PROJECT_DIR, bucket_name, logger
from ai_genomics.getters.data_getters import load_s3_data, save_to_s3
from ai_genomics.getters.patents import (
    get_ai_sample_patents,
    get_genomics_sample_patents,
    get_ai_genomics_patents,
)
import json
import boto3
from boto3.s3.transfer import TransferConfig

s3_client = boto3.client("s3")

CB_DATA = load_s3_data(bucket_name, "outputs/crunchbase/crunchbase_ai_genom_comps.csv")
GTR_DATA = load_s3_data(bucket_name, "outputs/gtr/gtr_ai_genomics_projects.csv")

OA_ABSTRACTS = load_s3_data(
    bucket_name, "outputs/openalex/ai_genomics_openalex_abstracts.json"
)

AI_PATENTS, GENOMICS_PATENTS, AI_GENOMICS_PATENTS = (
    get_ai_sample_patents(),
    get_genomics_sample_patents(),
    get_ai_genomics_patents(),
)

OA_LOOKUP_LOCAL_PATH = str(PROJECT_DIR) + "/outputs/data/openalex/"
LOOKUP_TABLE_PATH = "inputs/lookup_tables/"
VALID_DF_TYPES = ["ai", "genomics", "ai_genomics"]

config = TransferConfig(
    multipart_threshold=1024 * 25,
    max_concurrency=10,
    multipart_chunksize=1024 * 25,
    use_threads=True,
)


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
            .dropna(subset=["abstract_text"])[["publication_number", "abstract_text"]]
            .rename(columns={"publication_number": "id", "abstract_text": "abstract"})
            .T.to_dict()
        )
        patent_lookups.append(list(patent_lookup.values()))

    for patent_name, patent_lookup in zip(
        ("ai", "genomics", "ai_genomics"), patent_lookups
    ):
        save_to_s3(
            bucket_name,
            patent_lookup,
            os.path.join(LOOKUP_TABLE_PATH, f"{patent_name}_patents_lookup.json"),
        )

    # generate and save cb look ups across ai + genomics, ai baseline and genomics baseline
    assert pd.Series(CB_DATA.id).is_unique == True, "not every id is unique"
    cb_lookup = list(
        CB_DATA[["id", "description_combined"]]
        .rename(columns={"description_combined": "abstract"})
        .T.to_dict()
        .values()
    )
    save_to_s3(
        bucket_name, cb_lookup, os.path.join(LOOKUP_TABLE_PATH, "cb_lookup.json")
    )

    # generate and save gtr look ups across ai + genomics, ai baseline and genomics baseline gtr
    assert pd.Series(GTR_DATA.id).is_unique == True, "not every id is unique"
    gtr_lookup = list(
        GTR_DATA[["id", "abstract_text"]]
        .rename(columns={"abstract_text": "abstract"})
        .T.to_dict()
        .values()
    )
    save_to_s3(
        bucket_name, gtr_lookup, os.path.join(LOOKUP_TABLE_PATH, "gtr_lookup.json")
    )

    # generate openalex look ups across ai + genomics, ai baseline and genomics baseline openalex
    assert len(OA_ABSTRACTS.keys()) == len(
        set(OA_ABSTRACTS.keys())
    ), "not every id is unique"
    bad_abstracts = [
        "No abstract.",
        "X",
        "en",
        "None",
        "N/A",
        "nema",
        "ABSTRACT",
        ".",
        "■■■",
        "x",
        "Abstract",
        "n/a",
        "Removed.",
        ":",
        "-- / --",
        "--/--",
        "-",
        "N/a",
        "<p />",
        "/",
    ]

    oa_abstracts_clean_list = []
    oa_abstracts_clean = {}
    for id_, abstract in OA_ABSTRACTS.items():
        if abstract not in bad_abstracts:
            oa_abstracts_clean["id"] = id_
            oa_abstracts_clean["abstract"] = abstract
        oa_abstracts_clean_list.append(oa_abstracts_clean)

    # when I try to run the script, I'm getting an error message that file is greater than 5GB so will need to download locally
    # then push to s3 with different config

    if not os.path.isdir(OA_LOOKUP_LOCAL_PATH):
        os.mkdir(OA_LOOKUP_LOCAL_PATH)

    with open(OA_LOOKUP_LOCAL_PATH + "oa_lookup.json", "w") as f:
        json.dump(oa_abstracts_clean_list, f)
    logger.info("loaded oa_lookup to file.")

    # Upload to bucket-name at key-name with config
    s3_client.upload_file(
        OA_LOOKUP_LOCAL_PATH + "oa_lookup.json",
        bucket_name,
        oa_abstracts_clean,
        os.path.join(LOOKUP_TABLE_PATH, "ai_genomics_oa_lookup.json"),
    )
    logger.info("uploaded oa_lookup to s3")
