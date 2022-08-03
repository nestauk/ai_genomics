# Creates some summary statistics for the openalex data by year

import json
import logging
import os

import boto3
import pandas as pd

import ai_genomics.utils.openalex as oalex
from ai_genomics import PROJECT_DIR
from ai_genomics.getters.openalex import get_openalex_instits


OA_NAME_ID_LOOKUP = {"artificial_intelligence": "C154945302", "genetics": "C54355233"}
OALEX_PATH = f"{PROJECT_DIR}/outputs/data/openalex"

if __name__ == "__main__":

    os.makedirs(OALEX_PATH, exist_ok=True)

    s3 = boto3.resource("s3")
    ai_genomics_bucket = s3.Bucket("ai-genomics")

    inst_dict = {inst["id"]: inst for inst in get_openalex_instits()}

    for concept_name in ["artificial_intelligence", "genetics"]:
        logging.info(concept_name)
        year_summaries = {
            year: oalex.year_summary(
                oalex.fetch_openalex(ai_genomics_bucket, concept_name, year), inst_dict
            )
            for year in range(2007, 2022)
        }

        with open(f"{OALEX_PATH}/{concept_name}_year_summaries.json", "w") as outfile:
            json.dump(year_summaries, outfile)
