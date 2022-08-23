# Processes and saves the openalex data

import json
import logging
import os

from toolz import pipe

from ai_genomics.utils import openalex
from ai_genomics import PROJECT_DIR
from ai_genomics.getters.openalex import get_openalex_instits


OA_NAME_ID_LOOKUP = {"artificial_intelligence": "C154945302", "genetics": "C54355233"}
OALEX_PATH = f"{PROJECT_DIR}/inputs/data/openalex"


def fetch_save_year(concept_name: str, year: int, make_df: bool = True):
    """Fetch and save the openalex data for a given concept and year
    Args:
        concept_name: The name of the concept
        year: the year
    """
    if os.path.exists(f"{OALEX_PATH}/works_{concept_name}_{year}.csv"):
        logging.info(f"{concept_name}_{year} already exists")
        return

    oalex_works = openalex.fetch_openalex(concept_name, year)

    logging.info("Processing and saving")
    # Works
    (
        openalex.make_work_corpus_metadata(oalex_works).to_csv(
            f"{OALEX_PATH}/works_{concept_name}_{year}.csv", index=False
        )
    )

    # concepts

    if make_df:

        (
            openalex.make_work_concepts(oalex_works)
            .assign(year=year)
            .to_csv(f"{OALEX_PATH}/concepts_{concept_name}_{year}.csv", index=False)
        )

    else:
        with open(f"{OALEX_PATH}/concepts_{concept_name}_{year}.json", "w") as outfile:
            json.dump(openalex.make_work_concepts(oalex_works, make_df), outfile)

    # mesh
    (
        openalex.make_work_concepts(
            oalex_works, variable="mesh", keys_to_keep=openalex.MESH_VARS
        )
        .assign(year=year)
        .to_csv(f"{OALEX_PATH}/mesh_{concept_name}_{year}.csv", index=False)
    )

    # Authorships
    (
        openalex.make_work_authorships(oalex_works)
        .assign(year=year)
        .to_csv(f"{OALEX_PATH}/authorships_{concept_name}_{year}.csv", index=False)
    )

    # Citations
    with open(f"{OALEX_PATH}/citations_{concept_name}_{year}.json", "w") as outfile:
        json.dump(openalex.make_citations(oalex_works), outfile)

    # Deinverted abstracts
    with open(f"{OALEX_PATH}/abstracts_{concept_name}_{year}.json", "w") as outfile:
        json.dump(openalex.make_deinverted_abstracts(oalex_works), outfile)


if __name__ == "__main__":

    os.makedirs(OALEX_PATH, exist_ok=True)

    pipe(
        get_openalex_instits(),
        partial(openalex.make_inst_metadata, meta_vars=openalex.INST_META_VARS),
    ).to_csv(f"{OALEX_PATH}/oalex_institutions_meta.csv", index=False)

    for year in range(2007, 2022):
        for concept_name in ["artificial_intelligence", "genetics"]:

            make_df = (
                True
                if (year == 2021) & (concept_name == "artificial_intelligence")
                else True
            )

            fetch_save_year(concept_name, year, make_df)
