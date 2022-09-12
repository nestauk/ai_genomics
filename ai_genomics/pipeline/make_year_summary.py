# Processes and saves the openalex data

import json
import logging
import os
from functools import partial

from toolz import pipe
from metaflow import FlowSpec, step, batch

from ai_genomics.utils import openalex
from ai_genomics import PROJECT_DIR
from ai_genomics.getters.openalex import get_openalex_instits


OALEX_PATH = f"{PROJECT_DIR}/inputs/data/openalex/"


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

    # Concepts

    if make_df:

        (
            openalex.make_work_concepts(oalex_works)
            .assign(year=year)
            .to_csv(f"{OALEX_PATH}/concepts_{concept_name}_{year}.csv", index=False)
        )

    else:
        with open(f"{OALEX_PATH}/concepts_{concept_name}_{year}.json", "w") as outfile:
            json.dump(openalex.make_work_concepts(oalex_works, make_df), outfile)

    # Mesh
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


class MakeYearSummaryFlow(FlowSpec):
    @step
    def start(self):
        self.next(self.make_directories)

    @step
    def make_directories(self):
        os.makedirs(OALEX_PATH, exist_ok=True)
        pipe(
            get_openalex_instits(),
            partial(openalex.make_inst_metadata, meta_vars=openalex.INST_META_VARS),
        ).to_csv(f"{OALEX_PATH}/oalex_institutions_meta.csv", index=False)
        self.years = list(range(2007, 2022))
        self.next(self.fetch_save_year, foreach="years")

    @step
    def fetch_save_year(self):
        for concept_name in ["artificial_intelligence", "genetics"]:
            fetch_save_year(concept_name, self.input, make_df=True)
        self.next(self.join)

    @step
    def join(self, inputs):
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    MakeYearSummaryFlow()
