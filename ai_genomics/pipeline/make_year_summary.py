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
from ai_genomics.getters.data_getters import save_to_s3


OALEX_PATH = f"{PROJECT_DIR}/inputs/data/openalex/"


def fetch_save_year(concept_name: str, year: int, make_df: bool = True):
    """Fetch and save the openalex data for a given concept and year
    Args:
        concept_name: The name of the concept
        year: the year
    """
    oalex_works = openalex.fetch_openalex(concept_name, year)

    logging.info("Processing and saving")
    # Works
    # (
    #     openalex.make_work_corpus_metadata(oalex_works).to_csv(
    #         f"{OALEX_PATH}/works_{concept_name}_{year}.csv", index=False
    #     )
    # )
    works_corpus_metadata = openalex.make_work_corpus_metadata(oalex_works)
    save_to_s3(
        "ai-genomics",
        works_corpus_metadata,
        f"inputs/data/openalex/works_{concept_name}_{year}.csv",
    )

    # Concepts

    if make_df:

        # (
        #     openalex.make_work_concepts(oalex_works)
        #     .assign(year=year)
        #     .to_csv(f"{OALEX_PATH}/concepts_{concept_name}_{year}.csv", index=False)
        # )
        work_concepts = openalex.make_work_concepts(oalex_works).assign(year=year)
        save_to_s3(
            "ai-genomics",
            work_concepts,
            f"inputs/data/openalex/concepts_{concept_name}_{year}.csv",
        )

    else:
        # with open(f"{OALEX_PATH}/concepts_{concept_name}_{year}.json", "w") as outfile:
        #     json.dump(openalex.make_work_concepts(oalex_works, make_df), outfile)
        work_concepts = openalex.make_work_concepts(oalex_works, make_df)
        save_to_s3(
            "ai-genomics",
            work_concepts,
            f"inputs/data/openalex/concepts_{concept_name}_{year}.json",
        )

    # Mesh
    # (
    #     openalex.make_work_concepts(
    #         oalex_works, variable="mesh", keys_to_keep=openalex.MESH_VARS
    #     )
    #     .assign(year=year)
    #     .to_csv(f"{OALEX_PATH}/mesh_{concept_name}_{year}.csv", index=False)
    # )
    openalex_concepts = openalex.make_work_concepts(
        oalex_works, variable="mesh", keys_to_keep=openalex.MESH_VARS
    ).assign(year=year)
    save_to_s3(
        "ai-genomics",
        openalex_concepts,
        f"inputs/data/openalex/mesh_{concept_name}_{year}.csv",
    )

    # Authorships
    # (
    #     openalex.make_work_authorships(oalex_works)
    #     .assign(year=year)
    #     .to_csv(f"{OALEX_PATH}/authorships_{concept_name}_{year}.csv", index=False)
    # )
    work_authorships = openalex.make_work_authorships(oalex_works)
    work_authorships.assign(year=year)
    save_to_s3(
        "ai-genomics",
        work_authorships,
        f"inputs/data/openalex/authorships_{concept_name}_{year}.csv",
    )

    # Citations
    # with open(f"{OALEX_PATH}/citations_{concept_name}_{year}.json", "w") as outfile:
    #     json.dump(openalex.make_citations(oalex_works), outfile)
    citations_json = openalex.make_citations(oalex_works)
    save_to_s3(
        "ai-genomics",
        citations_json,
        f"inputs/data/openalex/citations_{concept_name}_{year}.json",
    )

    # Deinverted abstracts
    # with open(f"{OALEX_PATH}/abstracts_{concept_name}_{year}.json", "w") as outfile:
    #     json.dump(openalex.make_deinverted_abstracts(oalex_works), outfile)
    deinverted_abstracts_json = openalex.make_deinverted_abstracts(oalex_works)
    save_to_s3(
        "ai-genomics",
        deinverted_abstracts_json,
        f"inputs/data/openalex/abstracts_{concept_name}_{year}.json",
    )


class MakeYearSummaryFlow(FlowSpec):
    @step
    def start(self):
        self.next(self.save_institutions_meta)

    @step
    def save_institutions_meta(self):
        save_to_s3(
            "ai-genomics",
            pipe(
                get_openalex_instits(),
                partial(openalex.make_inst_metadata, meta_vars=openalex.INST_META_VARS),
            ),
            "inputs/data/openalex/oalex_institutions_meta.csv",
        )
        self.years = list(range(2007, 2023))
        self.next(self.fetch_save_year, foreach="years")

    @batch(cpu=4, memory=32000)
    @step
    def fetch_save_year(self):
        print("Task started")
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
