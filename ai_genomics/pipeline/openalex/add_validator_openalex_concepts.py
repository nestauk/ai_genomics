from metaflow import FlowSpec, S3, step, Parameter, retry, batch

import json
import pandas as pd

import json
import pandas as pd
import numpy as np

from ai_genomics import PROJECT_DIR, bucket_name
from ai_genomics.utils.reading import read_json
from ai_genomics.getters.data_getters import load_s3_data, save_to_s3


class ValidatorOpenAlexConceptsFlow(FlowSpec):
    production = Parameter("production", help="Run in production?", default=False)

    @step
    def start(self):
        """
        Starts the flow.
        """
        concepts = pd.read_parquet(
            f"{PROJECT_DIR}/outputs/openalex/openalex_concepts.parquet"
        )
        print("Concepts Loaded!")
        all_concepts = load_s3_data(bucket_name, "inputs/openalex/all_oa_concepts.csv")
        threshold = 0.0
        concepts = concepts[concepts["score"] > threshold]
        concepts_with_scope = concepts.merge(
            all_concepts, how="left", left_on="display_name", right_on="display_name"
        )
        concepts_with_scope["Scope"] = (
            concepts_with_scope["Scope"]
            .replace("In scope", 3)
            .replace("Tangential", 2)
            .replace("Not in scope", 1)
            .fillna(0)
        )
        grouped_concepts_with_scope = (
            concepts_with_scope.groupby("doc_id").agg({"Scope": max}).reset_index()
        )
        grouped_concepts_with_scope["genomics_in_scope"] = np.where(
            grouped_concepts_with_scope["Scope"] == 3, True, False
        )
        grouped_concepts_with_scope["genomics_tangential"] = np.where(
            grouped_concepts_with_scope["Scope"] == 2, True, False
        )
        grouped_concepts_with_scope["genomics_not_in_scope"] = np.where(
            grouped_concepts_with_scope["Scope"] == 1, True, False
        )
        all_works = load_s3_data(bucket_name, "outputs/openalex/openalex_works.csv")
        all_works = all_works.merge(
            grouped_concepts_with_scope,
            how="left",
            left_on="work_id",
            right_on="doc_id",
        )
        save_to_s3(
            bucket_name, all_works, "outputs/openalex/openalex_works_validated.csv"
        )
        self.next(self.end)

    @step
    def end(self):
        """
        Ends the flow.
        """
        pass


if __name__ == "__main__":
    ValidatorOpenAlexConceptsFlow()
