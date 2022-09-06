"""ai_genomics/pipeline/validation/definitions.py
A script to generate samples used in the expert validation exercise.
"""
from ai_genomics import bucket_name, get_yaml_config, PROJECT_DIR
from ai_genomics.getters.data_getters import load_s3_data
from ai_genomics.getters.patents import get_cpc_lookup, get_ai_genomics_patents
from ai_genomics.pipeline.validation.utils import generate_overlapping_samples
from ai_genomics.pipeline.patent_data.cpc_utils import find_context
from ai_genomics.utils.reading import make_path_if_not_exist
import ast
import json
import pandas as pd


if __name__ == "__main__":

    config = get_yaml_config(PROJECT_DIR / "ai_genomics/config/validation.yml")
    out_dir = PROJECT_DIR / "inputs/validation"
    make_path_if_not_exist(out_dir)

    patents = get_ai_genomics_patents()
    code_freqs = (
        patents["cpc_codes"]
        .apply(lambda c: ast.literal_eval(c))
        .explode()
        .value_counts()
    )
    drop_codes = code_freqs[
        code_freqs < config["definitions"]["min_frequency"]
    ].index.values

    cpc_lookup = load_s3_data(
        bucket_name,
        "outputs/patent_data/class_codes/cpc_with_definitions.json",
    )

    cpc_code_df = pd.DataFrame(
        {
            "code": cpc_lookup["genomics"].keys(),
            "description": cpc_lookup["genomics"].values(),
            "classification_system": "cpc",
        }
    )
    cpc_code_df = cpc_code_df[~cpc_code_df["code"].isin(drop_codes)]

    code_lookup = get_cpc_lookup()
    contexts = [find_context(c, code_lookup) for c in cpc_code_df["code"]]
    cpc_code_df["context"] = contexts
    cpc_code_df = cpc_code_df[
        ["classification_system", "code", "context", "description"]
    ]

    overlap = (
        cpc_code_df.shape[0]
        if cpc_code_df.shape[0] < config["definitions"]["overlap"]
        else config["definitions"]["overlap"]
    )
    overlapping_samples = generate_overlapping_samples(
        cpc_code_df,
        n_splits=config["n_contributors"],
        overlap=overlap,
    )

    for i, sample in enumerate(overlapping_samples):
        sample.to_csv(
            out_dir / f"patent_definitions_{i}.csv",
            index=False,
        )

    oa_concepts_dir = PROJECT_DIR / "inputs/openalex/genetics_concepts_definitions.json"
    with open(oa_concepts_dir, "r") as f:
        oa_concepts = json.load(f)

    oa_concepts_df = pd.DataFrame(
        {
            "classification_system": "oa",
            "code": [c[0] for c in oa_concepts],
            "description": [c[1] for c in oa_concepts],
        }
    )

    oa_concepts_df.to_csv(out_dir / "oa_definitions.csv")
