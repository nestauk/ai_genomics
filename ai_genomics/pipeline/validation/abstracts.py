"""ai_genomics/pipeline/validation/abstracts.py
Generates csv files of abstracts with some overlapping rows for validation.
"""
from ai_genomics import PROJECT_DIR, get_yaml_config
from ai_genomics.utils.reading import make_path_if_not_exist
from ai_genomics.getters.patents import get_ai_genomics_patents
from ai_genomics.getters.openalex import (
    _get_openalex_ai_genomics_abstracts,
    _get_openalex_ai_genomics_works,
)
from ai_genomics.pipeline.validation.utils import (
    generate_overlapping_samples,
)
import pandas as pd


if __name__ == "__main__":

    config = get_yaml_config(PROJECT_DIR / "ai_genomics/config/validation.yml")

    out_dir = PROJECT_DIR / "inputs/validation"
    make_path_if_not_exist(out_dir)

    patent_keep_cols = ["publication_number", "title_text", "abstract_text"]

    patents = (
        get_ai_genomics_patents()
        .query("title_language == 'en' & abstract_language == 'en'")
        .loc[:, patent_keep_cols]
        .assign(Source="patent")
        .drop_duplicates(subset="abstract_text")
        .rename(
            columns={
                "publication_number": "ID",
                "title_text": "Title",
                "abstract_text": "Abstract",
            },
        )
    )

    col_order = ["Source", "ID", "Title", "Abstract"]
    overlapping_patent_samples = generate_overlapping_samples(
        patents[col_order],
        n_splits=config["n_contributors"],
        overlap=config["abstracts"]["overlap"],
        max_sample_size=config["abstracts"]["max_sample_size"],
    )

    for i, sample in enumerate(overlapping_patent_samples):
        sample.to_csv(
            out_dir / f"patent_abstracts_{i}.csv",
            index=False,
        )

    oa_abstracts = _get_openalex_ai_genomics_abstracts()
    oa_abstracts = pd.DataFrame(
        {
            "ID": oa_abstracts.keys(),
            "Abstract": oa_abstracts.values(),
            "Source": "publication",
        },
    )

    oa_keep_cols = ["display_name", "work_id"]
    works = (
        _get_openalex_ai_genomics_works()
        .query("predicted_language == 'en'")
        .loc[:, oa_keep_cols]
        .rename(columns={"display_name": "Title", "work_id": "ID"})
        .merge(oa_abstracts, on="ID")
    )

    overlapping_works_samples = generate_overlapping_samples(
        works[col_order],
        n_splits=config["n_contributors"],
        overlap=config["abstracts"]["overlap"],
        max_sample_size=config["abstracts"]["max_sample_size"],
    )

    for i, sample in enumerate(overlapping_works_samples):
        sample.to_csv(
            out_dir / f"oa_abstracts_{i}.csv",
            index=False,
        )
