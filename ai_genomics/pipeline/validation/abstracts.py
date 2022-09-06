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

    patent_drop_cols = [
        "cpc_codes",
        "ipc_codes",
        "publication_date",
        "filing_date",
        "grant_date",
        "priority_date",
        "entity_status",
        "title_language",
        "abstract_language",
        "application_number",
    ]

    patents = (
        get_ai_genomics_patents()
        .query("title_language == 'en' & abstract_language == 'en'")
        .drop(patent_drop_cols, axis=1)
        .rename(
            columns={
                "publication_number": "ID",
                "title_text": "Title",
                "abstract_text": "Abstract",
            },
        )
        .assign(Source="patent")
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

    openalex_drop_cols = [
        "Unnamed: 0.1",
        "index",
        "Unnamed: 0",
        "doi",
        "publication_year",
        "publication_date",
        "cited_by_count",
        "is_retracted",
        "venue_id",
        "venue_display_name",
        "venue_url",
        "predicted_language",
        "language_probability",
        "has_abstract",
        "arxiv_id",
        "ambiguous",
    ]

    oa_abstracts = _get_openalex_ai_genomics_abstracts()
    oa_abstracts = pd.DataFrame(
        {
            "ID": oa_abstracts.keys(),
            "Abstract": oa_abstracts.values(),
            "Source": "publication",
        },
    )
    works = (
        _get_openalex_ai_genomics_works()
        .query("predicted_language == 'en'")
        .drop(columns=openalex_drop_cols, axis=1)
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
