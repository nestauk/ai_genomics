import pandas as pd
from ai_genomics import bucket_name
from ai_genomics.getters.crunchbase import get_ai_genomics_crunchbase_org_ids
from ai_genomics.getters.data_getters import save_to_s3
from ai_genomics.getters.gtr import get_ai_genomics_gtr_data
from ai_genomics.getters.openalex import get_openalex_ai_genomics_works, work_abstracts
from ai_genomics.getters.patents import get_ai_genomics_patents
from ai_genomics.utils.crunchbase import fetch_crunchbase, parse_s3_table

YEARS = range(2007, 2022)
SAMPLE_SIZE = 100


def openalex_abstracts_dict_to_df(openalex_abstracts: dict) -> pd.DataFrame:
    """Converts OpenAlex abstracts into a DataFrame"""
    return (
        pd.DataFrame.from_dict(openalex_abstracts, orient="index")
        .reset_index()
        .rename(columns={"index": "work_id", 0: "abstract_text"})
    )


def make_ai_genomics_openalex_samples(sample_size=SAMPLE_SIZE) -> pd.DataFrame:
    """Returns a sample of AI Genomics OpenAlex work abstracts"""
    oa_ai_abstracts = work_abstracts(discipline="artificial_intelligence", years=YEARS)
    oa_genomics_abstracts = work_abstracts(discipline="genetics", years=YEARS)
    combined_abstracts = (
        pd.concat(
            [
                openalex_abstracts_dict_to_df(oa_ai_abstracts),
                openalex_abstracts_dict_to_df(oa_genomics_abstracts),
            ]
        )
        .query("abstract_text.notnull()")
        .drop_duplicates()
    )
    return (
        get_openalex_ai_genomics_works()
        .sample(sample_size)
        .merge(how="inner", right=combined_abstracts, on="work_id")[
            ["work_id", "abstract_text"]
        ]
        .reset_index(drop=True)
    )


def make_ai_genomics_gtr_samples(sample_size=SAMPLE_SIZE) -> pd.DataFrame:
    """Returns a sample of AI Genomics Gateway to Research project abstracts"""
    return (
        get_ai_genomics_gtr_data("projects")[["id", "abstract_text"]]
        .sample(sample_size)
        .reset_index(drop=True)
    )


def make_ai_genomics_patent_samples(sample_size=SAMPLE_SIZE) -> pd.DataFrame:
    """Returns a sample of AI Genomics patent abstracts"""
    return (
        get_ai_genomics_patents()[["publication_number", "abstract_text"]]
        .sample(sample_size)
        .reset_index(drop=True)
    )


def make_ai_genomics_cb_samples(sample_size=SAMPLE_SIZE) -> pd.DataFrame:
    """Returns a sample of AI Genomics Crunchbase company descriptions"""
    cb_orgs = parse_s3_table(fetch_crunchbase("orgs"))[
        ["id", "long_description", "short_description"]
    ]
    return (
        get_ai_genomics_crunchbase_org_ids()
        .merge(cb_orgs, how="inner", left_on="cb_org_id", right_on="id")
        .assign(description=lambda x: x.long_description.fillna(x.short_description))
        .query("description.notnull()")
        .sample(sample_size)
        .reset_index(drop=True)[["cb_org_id", "description"]]
    )


if __name__ == "__main__":
    samples = {
        "ai_genomics_openalex_samples": make_ai_genomics_openalex_samples(),
        "ai_genomics_gtr_samples": make_ai_genomics_gtr_samples(),
        "ai_genomics_patent_samples": make_ai_genomics_patent_samples(),
        "ai_genomics_cb_samples": make_ai_genomics_cb_samples(),
    }

    for sample_name, sample_df in samples.items():
        save_to_s3(
            bucket_name, sample_df, f"inputs/ai_genomics_samples/{sample_name}.csv"
        )
