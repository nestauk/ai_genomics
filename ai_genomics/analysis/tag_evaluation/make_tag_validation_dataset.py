"""Script to 1) extract openalex concepts from openalex sample and
2) generate validation dataset to compare tags across
sample of sample of texts with DBpedia and OpenAlex tags."""

from ai_genomics.getters.data_getters import load_s3_data, get_s3_dir_files, save_to_s3

from ai_genomics import bucket_name, logger
from typing import List
import pandas as pd
import requests
import ast

SAMPLES_DIR = get_s3_dir_files(bucket_name, "inputs/ai_genomics_samples/")
OA_SAMPLE = load_s3_data(
    bucket_name, "inputs/ai_genomics_samples/ai_genomics_openalex_samples.csv"
)


def make_openalex_api_url(work_url: str) -> str:
    """Convert OpenAlex sample work id into json-compatible url."""
    return f"https://api.openalex.org/works/{work_url.split('/')[-1]}"


def get_openalex_sample_concept(api_url: str) -> List[str]:
    """Calls OpenAlex API to get concepts for a given OpenAlex API URL."""
    oa_response = requests.get(api_url)
    if not oa_response.ok:
        logger.error(f"response is not 200 - {oa_response}")
    else:
        oa_data = oa_response.json()
        if "concepts" in oa_data.keys():
            tags = [oa["display_name"] for oa in oa_data["concepts"]]
            scores = [oa["score"] for oa in oa_data["concepts"]]
            levels = [oa["level"] for oa in oa_data["concepts"]]
            return [api_url, tags, scores, levels]
        else:
            logger.info(f"no concepts found for {api_url}")


def get_openalex_sample_concepts(oa_sample: pd.DataFrame = OA_SAMPLE) -> pd.DataFrame:
    """Calls OpenAlex API to get concepts per OpenAlex sample work id.
    Returns OpenAlex sample with associated concepts, concept
    scores and concept levels.

    Args:
        oa_sample: DataFrame of OpenAlex text samples

    Returns:
        OpenAlex sample with associated concepts, concept
            scores and concept levels.
    """
    oa_sample["api_url"] = oa_sample["work_id"].apply(make_openalex_api_url)
    oa_tags = [get_openalex_sample_concept(sample) for sample in oa_sample["api_url"]]

    oa_with_concepts = pd.DataFrame(
        oa_tags, columns=["api_url", "tags", "scores", "levels"]
    )
    oa_sample_with_concepts = pd.merge(
        oa_with_concepts, oa_sample, on="api_url", how="right"
    )

    return oa_sample_with_concepts[
        ["work_id", "abstract_text", "tags", "scores", "levels"]
    ]


def load_sample_tags(
    tags_dir: str = "oa_tags", sample_dirs: List[str] = SAMPLES_DIR
) -> pd.DataFrame:
    """Load and concatenate dataset samples with tags extracted.
        returns a standardised dataframe of descriptions,
        tags extracted and the data source

    Args:
        tags_dir: the tag method type as described in file names
        sample_dirs: A list of file names in S3 dataset samples directory
    """
    dfs = []
    for sample_dir in sample_dirs:
        if (tags_dir in sample_dir) and sample_dir.endswith(".csv"):
            sample_with_tags = load_s3_data(bucket_name, sample_dir)
            sample_with_tags = sample_with_tags.rename(
                columns={
                    "abstract_text": "description",
                    "source": "description",
                    "entities": "tags",
                }
            )
            sample_with_tags["data_source"] = sample_dir.split("/")[-1].split("_")[2]
            if "scores" in sample_with_tags.columns:
                sample_with_tags = sample_with_tags[
                    ["description", "tags", "scores", "data_source"]
                ]
            else:
                sample_with_tags = sample_with_tags[
                    ["description", "tags", "data_source"]
                ]
            dfs.append(sample_with_tags)

    return pd.concat(dfs).reset_index(drop=True)


if __name__ == "__main__":

    # first, add concepts to sample of openalex data
    oa_sample_concepts = get_openalex_sample_concepts()
    logger.info("extracted open alex concepts for openalex sample")

    save_to_s3(
        bucket_name,
        oa_sample_concepts,
        "inputs/ai_genomics_samples/samples_with_oa_tags/ai_genomics_openalex_samples_with_oa_tags.csv",
    )

    # then generate validation dataset to label
    oa_samples = load_sample_tags()
    db_samples = load_sample_tags("db_tags")

    oa_db_sample = pd.merge(oa_samples, db_samples, on="description").rename(
        columns={
            "tags_x": "oa_tags",
            "tags_y": "db_tags",
            "data_source_x": "data_source",
        }
    )
    oa_db_sample_sample = (
        oa_db_sample[["description", "data_source", "oa_tags", "db_tags"]]
        .groupby("data_source")
        .sample(20, random_state=42)
        .reset_index(drop=True)
    )

    logger.info(
        "generated sample of sample of texts with extracted OpenAlex and DBpedia tags."
    )
    save_to_s3(
        bucket_name,
        oa_db_sample_sample,
        "inputs/ai_genomics_samples/ai_genomics_sample_validation_set.csv",
    )
