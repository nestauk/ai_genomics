"""Script to add OA tag thresholds in labelled tag extraction validation dataset."""
import ast
import pandas as pd

from ai_genomics.getters.data_getters import load_s3_data, save_to_s3
from ai_genomics import bucket_name
from ai_genomics.analysis.tag_evaluation.make_tag_validation_dataset import (
    load_sample_tags,
)

LABELLED_TAG_EVALUATION_DATASET = load_s3_data(
    bucket_name,
    "inputs/ai_genomics_samples/labelled_ai_genomics_sample_validation_set.csv",
)


def get_oa_tags_above_threshold(
    oa_samples: pd.DataFrame, threshold: int
) -> pd.DataFrame:
    """Generates column of OpenAlex tags above a threshold."""
    tag_indx_above_threshold = oa_samples["scores"].apply(
        lambda x: [i for i, score in enumerate(x) if score > threshold]
    )
    oa_samples[f"oa_tags_above_{threshold}"] = [
        [tags[i] for i in tag_indx]
        for tag_indx, tags in zip(tag_indx_above_threshold, oa_samples["tags"])
    ]

    return oa_samples


if __name__ == "__main__":

    oa_samples = load_sample_tags()
    labelled_dataset_oa_scores = pd.merge(
        LABELLED_TAG_EVALUATION_DATASET, oa_samples, on="description"
    )

    for col in ("scores", "tags"):
        labelled_dataset_oa_scores[col] = labelled_dataset_oa_scores[col].apply(
            ast.literal_eval
        )

    labelled_dataset_oa_scores.scores = labelled_dataset_oa_scores.scores.apply(
        lambda x: [float(i) for i in x]
    )

    for thresh in (0.5, 0.6, 0.7):
        labelled_dataset_oa_scores = get_oa_tags_above_threshold(
            labelled_dataset_oa_scores, thresh
        )

    labelled_dataset_oa_scores = labelled_dataset_oa_scores[
        [
            "description",
            "data_source_x",
            "oa_tags",
            "db_tags",
            "oa_tags_above_0.5",
            "oa_tags_above_0.6",
            "oa_tags_above_0.7",
            "oa_false_positives",
            "oa_false_negatives",
            "db_false_positives",
            "db_false_negatives",
        ]
    ].rename(columns={"data_source_x": "data_source"})

    save_to_s3(
        bucket_name,
        labelled_dataset_oa_scores,
        "inputs/ai_genomics_samples/labelled_ai_genomics_sample_validation_set_with_oa_thresholds.csv",
    )
