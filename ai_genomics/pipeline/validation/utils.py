"""ai_genomics/pipeline/validation/utils.py
Utilities for creating samples to be manually validated.
"""
import numpy as np
import os
import pandas as pd
from toolz.functoolz import pipe
from typing import Dict, Generator, Union

from ai_genomics import PROJECT_DIR
from ai_genomics import logger


def patent_code_to_description_lookup(classification: str = "ipc") -> Dict:
    """Generates a lookup between IPC or CPC codes and their descriptions.

    Args:
        classification: Classification scheme to create lookup for. One of
            'cpc' or 'ipc'. Defaults to 'ipc'.

    Returns:
        Dictionary where keys are patent classification codes and values are
            their descriptions.
    """

    allowed_classifications = ["ipc", "cpc"]
    if classification not in allowed_classifications:
        err_msg = f"Allowed classifications are {allowed_classifications}."
        error = ValueError(err_msg)
        logger.error(err_msg, exc_info=error)
        raise error

    lookup_dir = PROJECT_DIR / f"inputs/patent_data/{classification}"
    files = os.listdir(lookup_dir)

    lookups = []
    for f in files:
        if f.endswith(".txt"):
            lookups.append(
                pd.read_csv(
                    lookup_dir / f,
                    sep="\t",
                    header=None,
                )
            )

    # cpc codes have an additional column for taxonomy level
    description_col = 1 if classification == "ipc" else 2

    return pipe(
        lookups,
        pd.concat,
        lambda l: dict(zip(l[0], l[description_col])),
    )


def generate_overlapping_samples(
    data: pd.DataFrame,
    n_splits: int,
    overlap: int,
    max_sample_size: Union[int, None] = None,
    shuffle: bool = True,
) -> Generator[pd.DataFrame, None, None]:
    """Slices a dataframe into evenly sized chunks. Each chunk will contain
    different rows, except for a number of rows as defined by `overlap`, which
    will be shared and all the same.

    This is useful for creating samples for manual validation where you want
    each contributor to validate different samples, but also want to have some
    overlap to be able to measure the level of inter-contributor agreement.

    Args:
        n_splits: The number of chunks to slice the dataframe into.
        overlap: The number of shared samples the chunks will have.
        max_sample_size: The maximum number of samples allowed in each chunk.
            Chunks will be truncated to this length.
        shuffle: Whether to shuffle the data before slicing. Defaults to True.

    Yields:
        Slices of a dataframe that share a common set of rows.
    """
    if shuffle:
        data = data.sample(frac=1.0, random_state=42)

    data_overlap = data.sample(overlap, random_state=42)
    data_chunks = np.array_split(data.drop(data_overlap.index), n_splits)

    for data_chunk in data_chunks:
        sample = pd.concat([data_overlap, data_chunk])
        yield sample.iloc[:max_sample_size]
