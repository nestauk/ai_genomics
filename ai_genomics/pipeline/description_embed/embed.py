import click
import json
import logging
import numpy as np
import pandas as pd
import os
from sentence_transformers import SentenceTransformer
from toolz import partition_all
from torch import cuda


logger = logging.getLogger(__name__)


def embed(texts, model_name, chunk_size=10_000):
    model = SentenceTransformer(model_name)
    text_chunks = partition_all(chunk_size, texts)
    embedding_chunks = [model.encode(tc) for tc in text_chunks]
    return np.concatenate(embedding_chunks)


@click.option(
    "--directory",
    prompt="Directory with data files.",
)
@click.option(
    "--model-name",
    prompt="Name of sentence transformer to use.",
    default="allenai-specter",
)
@click.command()
def run(directory, model_name):

    files = os.listdir(directory)

    for file in files:
        path = f"{directory}/{file}"

        with open(path, "r") as f:
            data = json.load(f)

        logger.info(f"Embedding {file}")
        embeddings = embed(
            list(data.values()),
            model_name,
        )

        fout = file.split(".")[0]
        df = pd.DataFrame(
            index=data.keys(),
            data=embeddings,
        )
        df.to_csv(f"{directory}/{fout}_embeddings.csv")


if __name__ == "__main__":

    if not cuda.is_available():
        raise EnvironmentError("CUDA is not available")

    run()
