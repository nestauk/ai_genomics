# Fetch specter embeddings for a sample of works
# %%
import logging
import json
import os
import requests
from requests.auth import HTTPBasicAuth
from typing import Dict, Union, List

import ratelim
from dotenv import find_dotenv, load_dotenv

from ai_genomics.getters.openalex import work_metadata
from ai_genomics import PROJECT_DIR

# Set up directory
SEM_PATH = f"{PROJECT_DIR}/inputs/data/semantic_scholar"
os.makedirs(SEM_PATH, exist_ok=True)

# Set up authentication
load_dotenv(find_dotenv())
api_key = os.getenv("semantic_key")
HEADERS = {"x-api-key": api_key}

API_TEMPLATE = "https://partner.semanticscholar.org/graph/v1/paper/{}?fields=embedding"


@ratelim.patient(100, 10)
def fetch_embedding(doi: str) -> Union[Dict, None]:
    """
    Fetch paper embedding from the semantic scholar API
    """

    response = requests.get(API_TEMPLATE.format(doi), headers=HEADERS)

    if response:
        return response.json()["embedding"]["vector"]
    else:
        logging.info(response)
        return None


if __name__ == "__main__":

    logging.info("Reading data")
    meta = work_metadata("artificial_intelligence", [2012, 2017, 2021]).dropna(
        axis=0, subset=["doi"]
    )

    meta_sample = meta.sample(100000).reset_index(drop=True)

    meta_dois = ["/".join(url.split("/")[3:]) for url in meta_sample["doi"].values]

    embeddings_collection: List = []

    logging.info("Start fetching")
    for n, doi in enumerate(meta_dois):
        if n % 5000 == 0:
            logging.info(f"fetched {n}")
            with open(f"{SEM_PATH}/sample_embeddings.json", "w") as outfile:
                json.dump(embeddings_collection, outfile)

        embeddings_collection.append((doi, fetch_embedding(doi)))

    with open(f"{SEM_PATH}/sample_embeddings.json", "w") as outfile:
        json.dump(embeddings_collection, outfile)
