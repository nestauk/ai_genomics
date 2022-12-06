# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     comment_magics: true
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.14.0
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
# %load_ext autoreload
# %autoreload 2

import json
import os
from ai_genomics import PROJECT_DIR
from dotenv import find_dotenv, load_dotenv
import requests
import ratelim
import time
import numpy as np
import pandas as pd
from itertools import chain
from datetime import datetime
import uuid
from typing import Dict, List
from toolz import pipe
from functools import partial

# %%
load_dotenv(find_dotenv())

# %%
# Tasks

# 1. Download all citations with context to alphafold
# 2. Extract DOIs
# 3. Fetch papers from openalex
# 4. Analyse

# %%
alphafold_doi = "10.1038/s41586-021-03819-2"

# %%
sem_key = os.getenv("semantic_key")
HEADERS = {"x-api-key": sem_key}

API_TEMPLATE = (
    "https://partner.semanticscholar.org/graph/v1/paper/10.1038/s41586-021-03819-2"
)
PAPER_SUFFIX = "/?fields=title,citationCount"
CITATION_SUFFIX = "/citations?fields=paperId,title,externalIds,fieldsOfStudy,contexts,intents,isInfluential,publicationDate,year,venue"

# %%
sem_key

# %%
paper_cits = requests.get(f"{API_TEMPLATE}{PAPER_SUFFIX}", headers=HEADERS)
n_cits = paper_cits.json()["citationCount"]

# %%
collected = []
offset = 0

while offset < n_cits:
    time.sleep(2)
    print(offset)

    cits = requests.get(
        f"{API_TEMPLATE}{CITATION_SUFFIX}&offset={offset}&limit=500", headers=HEADERS
    )
    collected.append(cits.json()["data"])
    offset += 500

# %%
collected_flat = list(chain(*collected))


# %%
def add_if_present(_dict, key):

    return _dict[key] if key in _dict.keys() else np.nan


# %%
def paper_metadata(sem_result):
    """Parse metadata from the json file"""

    res = {}
    try:
        res["internal_id"] = str(uuid.uuid4())
        res["title"] = add_if_present(sem_result["citingPaper"], "title")
        res["influential"] = add_if_present(sem_result, "isInfluential")
        res["doi"] = (
            add_if_present(sem_result["citingPaper"]["externalIds"], "DOI")
            if type(sem_result["citingPaper"]["externalIds"]) == dict
            else np.nan
        )
        res["pubmed"] = (
            add_if_present(sem_result["citingPaper"]["externalIds"], "PubMed")
            if type(sem_result["citingPaper"]["externalIds"]) == dict
            else np.nan
        )
        res["paper_id"] = (
            add_if_present(sem_result["citingPaper"]["externalIds"], "CorpusId")
            if type(sem_result["citingPaper"]["externalIds"]) == dict
            else np.nan
        )
        res["venue"] = add_if_present(sem_result["citingPaper"], "venue")
        res["publication_date"] = add_if_present(
            sem_result["citingPaper"], "publicationDate"
        )
        res["year"] = add_if_present(sem_result["citingPaper"], "year")

        return res

    except:
        print(sem_result)


# %%
paper_df = pd.DataFrame([paper_metadata(res) for res in collected_flat]).assign(
    date=lambda df: [
        datetime.strptime(d, "%Y-%m-%d") if type(d) == str else np.nan
        for d in df["publication_date"]
    ]
)

# %%
paper_df["date"].value_counts().plot()

# %%
cit_contexts = {
    internal_id: paper["contexts"]
    for internal_id, paper in zip(paper_df["internal_id"].values, collected_flat)
    if len(paper["contexts"]) > 0
}

# %%
len(cit_contexts)

# %%
citing_dois = list(set(paper_df.dropna(axis=0, subset=["doi"])["doi"]))

# %%
len(citing_dois)


# %%
@ratelim.patient(100, 60)
def fetch_openalex(doi):

    req = requests.get(
        f"https://api.openalex.org/works/https://doi.org/{doi}?mailto=juan.mateos-garcia@nesta.org.uk"
    )

    if req:
        return req.json()
    else:
        print(req.status_code)


# %%
alphafold_oalex = [fetch_openalex(doi) for doi in citing_dois]

# %%
alphafold_oalex_clean = [x for x in alphafold_oalex if type(x) == dict]

# %%
len(alphafold_oalex_clean)

# %%
# Analysis

# %%
100 * paper_df["influential"].mean()

# %%
# Context of citations

# %%
# Flatten citation contexts
# cit_corpus = [(_id,c) for context in cit_contexts for _id,cont in cit_contexts.items()]

cit_corpus = list(
    chain(*[[(_id, c) for c in cont] for _id, cont in cit_contexts.items()])
)

# %%
# Process text
from ai_genomics.utils import text_modeling as textmod

# %%
import random
import spacy
from spacy.tokenizer import Tokenizer
from spacy.lang.en import English
from spacy.tokens import Doc
import ai_genomics.utils.text_modeling as text_modeling
import re
from string import digits

nlp = spacy.load("en_core_web_sm")
tokenizer = Tokenizer(nlp.vocab)


# %%
def text_processing(corpus: List, bigram_n: int = 3) -> tuple:
    """Process and bigram text"""
    return pipe(
        corpus,
        list,
        lambda corpus: [text_modeling.remove_symbols(d) for d in corpus],
        lambda corpus: tokenizer.pipe(corpus),
        lambda corpus: [text_modeling.remove_stop_punct(d) for d in corpus],
        partial(text_modeling.build_ngrams, n=bigram_n),
    )


# %%
# Remove numbers from text


def pre_process(cit_context):

    return pipe(
        cit_context,
        lambda string: re.sub(r"[0-9]", "", string),
        lambda string: " ".join([x for x in string.split(" ") if "_" not in x]),
    )


cit_corpus_proc = [(c[0], pre_process(c[1])) for c in cit_corpus]

# %%
text_processed = text_processing([c[1] for c in cit_corpus_proc])

# %%
mdl = text_modeling.train_lda(text_processed[0], k=100, top_remove=20, verbose=False)

# %%
topic_df = text_modeling.create_doc_topics(
    mdl,
    text_modeling.create_topic_names(mdl, k=100, n_words=5),
    [c[0] for c in cit_corpus_proc],
)

# %%
topic_df.columns = [f"{str(n)}_{col}" for n, col in enumerate(topic_df.columns)]

# %%
