# Scripts to implement influence analysis
import logging
import numpy as np
import pandas as pd
from functools import partial
from toolz import pipe
from typing import Dict, List

import spacy
from spacy.tokenizer import Tokenizer

import ai_genomics.utils.text_modeling as tm
from ai_genomics.getters.data_getters import save_to_s3

# NLP stuff
nlp = spacy.load("en_core_web_sm")
tokenizer = Tokenizer(nlp.vocab)


def binarise_top(array: np.array, q: float = 0.9):
    """Binarises a distribution: 1 if above q quantile, 0 otherwise"""
    thres = np.quantile(array, q)
    return [w > thres for w in array]


def text_processing(
    text_dict: Dict[str, str], n_grams: int = 3
) -> Dict[str, List[str]]:
    """Text processing pipeline

    Args:
        text_dict: Dictionary of text to process.
        n_grams: size of n-grams to extract.
    Returns:
        Dictionary of processed text.
    """

    return pipe(
        text_dict.values(),
        list,
        lambda corpus: [tm.remove_symbols(d) for d in corpus],
        lambda corpus: [tm.remove_digits(d) for d in corpus],
        lambda corpus: tokenizer.pipe(corpus),
        lambda corpus: [tm.remove_stop_punct(d) for d in corpus],
        lambda corpus: [tm.remove_short_tokens(d) for d in corpus],
        partial(tm.build_ngrams, n=n_grams),
        lambda tok_output: {
            _id: tok
            for _id, tok in zip(text_dict.keys(), tok_output[0])
            if len(tok) > 0  # Remove empty documents
        },
    )


def make_topic_distribution(
    text_dict: Dict[str, str], num_topics: int = 100, top_remove: int = 50
) -> pd.DataFrame:
    """Fits topic model and creates topic distribution

    Args:
        text_dict: Dictionary of text to process.
        num_topics: Number of topics to fit.
        top_remove: Number of top words to remove from topic model.

    Returns:
        Topic distribution for the corpus
    """
    lda_model = tm.train_lda(list(text_dict.values()), k=num_topics, top_remove=50)

    # Create topic dataframe
    return tm.create_doc_topics(
        lda_model,
        tm.create_topic_names(lda_model, num_topics),
        list(text_dict.keys()),
    )


def calculate_topic_shares(
    topic_df: pd.DataFrame,
    disc_names_ids: Dict[str, List],
    baseline_discs: List[str] = ["ai", "genomics"],
    binarise_thres: float = 0.9,
) -> pd.DataFrame:
    """Calculates the topic shares for each category

    Args:
        topic_df: Topic distribution for the corpus.
        disc_names_ids: Dictionary of disc names and ids.
        baseline_discs: List of baseline disciplines.
        binarise_thres: quantile threshold for binarising topic distribution.

    Returns:
        Df of discipline shares in each topic
    """
    # Calculate the topic shares for each category

    return (
        topic_df.apply(lambda top: binarise_top(top, q=binarise_thres))
        .assign(topic_category=lambda df: df.index.map(disc_names_ids))
        .melt(id_vars="topic_category")
        .groupby(["topic_category", "variable"])["value"]
        .sum()
        .unstack(level=0)[baseline_discs]
        .apply(lambda x: x / x.sum(), axis=1)
        .stack()
        .reset_index(name="disc_weight")
    )


def calculate_influence_score(
    topic_df: pd.DataFrame, disc_shares: pd.DataFrame
) -> pd.DataFrame:
    """Calculates the influence score for each document

    Args:
        topic_df: Topic distribution for the corpus.
        disc_names_ids: Dictionary of disc names and ids.
        disc_shares: Topic shares for each category.

    Returns:
        DF of influence score for each document
    """
    return (
        topic_df.stack()
        .reset_index(name="topic_weight")
        .rename(columns={"level_1": "topic", "level_0": "doc_id"})
        .merge(disc_shares, left_on="topic", right_on="variable")
        .drop(axis=1, labels=["variable"])
        .assign(topic_disc_weighted=lambda df: df["topic_weight"] * df["disc_weight"])
        .groupby(["doc_id", "topic_category"])["topic_disc_weighted"]
        .sum()
    )


if __name__ == "__main__":
    logging.info("testing with the GtR data")

    import ai_genomics.getters.gtr as gtr

    gtr_meta = gtr.get_ai_genomics_project_table(local=False)

    # We need to sample baselines of similar sizes. Otherwise the shares
    # (and perhaps the topics?) will be unbalanced
    gtr_meta_sampled = pd.concat(
        [
            gtr_meta.query("ai_genomics==True"),
            gtr_meta.query("ai_genomics==False").query("ai==True").sample(2000),
            gtr_meta.query("ai_genomics==False").query("genomics==True").sample(2000),
        ]
    )

    gtr_text = {
        _id: text
        for _id, text in zip(gtr_meta_sampled["id"], gtr_meta_sampled["abstract_text"])
    }

    gtr_proc = text_processing(gtr_text)
    gtr_topic_df = make_topic_distribution(gtr_proc, num_topics=200)

    # This seeks to ensure that the set of categories is disjoint i.e.
    # if a paper is in ai_genomics then it cannot be in AI or genomics
    disc_ids = {}

    for var in ["ai_genomics", "ai", "genomics"]:

        if var == "ai_genomics":

            disc_ids[var] = set(
                gtr_meta_sampled.loc[gtr_meta_sampled[var] == True]["id"]
            )

        else:

            candidates = gtr_meta_sampled.loc[gtr_meta_sampled[var] == True]["id"]

            disc_ids[var] = [x for x in candidates if x not in disc_ids["ai_genomics"]]

        disc_id_lookup = {
            _id: cat for cat, id_list in disc_ids.items() for _id in id_list
        }

    # This calculates the share of a topic accounted by a discipline
    topic_shares = calculate_topic_shares(
        gtr_topic_df, disc_id_lookup, baseline_discs=["ai", "genomics"]
    )

    logging.info("top_topics in AI")
    logging.info(
        topic_shares.query("topic_category=='ai'")
        .sort_values("disc_weight", ascending=False)
        .head(n=10)
    )

    logging.info("top_topics in genomics")
    logging.info(
        topic_shares.query("topic_category=='genomics'")
        .sort_values("disc_weight", ascending=False)
        .head(n=10)
    )

    # This calculates the influence of a discipline in a paper based
    # on its topic distribution
    disc_score = calculate_influence_score(gtr_topic_df, topic_shares)

    gtr_meta_sampled = gtr_meta_sampled.merge(
        disc_score.reset_index(drop=False), left_on="id", right_on="doc_id"
    )

    logging.info("abstracts for projects influenced by AI")
    for t in gtr_meta_sampled.query("topic_category=='ai'").sort_values(
        "topic_disc_weighted", ascending=False
    )["abstract_text"][:5]:
        print("\t" + t[:500] + "...")
        print("\n")

    logging.info("abstracts for projects influenced by Genomics")
    for t in gtr_meta_sampled.query("topic_category=='genomics'").sort_values(
        "topic_disc_weighted", ascending=False
    )["abstract_text"][:5]:
        print("\t" + t[:500] + "...")
        print("\n")
