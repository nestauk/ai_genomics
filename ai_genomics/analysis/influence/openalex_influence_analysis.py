# Analysis of influence in OpenAlex data

import logging
import pandas as pd
import numpy as np
import spacy
from toolz import pipe
from functools import partial
from spacy.tokenizer import Tokenizer

import ai_genomics.getters.openalex as oal
import ai_genomics.utils.text_modeling as tm
from ai_genomics.getters.data_getters import save_to_s3

# NLP stuff
nlp = spacy.load("en_core_web_sm")
tokenizer = Tokenizer(nlp.vocab)


def binarise_top(array: np.array, q: float = 0.9):
    """Binarises a distribution: 1 if above q quantile, 0 otherwise"""
    thres = np.quantile(array, q)
    return [w > thres for w in array]


NUM_TOPICS = 100

if __name__ == "__main__":

    logging.info("Reading data")
    works = oal.get_openalex_ai_genomics_works()
    abstrs = oal.get_openalex_ai_genomics_abstracts(local=False)

    logging.info("Sampling")
    # Sample all AI genomics and a subset of AI and genomics work
    works_sampled = (
        pd.concat(
            [
                works.query("ai_genomics==True"),
                works.query("ai_genomics==False").query("ai==True").sample(10000),
                works.query("ai_genomics==False").query("genomics==True").sample(10000),
            ]
        )
        .drop_duplicates("work_id")
        .reset_index(drop=True)
    )

    sampled_ids = set(works_sampled["work_id"])

    # Extract the sampled IDs
    abstr_sampled = {k: v for k, v in abstrs.items() if k in sampled_ids}

    # Get the ids that belong to each category
    ai_gen_id, ai_id, gen_id = [
        set(works_sampled.loc[works_sampled[var] == True]["work_id"])
        for var in ["ai_genomics", "ai", "genomics"]
    ]

    logging.info("Text processing and topic modelling")

    # This is the prototype text pipeline. It could be refactored
    # into spacy pipelines or something
    abstr_tok = pipe(
        abstr_sampled.values(),
        list,
        lambda corpus: [tm.remove_symbols(d) for d in corpus],
        lambda corpus: [tm.remove_digits(d) for d in corpus],
        lambda corpus: tokenizer.pipe(corpus),
        lambda corpus: [tm.remove_stop_punct(d) for d in corpus],
        lambda corpus: [tm.remove_short_tokens(d) for d in corpus],
        partial(tm.build_ngrams, n=3),
        lambda tok_output: {
            _id: tok
            for _id, tok in zip(abstr_sampled.keys(), tok_output[0])
            if len(tok) > 0  # Remove empty documents
        },
    )

    # Train topic model
    ai_genom_lda = tm.train_lda(list(abstr_tok.values()), k=NUM_TOPICS, top_remove=50)

    logging.info("Analyse topic distribution")
    # Create topic dataframe

    topic_df = tm.create_doc_topics(
        ai_genom_lda,
        tm.create_topic_names(ai_genom_lda, NUM_TOPICS),
        list(abstr_tok.keys()),
    )

    # Binarise topic distributions,
    topic_df_bin = topic_df.apply(lambda top: binarise_top(top)).assign(
        topic_category=lambda df: [
            "ai_genom" if ind in ai_gen_id else "ai" if ind in ai_id else "genom"
            for ind in df.index
        ]
    )
    # Count topics in each category topic
    topic_disc_count = (
        topic_df_bin.melt(id_vars="topic_category")
        .groupby(["topic_category", "variable"])["value"]
        .sum()
        .unstack(level=0)
    )

    # Calculate the proportion of papers with high value for
    # topics in each category
    topic_disc_share = (
        topic_disc_count[["ai", "genom"]]
        .apply(lambda x: x / x.sum(), axis=1)
        .stack()
        .reset_index(name="disc_weight")
    )

    # Combine topic weights with discipline weights
    # Topic disc weighted is the disc-weighted weight of a topic in a document
    topic_combined = (
        topic_df.stack()
        .reset_index(name="topic_weight")
        .rename(columns={"level_1": "topic", "level_0": "work_id"})
        .merge(topic_disc_share, left_on="topic", right_on="variable")
        .drop(axis=1, labels=["variable"])
        .assign(topic_disc_weighted=lambda df: df["topic_weight"] * df["disc_weight"])
    )

    # Here we calculate the topic-disc-weighted influence of a topic
    paper_disc_score = (
        topic_combined.groupby(["work_id", "topic_category"])["topic_disc_weighted"]
        .sum()
        .reset_index(name="weighted_disc_score")
        .assign(
            cat_assigned=lambda df: [
                "ai_genom" if ind in ai_gen_id else "ai" if ind in ai_id else "genom"
                for ind in df["work_id"]
            ]
        )
    )
    logging.info("Validation of results")
    logging.info("Do the topic-disc-weighted scores match the category?")
    # Mean discipline scores for papers in different categories
    logging.info(
        paper_disc_score.groupby(["cat_assigned", "topic_category"])[
            "weighted_disc_score"
        ].mean()
    )

    logging.info(
        "Do papers with less -> more AI influence publish in different venues?"
    )

    # Discretise AI discipline scores into quintiles
    result_exploration_table = (
        paper_disc_score.query("topic_category=='ai'")
        .reset_index(drop=True)
        .assign(
            ai_quart=lambda df: pd.qcut(
                df["weighted_disc_score"], q=np.arange(0, 1.1, 0.2), labels=False
            )
        )
    )

    # Top publication venues for papers in different AI influence categories
    for q in range(5):
        logging.info(f"Top venues for papers in quintile of AI influence {q}")
        # Remember that genomic influence is 1 minus AI influence"
        results_selected = set(
            result_exploration_table.query(f"ai_quart=={q}")["work_id"]
        )

        results_meta = works_sampled.loc[
            works_sampled["work_id"].isin(results_selected)
        ]

        logging.info(
            results_meta["venue_display_name"].value_counts(normalize=True).head(n=10)
        )

        logging.info("\n")

    logging.info("Saving provisional results")
    save_to_s3(
        "ai-genomics",
        paper_disc_score.pivot_table(
            index=["work_id"], columns="topic_category", values="weighted_disc_score"
        ).to_dict(orient="index"),
        "outputs/analysis/openalex_influence_score.json",
    )
