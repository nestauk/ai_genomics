import logging
import pandas as pd
from functools import partial
from toolz import pipe
from collections import ChainMap
import os
import ai_genomics.analysis.influence.influence_analysis as infl
from ai_genomics.getters.data_getters import load_s3_data
from ai_genomics.getters.patents import (
    get_ai_genomics_patents,
    get_ai_sample_patents,
    get_genomics_sample_patents,
)
import ai_genomics.getters.gtr as gtr

from ai_genomics import PROJECT_DIR

os.makedirs(f"{PROJECT_DIR}/outputs/data/openalex", exist_ok=True)
os.makedirs(f"{PROJECT_DIR}/outputs/data/patstat", exist_ok=True)
os.makedirs(f"{PROJECT_DIR}/outputs/data/gtr", exist_ok=True)


def sample_getter(name):
    """Temporary AI and genomics sample getter"""

    return load_s3_data("ai-genomics", f"outputs/openalex/{name}")


DISCS = ["ai_genomics", "ai", "genomics"]


def make_influence_openalex(sample_size: int = 80000, num_topics: int = 200):
    """Makes and saves openalex influence tables"""

    logging.info("Making openalex influence scores")

    combined_works = (
        pd.concat(
            [sample_getter(f"{d}_openalex_works.csv").assign(source=d) for d in DISCS]
        )
        .drop_duplicates("work_id")
        .reset_index(drop=True)
    )
    combined_abstracts = dict(
        ChainMap(*[sample_getter(f"{d}_openalex_abstracts.json") for d in DISCS])
    )

    # Create sample
    combined_sample = (
        combined_works.loc[
            combined_works["work_id"].isin(set(combined_abstracts.keys()))
        ]
        .drop_duplicates("work_id")
        .sample(n=sample_size)
    )
    sampled_ids = set(combined_sample["work_id"])
    abstracts_sample = {
        _id: abstr for _id, abstr in combined_abstracts.items() if _id in sampled_ids
    }

    id_disc_lookup = {
        _id: disc
        for _id, disc in zip(
            combined_sample["work_id"].values, combined_sample["source"].values
        )
    }

    # Process text and fit topic model
    topic_df = pipe(
        abstracts_sample,
        infl.text_processing,
        partial(infl.make_topic_distribution, num_topics=num_topics),
    )

    # Generate influence outputs
    topic_shares = infl.calculate_topic_shares(
        topic_df, id_disc_lookup, baseline_discs=["ai", "genomics"]
    )

    topic_shares.to_csv(
        f"{PROJECT_DIR}/outputs/data/openalex/influence_topic_shares.csv", index=False
    )

    logging.info(topic_shares.head())

    infl.calculate_influence_score(topic_df, topic_shares).reset_index().rename(
        columns={"topic_disc_weighted": "disc_influence"}
    ).to_csv(f"{PROJECT_DIR}/outputs/data/openalex/influence_scores.csv", index=False)


def make_influence_patstat(sample_size: int = 3000, num_topics: int = 200):
    """Makes and saves patent influence tables"""

    logging.info("Making openalex influence scores")

    getters = [
        get_ai_genomics_patents,
        get_ai_sample_patents,
        get_genomics_sample_patents,
    ]

    # We remove duplicated patent families here
    combined_patents = (
        pd.concat(
            [  # We sample here to ensure a balanced number of patents from both baselines
                getter().assign(source=d)
                if d == "ai_genomics"
                else getter().sample(n=sample_size).assign(source=d)
                for getter, d in zip(getters, DISCS)
            ]
        )
        .query("abstract_language=='en'")
        .dropna(axis=0, subset=["abstract_text"])
        .drop_duplicates("family_id")
        .reset_index(drop=True)
    )
    pat_fam_id_lookup = combined_patents.set_index("publication_number")[
        "source"
    ].to_dict()

    pat_topic_df = pipe(
        combined_patents.set_index("publication_number")["abstract_text"].to_dict(),
        infl.text_processing,
        partial(infl.make_topic_distribution, num_topics=num_topics),
    )

    patent_topic_shares = infl.calculate_topic_shares(pat_topic_df, pat_fam_id_lookup)

    patent_topic_shares.to_csv(
        f"{PROJECT_DIR}/outputs/data/patstat/influence_topic_shares.csv", index=False
    )

    logging.info(patent_topic_shares.head())

    infl.calculate_influence_score(
        pat_topic_df, patent_topic_shares
    ).reset_index().rename(columns={"topic_disc_weighted": "disc_influence"}).to_csv(
        f"{PROJECT_DIR}/outputs/data/patstat/influence_scores.csv", index=False
    )


def make_influence_gtr(sample_size: int = 2000, num_topics: int = 200):
    """Makes and saves GTR influence tables"""

    logging.info("Making GTR influence scores")

    gtr_meta = gtr.get_ai_genomics_project_table(local=False)

    # We need to sample baselines of similar sizes. Otherwise the shares
    # (and perhaps the topics?) will be unbalanced
    gtr_meta_sampled = pd.concat(
        [
            gtr_meta.query("ai_genomics==True"),
            gtr_meta.query("ai_genomics==False").query("ai==True").sample(sample_size),
            gtr_meta.query("ai_genomics==False")
            .query("genomics==True")
            .sample(sample_size),
        ]
    )

    gtr_text = {
        _id: text
        for _id, text in zip(gtr_meta_sampled["id"], gtr_meta_sampled["abstract_text"])
    }

    gtr_topic_df = pipe(
        gtr_text,
        infl.text_processing,
        partial(infl.make_topic_distribution, num_topics=num_topics),
    )

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

    disc_id_lookup = {_id: cat for cat, id_list in disc_ids.items() for _id in id_list}

    gtr_topic_shares = infl.calculate_topic_shares(
        gtr_topic_df, disc_id_lookup, baseline_discs=["ai", "genomics"]
    )

    gtr_topic_shares.to_csv(
        f"{PROJECT_DIR}/outputs/data/gtr/influence_topic_shares.csv", index=False
    )

    logging.info(gtr_topic_shares.head())

    infl.calculate_influence_score(gtr_topic_df, gtr_topic_shares).reset_index().rename(
        columns={"topic_disc_weighted": "disc_influence"}
    ).to_csv(f"{PROJECT_DIR}/outputs/data/gtr/influence_scores.csv", index=False)


if __name__ == "__main__":

    make_influence_openalex()

    make_influence_patstat()

    make_influence_gtr()
