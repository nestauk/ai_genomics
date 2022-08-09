# Script to generate openalex definitions

from distutils.errors import LibError
import logging
import random
import re
from itertools import product, permutations
from toolz import pipe
from typing import Dict, Tuple, Union

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from ai_genomics import config, PROJECT_DIR
from ai_genomics.getters.openalex import (
    work_metadata,
    work_concepts,
    work_abstracts,
    get_concepts_df,
)
from ai_genomics.getters.papers_w_code import read_pwc_papers


def get_arxiv_id(ven: str) -> str:
    """Extracts the arxiv id from the venue field in works

    Args:
        ven: venue field in works

    Returns:
        the Arxiv id
    """

    if "arxiv" not in ven:
        return np.nan
    else:
        if "export" in ven:
            return ven.split("/")[-1]
        elif "/pdf/" in ven:
            if "pdf" in ven.split("/")[-1]:
                return ven.split("/")[-1][:-4]
            else:
                return ven.split("/")[-1]
        elif "pdf" in ven:
            return ven.split("/")[-1][:-4]

        else:
            return ven.split("/")[-1]


def flag_ambiguous(abstract: str) -> bool:
    """Flags ambiguous abstracts

    Args:
        abstract: abstract to check

    Returns:
        A flag for ambiguous abstracts

    """

    _abstract = abstract.lower()

    ml_terms = set(config["ai_terms"])

    if any(ed in _abstract for ed in ["education"]) & (
        sum([machine in _abstract for machine in ml_terms]) == 0
    ):
        return True
    elif any(ed in _abstract for ed in ["network", "neural", " eeg "]) & (
        sum([machine in _abstract for machine in ml_terms]) == 0
    ):
        return True
    elif any(ed in _abstract for ed in ["language", "linguistic", "syntactic"]) & (
        sum([machine in _abstract for machine in ml_terms]) == 0
    ):
        return True
    else:
        return False


def fetch_no_ai() -> set:
    """Get arxiv IT papers that don't fall in AI categories"""

    ai_cats = set(config["ai_cats"])
    return set(
        (
            pd.read_csv(f"{PROJECT_DIR}/inputs/data/arxiv/arxiv_article_categories.csv")
            .groupby("article_id")["category_id"]
            .apply(lambda x: (len(set(x) & ai_cats) == 0))
            .reset_index(drop=False)
            .query("category_id==True")["article_id"]
        )
    )


def filter_works(works_meta: pd.DataFrame, abstracts: Dict) -> pd.DataFrame:
    """Filters the data including to remove non-english docs,
    docs with no abstracts and docs with ambiguous abstracts
    """

    return (
        works_meta.query("predicted_language=='en'")
        .query("has_abstract==True")
        .dropna(axis=0, subset=["venue_url"])
        .reset_index(drop=False)
        .assign(arxiv_id=lambda df: df["venue_url"].apply(get_arxiv_id))
        .assign(
            ambiguous=lambda df: [
                flag_ambiguous(abstracts[work_id]) for work_id in df["work_id"]
            ]
        )
        .query("ambiguous == False")
        .reset_index(drop=True)
    )


def definition_evaluation(
    meta_df: pd.DataFrame,
    concepts_df: pd.DataFrame,
    abstracts: Dict,
    selected_concepts: Dict,
    print_examples: bool = True,
    sample: int = 10,
    inclusive: bool = True,
    return_included: bool = False,
) -> Union[Tuple, pd.DataFrame]:
    """Evaluates the imapct of different definitions of AI

    Args:
        meta_df: dataframe of works metadata
        concepts_df: dataframe of openalex concepts
        abstracts: dictionary of abstracts
        selected_concepts: dictionary with concepts and score thresholds
        print_examples: flag to print examples
        sample: number of random examples to print if we are printing examples
        inclusive: flag to addopt an or or and criterion when selecting papers
            (inclusive includes any papers above score for any concept,
            exclusive requires all papers to be above score in the concept)
        return_included: flag to return a df with included papers

    Returns:
        Evaluation results and included papers or just evaluation results

    """

    logging.info(selected_concepts)

    included, excluded = subset_on_concepts(
        meta_df, concepts_df, selected_concepts, inclusive, return_excluded=True
    )

    tp = included["arx_ai_conf"].sum()
    fp = included["arx_no_ai"].sum()
    tn = excluded["arx_no_ai"].sum()
    fn = excluded["arx_ai_conf"].sum()

    tp_rate = tp / (tp + tn)
    fp_rate = fp / (tn + fp)
    fn_rate = fn / (tn + tp)
    tn_rate = tn / (tn + fp)
    precision = tp / (tp + fp)
    recall = tp / (tp + fn)
    f1_score = 2 * (precision * recall) / (precision + recall)

    if print_examples:
        random_sample = [
            (year, included.query(f"publication_year=={year}").sample(sample))
            for year in included["publication_year"].unique()
        ]

        random_excluded = [
            (year, excluded.query(f"publication_year=={year}").sample(sample))
            for year in excluded["publication_year"].unique()
        ]

        for r in random_sample:

            logging.info(r[0])
            logging.info("===")
            logging.info("\n")
            for ind, row in r[1].iterrows():

                logging.info(row["display_name"])
                logging.info("".join(["-"] * len(row["display_name"])))
                logging.info(row["work_id"])
                logging.info(abstracts[row["work_id"]])
                logging.info("\n")

        for r in random_excluded:

            logging.info("EXCLUDED")
            logging.info(r[0])
            logging.info("===")
            logging.info("\n")
            for ind, row in r[1].iterrows():

                logging.info(row["display_name"])
                logging.info("".join(["-"] * len(row["display_name"])))
                logging.info(row["work_id"])
                logging.info(abstracts[row["work_id"]])
                logging.info("\n")

        logging.info("\n \n \n")

    eval_results = {
        "concepts": ", ".join(
            ["_".join([k, str(np.round(v, 3))]) for k, v in selected_concepts.items()]
        ),
        "num_works": len(included),
        "tp_rate": tp_rate,
        "fp_rate": fp_rate,
        "fn_rate": fn_rate,
        "tn_rate": tn_rate,
        "precision": precision,
        "recall": recall,
        "f1_score": f1_score,
    }

    if return_included is True:
        return eval_results, included

    else:
        return eval_results


def subset_on_concepts(
    works_meta: pd.DataFrame,
    works_concepts: pd.DataFrame,
    selected_concepts: Dict,
    inclusive: bool = True,
    return_excluded: bool = True,
) -> pd.DataFrame:
    """Subsets the data on the selected concepts

    Args:
        meta_df: dataframe of works metadata
        concepts_df: dataframe of openalex concepts
        abstracts: dictionary of abstracts
        selected_concepts: dictionary with concepts and score thresholds
        inclusive: flag to addopt an or or and criterion when selecting papers
            (inclusive includes any papers above score for any concept,
            exclusive requires all papers to be above score in the concept)
        return_excluded: flag to also return excluded concepts

    Returns:
        A dataframe with included results
    """

    if inclusive:
        rel_papers = pipe(
            works_concepts.loc[
                [
                    score > selected_concepts[conc]
                    if conc in selected_concepts.keys()
                    else False
                    for conc, score in zip(
                        works_concepts["display_name"].values,
                        works_concepts["score"].values,
                    )
                ]
            ]["doc_id"],
            set,
        )
    else:
        rel_papers = pipe(
            works_concepts.assign(
                thres=lambda df: df["display_name"].map(selected_concepts)
            )
            .assign(high=lambda df: df["score"] > df["thres"])
            .query("high==True")
            .groupby("doc_id")
            .size()
            > (len(selected_concepts) - 1),
            lambda df: df.loc[df == True].index,
            set,
        )

    if return_excluded:
        return (
            works_meta.loc[works_meta["work_id"].isin(rel_papers)].reset_index(
                drop=True
            ),
            works_meta.loc[~works_meta["work_id"].isin(rel_papers)].reset_index(
                drop=True
            ),
        )
    else:
        return works_meta.loc[works_meta["work_id"].isin(rel_papers)].reset_index(
            drop=True
        )


def get_papers_with_concept(
    works_df: pd.DataFrame,
    concepts_df: pd.DataFrame,
    concept_list: list,
    concept_name="display_name",
) -> pd.DataFrame:
    """Gets df with papers with a concept"""

    return pipe(
        works_df,
        lambda df: df.loc[
            df["work_id"].isin(
                pipe(
                    concepts_df.loc[concepts_df[concept_name].isin(concept_list)][
                        "doc_id"
                    ],
                    set,
                )
            )
        ],
    )


if __name__ == "__main__":

    logging.info("Getting AI papers ids")

    pwc = read_pwc_papers().dropna(axis=0, subset=["proceeding"]).reset_index(drop=True)

    ai_confs = {
        name
        for name in pwc["proceeding"].unique()
        if any(c in name.lower() for c in config["ai_conf_acronyms"])
    }

    pwc_ai_ids = pipe(
        pwc.loc[pwc["proceeding"].isin(ai_confs)].reset_index(drop=True),
        lambda df: set(df["arxiv_id"]) - set([np.nan]),
    )

    logging.info("Getting non-AI papers ids")
    no_ai_arxiv = fetch_no_ai()

    logging.info("Getting Openalex data")

    works_meta = work_metadata("artificial_intelligence", [2012, 2017, 2021])
    concepts = work_concepts(
        "artificial_intelligence", "concepts", year_list=[2012, 2017, 2021]
    )
    abstracts = work_abstracts("artificial_intelligence", years=[2012, 2017, 2021])

    works_meta_filtered = filter_works(works_meta, abstracts)

    works_meta_labelled = works_meta_filtered.assign(
        arx_ai_conf=lambda df: [aid in pwc_ai_ids for aid in df["arxiv_id"]]
    ).assign(arx_no_ai=lambda df: [aid in no_ai_arxiv for aid in df["arxiv_id"]])

    logging.info(works_meta_labelled[["arx_ai_conf", "arx_no_ai"]].sum())

    logging.info("Evaluating combinations)")
    # Consider a single concept
    uni_combis = list(
        product(
            config["selection_criteria"]["concepts"],
            np.arange(*config["selection_criteria"]["thresholds"]),
        )
    )
    search_uni = [{item[0]: item[1]} for item in uni_combis]

    # Consider multiple concepts
    # This stitches the concepts and permutations of thresholds to do grid search

    search_multi = [
        {k: val for k, val in zip(config["selection_criteria"]["concepts"], vals)}
        for vals in list(
            permutations(np.arange(*config["selection_criteria"]["thresholds"]))
        )
    ]

    logging.info("Run evaluation")
    eval_results = pd.concat(
        [
            pd.DataFrame(
                [
                    definition_evaluation(
                        works_meta_labelled,
                        concepts,
                        abstracts,
                        selected_concepts,
                        print_examples=False,
                        inclusive=inc_bool,
                    )
                    for selected_concepts in [*search_uni, *search_multi]
                ]
            ).assign(inclusive=inc_bool)
            for inc_bool in [True, False]
        ]
    ).sort_values("f1_score", ascending=False)

    logging.info(eval_results.head(n=30))

    logging.info("Best criteria")
    logging.info(eval_results.iloc[0, :])

    best_score = eval_results.iloc[0]
    best_criteria = {
        criterion.split("_")[0]: float(criterion.split("_")[1])
        for criterion in best_score["concepts"].split(", ")
    }
    best_inclusive = best_score["inclusive"]

    logging.info("Print examples of the best definition")
    _, __ = definition_evaluation(
        works_meta_labelled,
        concepts,
        abstracts,
        best_criteria,
        print_examples=True,
        inclusive=best_inclusive,
        return_included=True,
    )

    logging.info("Apply criteria to AI dataset")
    all_works = work_metadata("artificial_intelligence", range(2012, 2022))
    all_concepts = work_concepts(
        "artificial_intelligence", "concepts", range(2012, 2022)
    )
    all_ai_mesh = work_concepts("artificial_intelligence", "mesh", range(2012, 2022))
    all_abstracts = work_abstracts("artificial_intelligence", range(2012, 2022))

    full_size = (
        all_works.query("predicted_language=='en'").query("has_abstract==True").shape[0]
        / 1e6
    )

    logging.info(f"Size of full dataset in english and with abstracts:{full_size} M")

    all_works_filtered = filter_works(all_works, all_abstracts)

    all_works_provisional = subset_on_concepts(
        all_works_filtered,
        all_concepts,
        best_criteria,
        inclusive=best_inclusive,
        return_excluded=False,
    )

    logging.info(f"provisional dataset size: {all_works_provisional.shape[0] / 1e6} M")

    all_works_provisional.to_csv(
        f"{PROJECT_DIR}/inputs/data/openalex/ai_openalex_corpus.csv", index=False
    )

    logging.info("Exploration of genomics data")

    concept_df = get_concepts_df()

    # Genomics concepts table
    genom_concs_df = concept_df.loc[
        ["genom" in conc.lower() for conc in concept_df["display_name"].values]
    ].sort_values("works_count", ascending=True)

    # Genomics concepts set
    genomics_concepts = set(genom_concs_df["display_name"])

    # Plot (in matplotlib!!!)
    fig, ax = plt.subplots(figsize=(9, 10))

    genom_concs_df.plot(
        kind="barh", x="display_name", y="works_count", legend=False, ax=ax
    )
    ax.set_ylabel("Concept", fontsize=12)
    ax.set_xlabel("Number of works", fontsize=12)
    plt.tight_layout()
    plt.savefig(f"{PROJECT_DIR}/outputs/figures/genomics_concepts.png")

    # Read genomics papers and concepts
    logging.info("Reading genetics works")
    y = range(2012, 2022)
    works_meta_genetics = work_metadata("genetics", y)
    works_concepts_genetics = work_concepts("genetics", "concepts", y)
    works_mesh_genetics = work_concepts("genetics", "mesh", y)
    abstracts_genetics = work_abstracts("genetics", y)

    logging.info(f"genetics total {len(works_meta_genetics)}")

    works_meta_genetics_filtered = (
        works_meta_genetics.query("predicted_language=='en'")
        .query("has_abstract==True")
        .reset_index(drop=True)
    )

    logging.info(f"genetics filtered total {len(works_meta_genetics_filtered)}")

    # Genetics papers with genomics concepts

    works_meta_genomics = get_papers_with_concept(
        works_meta_genetics_filtered, works_concepts_genetics, genomics_concepts
    )

    logging.info(f"genomics papers in genetics {len(works_meta_genomics)}")

    # Look at mesh genomics terms
    mesh_genomics_terms = [
        term
        for term in works_mesh_genetics["descriptor_name"].unique()
        if "genom" in term.lower()
    ]

    works_meta_genomics_mesh = get_papers_with_concept(
        works_meta_genetics_filtered,
        works_mesh_genetics,
        mesh_genomics_terms,
        concept_name="descriptor_name",
    )

    logging.info(f"mesh genomics papers in genetics {len(works_meta_genomics_mesh)}")

    # Check overlap with AI. This code neds to be simplified badly
    # Concepts
    ai_works_meta_genomics = get_papers_with_concept(
        all_works_provisional, all_concepts, genomics_concepts
    )

    logging.info(f"AI with genomics concepts {len(ai_works_meta_genomics)}")

    # Mesh
    ai_works_mesh_genomics = get_papers_with_concept(
        all_works_provisional,
        all_ai_mesh,
        mesh_genomics_terms,
        concept_name="descriptor_name",
    )

    logging.info(f"AI with genomics mesh {len(ai_works_mesh_genomics)}")

    overlap = len(
        set(ai_works_meta_genomics["work_id"]).union(
            set(ai_works_mesh_genomics["work_id"])
        )
    )

    logging.info(f"AI with genomics concept or mesh {overlap}")

    logging.info("Crude genomic abstract search")
    non_empty_abstracts = {k: v for k, v in all_abstracts.items() if type(v) == str}
    crude_genomic_search = {
        k: v for k, v in non_empty_abstracts.items() if "genom" in v
    }

    all_genomics_ids = (
        set(ai_works_meta_genomics["work_id"])
        .union(set(ai_works_mesh_genomics["work_id"]))
        .union(set(crude_genomic_search.keys()))
    )
    ai_genomics_all_approaches = all_works_provisional.loc[
        all_works_provisional["work_id"].isin(all_genomics_ids)
    ].reset_index(drop=True)

    logging.info(
        f"AI with genomics concept through all search strategies {len(ai_genomics_all_approaches)}"
    )

    logging.info("Crude search for AI papers in the genetics dataset")
    ai_terms_genetics = config["ai_terms_genetics"]

    non_empty_genetics = {k: v for k, v in abstracts_genetics.items() if type(v) == str}
    crude_genomic_ai_search = {
        k: v
        for k, v in non_empty_genetics.items()
        if any(t in v for t in ai_terms_genetics)
    }
    genetic_ai_abstract_ids = set(crude_genomic_ai_search.keys())

    logging.info(f"Genetics papers with AI terms: {len(genetic_ai_abstract_ids)}")

    logging.info("Look for genetics papers with AI concepts")
    genetic_ai_definition_ids = pipe(
        subset_on_concepts(
            works_meta_genetics_filtered,
            works_concepts_genetics,
            {"Artificial intelligence": 0, "Machine learning": 0, "Deep learning": 0},
            inclusive=True,
            return_excluded=False,
        )["work_id"],
        set,
    )

    logging.info(f"Genetics papers with AI concepts: {len(genetic_ai_definition_ids)}")

    logging.info("Combine everything")
    ai_gen_total_ids = all_genomics_ids.union(genetic_ai_abstract_ids).union(
        genetic_ai_definition_ids
    )

    logging.info(f"Genetics papers in all approaches: {len(ai_gen_total_ids)}")

    # Provisional dataset
    ai_genomics_provisional_dataset = (
        pd.concat(
            [
                all_works_provisional.loc[
                    all_works_provisional["work_id"].isin(ai_gen_total_ids)
                ],
                works_meta_genetics.loc[
                    works_meta_genetics["work_id"].isin(ai_gen_total_ids)
                ],
            ]
        )
        .drop_duplicates("work_id")
        .reset_index(drop=True)
    )

    ai_genomics_provisional_dataset.to_csv(
        f"{PROJECT_DIR}/outputs/ai_genomics_provisional_dataset.csv"
    )

    combined_abstracts = {**all_abstracts, **abstracts_genetics}

    # Create example table
    ai_genomics_example_table = []

    for _, sampled in ai_genomics_provisional_dataset.sample(5).iterrows():

        ai_genomics_example_table.append(
            {
                "title": sampled["display_name"],
                "abstract (truncated)": combined_abstracts[sampled["work_id"]][:700],
            }
        )

    logging.info(pd.DataFrame(ai_genomics_example_table).head())

    pd.DataFrame(ai_genomics_example_table).to_markdown(
        f"{PROJECT_DIR}/outputs/openalex_examples.md", index=False
    )
