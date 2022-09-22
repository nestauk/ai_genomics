# Script to explore AI definition

import boto3
import logging
import pandas as pd
from collections import Counter
import re
from toolz import pipe

from ai_genomics.pipeline.gtr.gtr_utils import fetch_gtr
from ai_genomics.getters.data_getters import save_to_s3
from ai_genomics import config, PROJECT_DIR

GTR_INPUTS_DIR = PROJECT_DIR / "inputs/data/gtr"
GTR_INPUTS_DIR.mkdir(exist_ok=True)

GTR_OUTPUTS_DIR = PROJECT_DIR / "outputs/data/gtr"
GTR_OUTPUTS_DIR.mkdir(exist_ok=True)

GTR_PROJ_NAME = "gtr_ai_genomics_projects.csv"

KEEP_GTR_VARS = [
    "id",
    "project_id",
    "title",
    "start",
    "grant_category",
    "abstract_text",
    "potential_impact",
]


def camel_case_to_snake_case_col_names(df: pd.DataFrame) -> pd.DataFrame:
    """Turn column names from camelCase to snake_case"""
    return df.rename(columns=lambda x: re.sub(r"(?<!^)(?=[A-Z])", "_", x).lower())


def send_output_to_s3(file_path: str, s3_destination: str):
    """TODO: PUT THIS IN UTILS"""
    s3 = boto3.resource("s3")
    (s3.Bucket("ai-genomics").upload_file(file_path, f"outputs/{s3_destination}"))


if __name__ == "__main__":
    logging.info("Reading GtR data")
    projects = fetch_gtr("gtr_projects-projects")
    topics = fetch_gtr("gtr_projects-topic")

    # Topic distribution
    topic_distr = pd.Series(Counter(topics["text"].values()))

    relevant_concepts = topic_distr.loc[
        [
            any(t in conc.lower() for t in config["gtr_topic_search"])
            for conc in topic_distr.index
        ]
    ]

    logging.info(relevant_concepts)

    # Get the project with relevant topics from the config file
    ai_topics, genomics_topics = [
        config[f"gtr_{disc}_concepts"] for disc in ["ai", "genom"]
    ]

    ai_projs, genom_projs = [
        set(
            [
                pid
                for pid, text in zip(
                    topics["project_id"].values(), topics["text"].values()
                )
                if any(t in text for t in terms)
            ]
        )
        for terms in [ai_topics, genomics_topics]
    ]

    logging.info(f"AI projects: {len(ai_projs)}")
    logging.info(f"Genomics projects: {len(genom_projs)}")

    # Look at abstracts
    ai_abstract_terms, genom_abstract_terms = [
        config[f"gtr_{t}_abstract"] for t in ["ai", "genom"]
    ]

    ai_projs_abstr, genom_projs_abstr = [
        set(
            [
                gtr_id
                for gtr_id, abstract_text, title in zip(
                    projects["id"].values(),
                    projects["abstractText"].values(),
                    projects["title"].values(),
                )
                if any(  # If any of the abstract terms are in the abstract or title
                    t in str(abstract_text).lower() + str(title).lower() for t in terms
                )
            ]
        )
        for terms in [ai_abstract_terms, genom_abstract_terms]
    ]

    logging.info(f"Projects with AI abstract / title: {len(ai_projs_abstr)}")
    logging.info(f"Projects with genomics abstract / title: {len(genom_projs_abstr)}")
    logging.info(
        f"Projects with AI and genomics abstract / title: {len(ai_projs_abstr & genom_projs_abstr)}"
    )

    #  Intersection of the two sets
    unified_projects = list(
        set(ai_projs_abstr.union(ai_topics) & genom_projs_abstr.union(genomics_topics))
    )

    ai_genomics_combined = (
        pd.DataFrame.from_dict(projects)
        .query(f"id in {unified_projects}")
        .reset_index(drop=True)
    )

    logging.info(f"AI and genomics projects combined: {len(ai_genomics_combined)}")

    # Get project examples
    project_examples = ai_genomics_combined.sample(5)[["title", "abstractText"]].rename(
        columns={"title": "Project title"}
    )
    project_examples["Abstract (truncated)"] = (
        project_examples["abstractText"].str[:700] + "..."
    )
    project_examples = project_examples[["Project title", "Abstract (truncated)"]]
    project_examples.to_markdown(f"{PROJECT_DIR}/outputs/gtr_examples.md", index=False)

    logging.info(project_examples)

    # Save relevant (AI and genomics, AI, genomics) projects locally and to s3
    ai_genomics_combined.pipe(camel_case_to_snake_case_col_names).to_json(
        GTR_INPUTS_DIR / "gtr_ai_genomics_projects.json"
    )

    projects_df = pipe(pd.DataFrame(projects), camel_case_to_snake_case_col_names)[
        KEEP_GTR_VARS
    ]

    projects_df["ai"], projects_df["genomics"], projects_df["ai_genomics"] = [
        projects_df["id"].isin(list_ids)
        for list_ids in [
            ai_projs_abstr,
            genom_projs_abstr,
            ai_projs_abstr & genom_projs_abstr,
        ]
    ]

    filtered_projects = projects_df.loc[
        projects_df[["ai", "genomics", "ai_genomics"]].values.sum(axis=1) > 0
    ].reset_index(drop=True)

    filtered_projects.to_csv(GTR_OUTPUTS_DIR / GTR_PROJ_NAME, index=False)

    save_to_s3("ai-genomics", filtered_projects, f"outputs/gtr/{GTR_PROJ_NAME}")

    # Get publications from projects
    publications_from_projects = fetch_gtr("gtr_projects-outcomes_publications")

    genom_publs = pd.DataFrame(
        [
            pap
            for pap in publications_from_projects
            if pap["project_id"] in ai_genomics_combined.id.to_list()
        ]
    )

    logging.info(f"AI and genomics publications: {len(genom_publs)}")

    genom_publs.pipe(camel_case_to_snake_case_col_names).to_json(
        GTR_INPUTS_DIR / "gtr_ai_genomics_publications.json"
    )
