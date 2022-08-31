# Script to explore AI definition

import logging
import pandas as pd
from collections import Counter
import re

from ai_genomics.pipeline.gtr import fetch_gtr
from ai_genomics import config, PROJECT_DIR

GTR_INPUTS_DIR = PROJECT_DIR / "inputs/data/gtr"
GTR_INPUTS_DIR.mkdir(exist_ok=True)


def camel_case_to_snake_case_col_names(df: pd.DataFrame) -> pd.DataFrame:
    """Turn column names from camelCase to snake_case"""
    return df.rename(columns=lambda x: re.sub(r"(?<!^)(?=[A-Z])", "_", x).lower())


if __name__ == "__main__":
    logging.info("Reading GtR data")
    projects = fetch_gtr("gtr_projects-projects")
    topics = fetch_gtr("gtr_projects-topic")

    # Topic distribution
    topic_distr = pd.Series(Counter([topic["text"] for topic in topics]))

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
                element["project_id"]
                for element in topics
                if any(t in element["text"] for t in terms)
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

    # Save project ids
    ai_genomics_combined.pipe(camel_case_to_snake_case_col_names).to_json(
        GTR_INPUTS_DIR / "gtr_ai_genomics_projects.json"
    )

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
