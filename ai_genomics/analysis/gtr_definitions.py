# Script to explore AI definition

import json
import logging
import random
import pandas as pd
from collections import Counter

from ai_genomics.pipeline.gtr import fetch_gtr
from ai_genomics import config, PROJECT_DIR

GTR_INPUTS_DIR = PROJECT_DIR / "inputs/data/gtr"
GTR_INPUTS_DIR.mkdir(exist_ok=True)

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
                element["id"]
                for element in projects
                if any(  # If any of the abstract terms are in the abstract or title
                    t
                    in str(element["abstractText"]).lower()
                    + str(element["title"]).lower()
                    for t in terms
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
    unified_projects = set(
        ai_projs_abstr.union(ai_topics) & genom_projs_abstr.union(genomics_topics)
    )

    # Extract projects (NB there are duplicates in the projects list)
    rel_projects = [proj for proj in projects if proj["id"] in unified_projects]

    ai_genomics_combined = []
    used_ids = []

    # We use this to catch dupes
    for _id in set([proj["id"] for proj in rel_projects]):
        for p in rel_projects:
            if _id not in used_ids and p["id"] == _id:
                ai_genomics_combined.append(p)
                used_ids.append(_id)

    logging.info(f"AI and genomics projects combined: {len(ai_genomics_combined)}")

    # Get project examples
    project_examples = [
        {
            "Project title": sampled["title"],
            "Abstract (truncated)": sampled["abstractText"][:700] + "...",
        }
        for sampled in random.sample(ai_genomics_combined, 5)
    ]

    pd.DataFrame(project_examples).to_markdown(
        f"{PROJECT_DIR}/outputs/gtr_examples.md", index=False
    )

    logging.info(pd.DataFrame(project_examples))

    # Save project ids
    with open(GTR_INPUTS_DIR / "gtr_projects.json", "w") as f:
        json.dump(ai_genomics_combined, f)

    publications_from_projects = fetch_gtr("gtr_projects-outcomes_publications")

    # Get publications from projects
    genom_publs = [
        pap for pap in publications_from_projects if pap["project_id"] in used_ids
    ]

    logging.info(f"Genomics publications: {len(genom_publs)}")
