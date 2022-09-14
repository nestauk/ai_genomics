"""Script to generate look up tables across datasets (ai and genomics, ai, genomics)
where output is {id: abstract} for DBpedia tagging."""

import pandas as pd
from datetime import datetime as date
import os

from ai_genomics import bucket_name
from ai_genomics.getters.data_getters import save_to_s3

from ai_genomics.getters.gtr import get_gtr_from_s3
from ai_genomics.getters.openalex import _get_openalex_ai_genomics_abstracts
from ai_genomics.getters.patents import get_ai_genomics_patents
from ai_genomics.getters.crunchbase import get_ai_genomics_crunchbase_org_ids
from ai_genomics.utils.crunchbase import fetch_crunchbase, parse_s3_table

# TO DOs
# wait on baseline PRs to be merged into dev to be able to generate lookups also
# for baseline datasets (97b_gtr_baseline, 97_crunchbase_baseline, 92_codes_with_descriptions)

LOOKUP_TABLE_PATH = "inputs/lookup_tables/"

if __name__ == "__main__":

    ai_genomics_patents_lookup = (
        get_ai_genomics_patents()
        .query("abstract_language == 'en'")
        .set_index("publication_number")["abstract_text"]
        .to_dict()
    )

    ai_genomics_gtr_lookup = (
        get_gtr_from_s3("projects").set_index("id")["abstractText"].to_dict()
    )

    cb_orgs = parse_s3_table(fetch_crunchbase("orgs"))[
        ["id", "long_description", "short_description"]
    ]

    ai_genomics_crunchbase_lookup = (
        get_ai_genomics_crunchbase_org_ids()
        .merge(cb_orgs, how="inner", left_on="cb_org_id", right_on="id")
        .assign(description=lambda x: x.long_description.fillna(x.short_description))
        .query("description.notnull()")
        .set_index("id")["description"]
        .to_dict()
    )

    ai_genomics_openalex_lookup = _get_openalex_ai_genomics_abstracts()

    date_stamp = str(date.today().date()).replace("-", "")

    for table_name, lookup_table in zip(
        ("patents", "grt", "cb", "openalex"),
        (
            ai_genomics_patents_lookup,
            ai_genomics_gtr_lookup,
            ai_genomics_crunchbase_lookup,
            ai_genomics_openalex_lookup,
        ),
    ):
        lookup_table_name = os.path.join(
            LOOKUP_TABLE_PATH, f"{date_stamp}_ai_genomics_{table_name}_lookup.json"
        )
        save_to_s3(bucket_name, lookup_table, lookup_table_name)
