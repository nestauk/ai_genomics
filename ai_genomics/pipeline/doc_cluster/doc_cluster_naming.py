import pandas as pd
import numpy as np

from ai_genomics import PROJECT_DIR
from ai_genomics.getters.clusters import get_doc_cluster_lookup
from ai_genomics.getters.gtr import get_ai_genomics_project_table
from ai_genomics.getters.openalex import get_openalex_ai_genomics_works
from ai_genomics.getters.patents import get_ai_genomics_patents


MAX_SAMPLES = 10


patents = get_ai_genomics_patents()
gtr_projects = get_ai_genomics_project_table()
oa_works = get_openalex_ai_genomics_works()

gtr_title_lookup = dict(zip(gtr_projects["id"], gtr_projects["title"]))
pat_title_lookup = dict(zip(patents["publication_number"], patents["title_text"]))
oa_title_lookup = dict(zip(oa_works["work_id"], oa_works["display_name"]))

clusters = get_doc_cluster_lookup(min_year=2012, max_year=2021)

records = []
for source, source_clusters in clusters.items():
    for cluster_id, ids in source_clusters.items():
        n = MAX_SAMPLES if len(ids) >= MAX_SAMPLES else len(ids)
        sample_ids = np.random.choice(ids, n, replace=False)
        for i in sample_ids:
            record = {
                "source": source,
                "id": i,
                "cluster": cluster_id,
            }
            if source == "oa":
                record.update(
                    {
                        "title": oa_title_lookup[i],
                    }
                )
            if source == "gtr":
                record.update(
                    {
                        "title": gtr_title_lookup[i],
                    }
                )
            if source == "pat":
                record.update({"title": pat_title_lookup[i]})
            records.append(record)

(
    pd.DataFrame.from_records(records)
    .sort_values(["cluster", "source"])
    .to_csv(PROJECT_DIR / "outputs/doc_cluster_naming.csv", index=False)
)
