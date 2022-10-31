from collections import defaultdict
from hdbscan import HDBSCAN
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.decomposition import TruncatedSVD
from sklearn.cluster import KMeans
from sklearn.pipeline import Pipeline
from typing import Sequence
from umap import UMAP
from yellowbrick.cluster import KElbowVisualizer

from ai_genomics import PROJECT_DIR
from ai_genomics.getters.openalex import (
    get_openalex_ai_genomics_works,
    get_openalex_ai_genomics_works_embeddings,
    get_openalex_ai_entities,
    get_openalex_ai_genomics_works_entity_groups,
)

from ai_genomics.getters.patents import (
    get_ai_genomics_patents,
    get_ai_genomics_patents_entities,
    get_patent_ai_genomics_abstract_embeddings,
    get_patent_ai_genomics_entity_groups,
)


LANG = "en"
K_MACRO_ENTITIES = 100
AI_MACRO_ENTITY_COLS = [87, 88, 98]
MIN_YEAR = 2020
MAX_YEAR = 2021
N_CLUSTERS = 20


def subset_oa_recent_in_scope(
    works: pd.DataFrame,
    min_year: int,
    max_year: int,
    lang: str,
    scope_cols: Sequence[str],
) -> pd.DataFrame:

    in_scope = " and ".join(scope_cols)
    return (
        works.query(in_scope)
        .query("publication_year >= @min_year and publication_year <= @max_year")
        .query("predicted_language == @lang")
        .drop_duplicates("work_id")
    )


def subset_pat_recent_in_scope(
    patents: pd.DataFrame,
    min_year: int,
    max_year: int,
    lang: str,
    scope_cols: Sequence[str],
) -> pd.DataFrame:

    in_scope = " and ".join(scope_cols)
    return (
        patents.assign(
            publication_year=pd.to_datetime(patents["publication_date"]).dt.year
        )
        .query("publication_year >= @min_year and publication_year <= @max_year")
        .query(in_scope)
        .query("abstract_language == @lang")
        .drop_duplicates("publication_number")
    )


def ai_macro_entity_ids(
    macro_entities: pd.DataFrame, ai_cols: List[str]
) -> pd.DataFrame:
    """_summary_

    Args:
        macro_entities (pd.DataFrame): _description_
        ai_cols (List[str]): _description_

    Returns:
        pd.DataFrame: _description_
    """
    return (
        macro_entities.set_index("id")[ai_cols]
        .sum(axis=1)
        .pipe(lambda x: x[x > 0])
        .index.values
    )


def normalize_embedding_cols(embeddings: pd.DataFrame):
    """Resolves issue with embedding column names."""
    embeddings.set_index("Unnamed: 0")
    embeddings.index.name = "id"
    embeddings = embeddings.rename(columns={c: int(c) for c in embeddings.columns})
    return embeddings


def id_to_source(uid):
    """Identifies the data source from a unique ID, such as an OpenAlex work ID, patent
    number, GtR project ID or Crunchbase company ID.
    """
    if uid.startswith("https:"):
        return "oa"
    elif len(uid.split("-")) == 3:
        return "pat"
    elif uid.split("-")[0].islower():
        return "cb"
    else:
        return "gtr"


def make_cluster_to_id_lookup(ids, cluster_labels):
    """Creates a cluster to ID lookup, separated by data source"""
    sources = list(map(id_to_source, ids))
    doc_clusters = {k: defaultdict(list) for k in set(sources)}

    for idx, source, cluster in zip(ids, sources, km.labels_):
        doc_clusters[source][cluster].append(idx)

    return doc_clusters


if __name__ == "__main__":
    oa_embeddings = normalize_embedding_cols(
        get_openalex_ai_genomics_works_embeddings()
    )
    pat_embeddings = normalize_embedding_cols(
        get_patent_ai_genomics_abstract_embeddings()
    )

    oa_macro_entities = get_openalex_ai_genomics_works_entity_groups(K_MACRO_ENTITIES)
    pat_macro_entities = get_patent_ai_genomics_entity_groups(K_MACRO_ENTITIES)

    oa_works = pd.read_parquet(
        PROJECT_DIR / "outputs/openalex/parquet_files/openalex_works_validated.parquet"
    )
    patents = get_ai_genomics_patents()

    oa_works = subset_oa_recent_in_scope(
        oa_works,
        MIN_YEAR,
        MAX_YEAR,
        LANG,
        ["ai", "genomics_in_scope_x"],
    )
    oa_ids = oa_works["work_id"].values

    patents = subset_pat_recent_in_scope(
        patents,
        MIN_YEAR,
        MAX_YEAR,
        LANG,
        ["in_scope"],
    )
    pat_ids = patents["publication_number"].values

    all_embeddings = pd.concat(
        [
            oa_works.loc[oa_ids],
            patents.loc[pat_ids],
        ]
    )

    km = KMeans(n_clusters=20, random_state=42)
    km.fit(all_embeddings)
    cluster_labels = km.labels_

    cluster_lookup = make_cluster_to_id_lookup(
        all_embeddings.index.values,
        cluster_labels,
    )

    with open(
        PROJECT_DIR / "outputs/data/cluster/openalex_patents_clusters.json", "w"
    ) as f:
        json.dump(cluster_lookup, f)

    # reduction = Pipeline(
    #     [
    #         ("svd", TruncatedSVD(n_components=50)),
    #         ("umap", UMAP(n_components=2)),
    #     ]
    # )
