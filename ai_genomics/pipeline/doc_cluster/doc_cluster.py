import click
from collections import defaultdict
import json
from numpy.typing import NDArray
import pandas as pd
from sklearn.cluster import KMeans
from typing import Dict, Sequence, List

from ai_genomics import PROJECT_DIR, logger, bucket_name
from ai_genomics.utils import id_to_source
from ai_genomics.getters.data_getters import save_to_s3
from ai_genomics.utils.gtr import parse_project_dates
from ai_genomics.getters.openalex import (
    get_openalex_ai_genomics_works_embeddings,
    get_openalex_ai_genomics_works_entity_groups,
)
from ai_genomics.getters.patents import (
    get_ai_genomics_patents,
    get_patent_ai_genomics_abstract_embeddings,
    get_patent_ai_genomics_entity_groups,
)
from ai_genomics.getters.gtr import (
    get_ai_genomics_project_table,
    get_gtr_ai_genomics_project_embeddings,
    get_gtr_ai_genomics_project_entity_groups,
)


LANG = "en"
K_MACRO_ENTITIES = 100
AI_MACRO_ENTITY_COLS = ["87", "88", "98"]
N_CLUSTERS = 20
RANDOM_STATE = 42


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


def subset_gtr_recent_in_scope(
    projects: pd.DataFrame,
    min_year: int,
    max_year: int,
    scope_cols: Sequence[str],
) -> pd.DataFrame:
    in_scope = " and ".join(scope_cols)
    return (
        projects.assign(start_year=pd.to_datetime(projects["start"]).dt.year)
        .query("start_year >= @min_year and start_year <= @max_year")
        .query(in_scope)
    )


def ai_macro_entity_ids(
    macro_entities: pd.DataFrame,
    ai_cols: List[str],
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


def normalize_embedding_cols(embeddings: pd.DataFrame) -> pd.DataFrame:
    """Resolves issue with embedding column names."""
    embeddings = embeddings.set_index("Unnamed: 0")
    embeddings.index.name = "id"
    embeddings = embeddings.rename(columns={c: int(c) for c in embeddings.columns})
    return embeddings


def make_cluster_to_id_lookup(
    ids: Sequence[str],
    cluster_labels: Sequence[int],
) -> Dict:
    """Creates a cluster to ID lookup, separated by data source"""
    sources = list(map(id_to_source, ids))
    doc_clusters = {k: defaultdict(list) for k in set(sources)}

    for idx, source, cluster in zip(ids, sources, cluster_labels):
        doc_clusters[source][cluster].append(idx)

    return doc_clusters


def ai_macro_entity_ids(macro_entities: pd.DataFrame, ai_cols: List[int]) -> NDArray:
    return (
        macro_entities.set_index("id")[ai_cols]
        .sum(axis=1)
        .pipe(lambda x: x[x > 0])
        .index.values
    )


@click.command()
@click.option(
    "--ai",
    is_flag=True,
    show_default=True,
    default=False,
    help="Whether to run only on AI subset of documents.",
)
@click.option(
    "--min-year",
    show_default=True,
    default=2020,
    help="Earliest year to use.",
    type=int,
)
@click.option(
    "--max-year",
    show_default=True,
    default=2021,
    help="Latest year to use.",
    type=int,
)
def run(ai, min_year, max_year):
    logger.info("Fetching embeddings")
    oa_embeddings = normalize_embedding_cols(
        get_openalex_ai_genomics_works_embeddings()
    )
    pat_embeddings = normalize_embedding_cols(
        get_patent_ai_genomics_abstract_embeddings()
    )
    gtr_embeddings = (
        get_gtr_ai_genomics_project_embeddings()
        .rename(columns={"project_id": "id"})
        .set_index("id")
    )
    gtr_embeddings = gtr_embeddings.rename(
        columns={c: int(c) for c in gtr_embeddings.columns}
    )

    logger.info("Fetching documents")
    oa_works = pd.read_parquet(
        PROJECT_DIR / "outputs/openalex/parquet_files/openalex_works_validated.parquet"
    )
    patents = get_ai_genomics_patents()

    logger.info("Subsetting data")
    oa_works = subset_oa_recent_in_scope(
        oa_works,
        min_year,
        max_year,
        LANG,
        ["ai", "genomics_in_scope_x"],
    )
    oa_ids = oa_works["work_id"].values

    patents = subset_pat_recent_in_scope(
        patents,
        min_year,
        max_year,
        LANG,
        ["in_scope"],
    )
    pat_ids = patents["publication_number"].values

    gtr_projects = parse_project_dates(get_ai_genomics_project_table())
    gtr_projects = subset_gtr_recent_in_scope(
        gtr_projects,
        min_year,
        max_year,
        ["ai_genomics"],
    )
    gtr_ids = gtr_projects["id"].values

    if ai:
        logger.info("Fetching macro entities")
        oa_macro_entities = get_openalex_ai_genomics_works_entity_groups(
            K_MACRO_ENTITIES
        )
        oa_ai_ids = ai_macro_entity_ids(oa_macro_entities, AI_MACRO_ENTITY_COLS)
        oa_ids = list(set(oa_ids).intersection(set(oa_ai_ids)))

        pat_macro_entities = get_patent_ai_genomics_entity_groups(K_MACRO_ENTITIES)
        pat_ai_ids = ai_macro_entity_ids(pat_macro_entities, AI_MACRO_ENTITY_COLS)
        pat_ids = list(set(pat_ids).intersection(set(pat_ai_ids)))

        gtr_macro_entities = get_gtr_ai_genomics_project_entity_groups(K_MACRO_ENTITIES)
        gtr_ai_ids = ai_macro_entity_ids(gtr_macro_entities, AI_MACRO_ENTITY_COLS)
        gtr_ids = list(set(gtr_ids).intersection(set(gtr_ai_ids)))

    embeddings = pd.concat(
        [
            oa_embeddings.loc[oa_ids],
            pat_embeddings.loc[pat_ids],
            gtr_embeddings.loc[gtr_ids],
        ]
    )

    logger.info(f"Clustering embeddings with shape {embeddings.shape}")
    km = KMeans(n_clusters=N_CLUSTERS, random_state=RANDOM_STATE)
    km.fit(embeddings)
    cluster_labels = [int(l) for l in km.labels_]

    cluster_lookup = make_cluster_to_id_lookup(
        embeddings.index.values,
        cluster_labels,
    )

    subset = "ai" if ai else "all"
    save_to_s3(
        bucket_name,
        cluster_lookup,
        f"outputs/data/cluster/doc_{subset}_{min_year}_{max_year}_clusters.json",
    )

    # logger.info("Making a chart")
    # reduction = Pipeline(
    #     [
    #         ("svd", TruncatedSVD(n_components=50)),
    #         ("umap", UMAP(n_components=2)),
    #     ]
    # )

    # embeddings_reduced = reduction.fit_transform(embeddings)

    # umap_chart_data = pd.DataFrame(
    #     {
    #         "UMAP 0": embeddings_reduced[:, 0],
    #         "UMAP 1": embeddings_reduced[:, 1],
    #         "Cluster": cluster_labels,
    #     }
    # )
    # umap_chart_data = umap_chart_data.sample(5_000)

    # umap_chart = (
    #     alt
    #     .Chart(umap_chart_data)
    #     .mark_circle()
    #     .encode(
    #         x="UMAP: 0",
    #         y="UMAP: 1",
    #         color="Cluster"
    #     )
    # )

    # saver = AltairSaver(
    #     PROJECT_DIR / "outputs/figures/png/",
    #     ["png"],
    # )

    # saver.save(
    #     configure_plots(umap_chart),
    #     f"doc_clusters_{subset}_umap_scatter",
    #     )


if __name__ == "__main__":
    run()
