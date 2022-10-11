"""This script clusters AI genomics entity embeddings using k-means at successive 
timestamps and propagates the cluster labels across timestamps.

It also saves out reduced entity embeddings and the propagated cluster labels to s3. 
"""
from typing import Dict, List
import itertools
import numpy as np
import pandas as pd
import umap
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import ast

from ai_genomics import bucket_name, logger
from ai_genomics.getters.data_getters import load_s3_data, save_to_s3
from ai_genomics.getters.patents import (
    get_ai_genomics_patents,
    get_ai_genomics_patents_entities,
)
from ai_genomics.getters.crunchbase import get_crunchbase_entities
from ai_genomics.getters.gtr import get_gtr_entities
from ai_genomics.utils.bert_vectorizer import BertVectorizer
from ai_genomics.utils.network_time_series import jaccard_similarity

# instantiate models
bert_model = BertVectorizer(multi_process=False).fit()
reducer = umap.UMAP()

# load entities
patent_ents, crunchbase_ents, gtr_ents = (
    get_ai_genomics_patents_entities(),
    get_crunchbase_entities(),
    get_gtr_entities(),
)

def filter_data(
    data: pd.DataFrame, query: str, date_col: str, id_col: str
) -> pd.DataFrame:
    """helper function to filter and rename columns across datasets"""
    data = (
        data.query(query)
        .reset_index(drop=True)
        .rename(columns={date_col: "date", id_col: "id"})
    )
    data["date"] = pd.to_datetime(data["date"]).dt.date
    return data


def timestamp_entities(
    list_of_dfs: List[pd.DataFrame],
    list_of_ents: List[dict],
    start_date: str = "2015-01-01",
) -> Dict[int, List[str]]:
    """Generates a dictionary where the key is the year and the value
    is a list of entities extracted up to and including that year.  
    """
    periods = 2022 - ast.literal_eval(start_date.split("-")[0])
    date_range = pd.date_range(start=start_date, periods=periods, freq="A")

    ents_per_date = dict()
    for min_date in date_range:
        all_ents = []
        for df, ents in zip(list_of_dfs, list_of_ents):
            df_ids = list(df[df.date < min_date].id)
            ents_subset = {k: [i[0] for i in v] for k, v in ents.items() if k in df_ids}
            all_ents.extend(list(set(list(itertools.chain(*ents_subset.values())))))
        ents_per_date[min_date.year] = all_ents

    return ents_per_date


def generate_entities_embedding_lookup(entities: List[str]) -> Dict[str, np.array]:
    """Generates an embedding lookup where the key is the entity
    and the value is the reduced embedding.
    """
    embeds = bert_model.transform(entities)
    embeds_reduced = reducer.fit_transform(embeds)
    ent_reduced_embeds_dict = dict(zip(entities, embeds_reduced))

    return ent_reduced_embeds_dict


def get_best_k(
    reduced_entities: Dict[str, np.array], ents_per_date: Dict[str, List[str]]
) -> List[int]:
    """Identifies the optimal number of clusters per timeslice based
    on the maximum mean silouette score for k between 10-70 at 5 intervals.

    Returns list of optimal ks per timeslice.
    """
    best_ks = []
    for ents in ents_per_date.values():
        results = []
        ent_embeds = [reduced_entities.get(ent) for ent in ents]
        for n in range(10, 70, 5):
            km = KMeans(n_clusters=n)
            clust = km.fit_predict(ent_embeds)
            score = silhouette_score(ent_embeds, clust)
            results.append([n, score])
        max_score = max([i[1] for i in results])
        best_ks.extend([i[0] for i in results if i[1] == max_score])

    return best_ks


def update_labels(
    new_labels_dict: Dict[int, int], timeslice_dict: Dict[int, List[str]]
) -> Dict[int, List[str]]:
    """Helper function to update cluster labels based on new labels dict."""
    new_labels = []
    for clust in list(timeslice_dict.keys()):
        new_clust = new_labels_dict.get(str(clust))
        if new_clust:
            new_labels.append(str(new_clust))
        else:
            new_labels.append(str(clust))

    return dict(zip(new_labels, timeslice_dict.values()))


def propagate_labels(
    timeslice_x: Dict[int, List[str]],
    timeslice_y: Dict[int, List[str]],
    min_jaccard_score: float = 0.5,
) -> Dict[int, List[str]]:
    """Propogates cluster labels from t-1 to t based on minimum jaccard similarity
    between entity lists.
    
    Returns updated cluster labels at time t
    """
    perm_dists = [
        [
            (clust_x, clust_y, jaccard_similarity(ent_x, ent_y))
            for clust_y, ent_y in timeslice_y.items()
        ]
        for clust_x, ent_x in timeslice_x.items()
    ]
    perm_dists = [[x for x in perm_dist if x[2] != 0] for perm_dist in perm_dists]

    perm_dists_df = pd.DataFrame(
        itertools.chain(*perm_dists),
        columns=["timeslice_x_cluster", "timeslice_y_cluster", "jaccard_score"],
    )
    new_y_clusters = dict()
    for timeslice, timeslice_info in perm_dists_df.groupby("timeslice_x_cluster"):
        max_score = max(timeslice_info["jaccard_score"])
        y_cluster = timeslice_info[
            timeslice_info.jaccard_score == max_score
        ].timeslice_y_cluster.values[0]
        # jaccard similarity score needs to be at least 0.5 to merge clusters
        if max_score > min_jaccard_score:
            new_y_clusters[str(y_cluster)] = timeslice
            logger.info(
                f"replace {y_cluster} with {timeslice} at t + 1 based on jaccard similarity."
            )

    # update timeslice_y labels
    return update_labels(new_y_clusters, timeslice_y)


if __name__ == "__main__":
    logger.info(
        "loading AI genomics DBpedia entities and datasets across all data sources...."
    )
    patents = get_ai_genomics_patents()
    patents_filtered = filter_data(
        data=patents,
        query="~grant_date.isna()",
        date_col="grant_date",
        id_col="publication_number",
    )
    logger.info("loaded and filtered patents data")

    crunchbase = load_s3_data(
        bucket_name, "outputs/crunchbase/crunchbase_ai_genom_comps.csv"
    )
    crunchbase_filtered = filter_data(
        data=crunchbase, query="ai_genom == True", date_col="founded_on", id_col="id"
    )
    logger.info("loaded and filtered crunchbase data")

    gtr = load_s3_data(bucket_name, "outputs/gtr/gtr_ai_genomics_projects.csv")
    gtr_filtered = filter_data(
        data=gtr, query="ai_genomics == True", date_col="start", id_col="id"
    )
    logger.info("loaded and filtered gtr data")

    # timeslice entities
    ents_per_date = timestamp_entities(
        list_of_dfs=[patents_filtered, crunchbase_filtered, gtr_filtered],
        list_of_ents=[patent_ents, crunchbase_ents, gtr_ents],
    )
    logger.info("timesliced entities on a yearly basis from 2015 onwards.")

    # embed and reduce ents and generate lookup
    all_ents = list(set(itertools.chain(*list(ents_per_date.values()))))
    ent_embeds_lookup = generate_entities_embedding_lookup(entities=all_ents)
    logger.info("generated reduced entity embedding lookup.")

    # identify optimal k at every timeslice
    best_ks = get_best_k(
        reduced_entities=ent_embeds_lookup, ents_per_date=ents_per_date
    )
    logger.info("got best k per timesliced entities.")

    # cluster entities with optimal k at each timeslice
    ents_per_date_clusts = dict()
    for best_k, (year, ents) in zip(best_ks, ents_per_date.items()):
        ent_embeds = [ent_embeds_lookup.get(ent) for ent in ents]
        km = KMeans(n_clusters=best_k).fit(ent_embeds)
        clust = km.predict(ent_embeds)
        clust_dict = {c: [] for c in clust}
        for i, c in enumerate(clust):
            clust_dict[c].append(ents[i])
        ents_per_date_clusts[year] = clust_dict
    logger.info("clustered entities at each timestamp using best ks.")

    # propogate cluster names across timeslices
    new_timeslice_y = propagate_labels(
        ents_per_date_clusts[2015], ents_per_date_clusts[2016]
    )
    propagated_labels = dict()
    for _ in range(5):
        timeslice_x = list(ents_per_date_clusts.keys())[_ + 1]
        timeslice_y = list(ents_per_date_clusts.keys())[_ + 2]
        new_timeslice_y = propagate_labels(
            new_timeslice_y, ents_per_date_clusts[timeslice_y]
        )
        propagated_labels[timeslice_y] = new_timeslice_y
    logger.info("propagated labels across timeslices.")

    # save dbpedia entity look up and timesliced clusters from 2015 onwards
    save_to_s3(
        bucket_name,
        ent_embeds_lookup,
        "outputs/analysis/tag_evolution/dbpedia_reduced_embeds.json",
    )
    save_to_s3(
        bucket_name,
        propagated_labels,
        "outputs/analysis/tag_evolution/dbpedia_clusters_timeslice.json",
    )
    logger.info("saved dbpedia entities and propagated entity clusters.")
