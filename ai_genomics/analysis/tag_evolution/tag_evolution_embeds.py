"""This script clusters AI genomics entity embeddings using k-means at successive 
timestamps and propagates the cluster labels across timestamps.

It also saves out reduced entity embeddings and the propagated cluster labels to s3. 
"""
import sys

sys.path.append("/Users/india.kerlenesta/Projects/ai_genomics")

from ai_genomics.getters.data_getters import load_s3_data
import pandas as pd
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
from ai_genomics.getters.openalex import get_openalex_ai_genomics_works
from ai_genomics.utils.bert_vectorizer import BertVectorizer
from ai_genomics.utils.network_time_series import jaccard_similarity

# instantiate models
bert_model = BertVectorizer(multi_process=False).fit()
reducer = umap.UMAP()

# load entities
patent_ents, crunchbase_ents, gtr_ents, oa_ents = (
    get_ai_genomics_patents_entities(),
    get_crunchbase_entities(),
    get_gtr_entities(),
    # temporarily as I wait for another PR to be merged
    load_s3_data(
        bucket_name, "outputs/entity_extraction/oa_ai_genomics_lookup_clean.json"
    ),
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
            ents_subset = {
                id_: [i[0] for i in ents.get(id_)] for id_ in df_ids if ents.get(id_)
            }
            all_ents.extend(list(itertools.chain(*ents_subset.values())))
        ents_per_date[min_date.year] = list(set(all_ents))

    return ents_per_date


def generate_entities_embedding_lookup(entities: List[str]) -> Dict[str, np.array]:
    """Generates an embedding lookup where the key is the entity
    and the value is the reduced embedding.
    """
    embeds = bert_model.transform(entities)
    embeds_reduced = reducer.fit_transform(embeds)
    ent_reduced_embeds_dict = dict(zip(entities, embeds_reduced))

    return ent_reduced_embeds_dict


# if this is the method we want to end up using, manually define k based on graphs
# rather than max
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


def propagate_labels(
    timeslice_x, timeslice_y, min_jaccard_score: float = 0.7,
) -> Dict[int, List[str]]:
    """Propogates cluster labels from t-1 to t based on minimum jaccard similarity
    between entity lists.    
    """
    cluster_perms = list(itertools.product(timeslice_x, timeslice_y))

    perm_dists = []
    for cluster_x, cluster_y in cluster_perms:
        timeslice_x_ents, timeslice_y_ents = (
            timeslice_x.get(cluster_x),
            timeslice_y.get(cluster_y),
        )
        dists = jaccard_similarity(timeslice_x_ents, timeslice_y_ents)
        if (dists != 0) & (dists > min_jaccard_score):
            perm_dists.append((cluster_x, cluster_y, dists))

    sorted_perm_dists = sorted(perm_dists, key=lambda x: (x[0], x[2]), reverse=True)
    sorted_perm_dists_clusts = list(
        dict([(i[0], i[1]) for i in sorted_perm_dists]).items()
    )

    while len(sorted_perm_dists_clusts) > 0:
        most_similar_clusts = sorted_perm_dists_clusts[0]
        # update timeslice y
        timeslice_y[sorted_perm_dists_clusts[0][0]] = timeslice_y.get(
            sorted_perm_dists_clusts[0][1]
        )
        sorted_perm_dists_clusts.remove(most_similar_clusts)


if __name__ == "__main__":
    logger.info(
        "loading AI genomics DBpedia entities and datasets across all data sources...."
    )
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

    oa = get_openalex_ai_genomics_works()
    oa_filtered = filter_data(
        data=oa,
        query="ai_genomics == True",
        date_col="publication_date",
        id_col="work_id",
    )
    logger.info("loaded and filtered oa data")

    # timeslice entities
    ents_per_date = timestamp_entities(
        list_of_dfs=[patents_filtered, crunchbase_filtered, gtr_filtered],
        list_of_ents=[patent_ents, crunchbase_ents, gtr_ents],
    )
    logger.info("timesliced entities on a yearly basis from 2015 onwards.")

    # embed and reduce ents and generate lookup
    all_ents = list(set(list(itertools.chain(*list(ents_per_date.values())))))
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

    #### propogate cluster names across timeslices
    years = list(ents_per_date_clusts.keys())
    for year in range(len(years) - 1):
        timeslice_x = ents_per_date_clusts[years[year]]
        timeslice_y = ents_per_date_clusts[years[year + 1]]
        propagate_labels(timeslice_x, timeslice_y)

    logger.info("propagated labels across timeslices.")

    # save dbpedia entity look up and timesliced clusters from 2015 onwards
    save_to_s3(
        bucket_name,
        ent_embeds_lookup,
        "outputs/analysis/tag_evolution/dbpedia_reduced_embeds.json",
    )
    # change keys to string to be able to save dict
    save_to_s3(
        bucket_name,
        {
            year: {str(k): v for k, v in ent_info.items()}
            for year, ent_info in ents_per_date_clusts.items()
        },
        "outputs/analysis/tag_evolution/dbpedia_clusters_timeslice_embed.json",
    )
    logger.info("saved dbpedia entities and propagated entity clusters.")
