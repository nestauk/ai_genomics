"""This script clusters AI genomics entity embeddings using k-means at successive 
timestamps and propagates the cluster labels across timestamps.

It saves out the propagated cluster labels and reduced entity embeddings to s3. 

python ai_genomics/pipeline/entity_cluster/create_entity_clusters_over_time.py
"""
import pandas as pd
from typing import Dict, List
import itertools
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import ast

from ai_genomics import bucket_name, logger, get_yaml_config, PROJECT_DIR
from ai_genomics.getters.data_getters import load_s3_data, save_to_s3
from ai_genomics.getters.patents import (
    get_ai_genomics_patents,
    get_ai_genomics_patents_entities,
)

from ai_genomics.getters.crunchbase import get_crunchbase_entities
from ai_genomics.getters.gtr import get_gtr_entities
from ai_genomics.getters.openalex import (
    get_openalex_ai_genomics_works,
    get_openalex_ai_genomics_entities,
)
from ai_genomics.utils.text import jaccard_similarity
from ai_genomics.utils.entities import generate_embed_lookup
from ai_genomics.utils.filtering import filter_data

CONFIG = get_yaml_config(PROJECT_DIR / "ai_genomics/config/entity_cluster.yaml")


def timestamp_entities(
    list_of_dfs: List[pd.DataFrame],
    list_of_ents: List[dict],
    start_date: str = "2010-01-01",
) -> Dict[int, List[str]]:
    """Generates a dictionary where the key is the year and the value
    is a list of entities extracted up to and including that year. 
    
    Args:
        list_of_dfs (List[pd.DataFrame]): List of filtered dataframes
            across patents, crunchbase, openalex and gtr
        list_of_ents (List[dict]): List of entity dictionaries across
            patents, crunchbase, openalex and gtr
        start_date (str): A YYYY-MM-DD string indicating the start date 
            to timeslice from. 
    
    Returns:
        A dictionary where the key is the year and the value 
        is a list of entities across datasets that appeared 
        up to that year. 
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


def get_best_k(
    reduced_entities: Dict[str, np.array], ents_per_date: Dict[str, List[str]]
) -> List[int]:
    """Identifies the optimal number of clusters per timeslice 
        based on the maximum mean silouette score for k 
        between 10-70 at 5 intervals.
    
    Args:
        reduced_entities (Dict[str, np.array]): A dictionary where the 
            key is the entity and the value is an embedding. 
        ents_per_date (List[dict]): A dictionary where the key is the 
            year and the value is a list of entities across datasets 
            that appeared up to that year.

    Returns:            
        list of optimal ks per timeslice.
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
    timeslice_x: Dict[str, List[str]],
    timeslice_y: Dict[str, List[str]],
    min_jaccard_score: float = 0.5,
) -> Dict[int, List[str]]:
    """Propogates cluster labels from t-1 to t based on maximum 
        jaccard similarity above a threshold between entity lists.    
    
    Args:
    timeslice_x (Dict[str, List[str]]): A dictionary where the 
        key is the cluster number and the value is a list of 
        entities belonging to a given cluster number. 
    timeslice_y (Dict[str, List[str]]): A dictionary where the 
        key is the cluster number and the value is a list of 
        entities belonging to a given cluster number. 
    min_jaccard_score (float): The minimum jaccard score required
        to propagate cluster labels from t-1 to t. 
    
    timeslice_y cluster numbers are changed in place based on similarty
        score thresholds. 
    """
    # generate cartesian product of cluster lists at t-1 and t
    cluster_perms = list(itertools.product(timeslice_x, timeslice_y))

    # calculate the jaccard simiarity between entity lists at t-1 and t
    perm_dists = []
    for cluster_x, cluster_y in cluster_perms:
        # get entity lists at each timeslice oer cluster number
        timeslice_x_ents, timeslice_y_ents = (
            timeslice_x.get(cluster_x),
            timeslice_y.get(cluster_y),
        )
        dists = jaccard_similarity(timeslice_x_ents, timeslice_y_ents)
        # if the similarity is not 0 AND above the minimum threshold
        if (dists != 0) & (dists > min_jaccard_score):
            perm_dists.append((cluster_x, cluster_y, dists))
    # sort the list of cluster numbers at t-1, cluster numbers at t and their
    # jaccard similarty scores
    sorted_perm_dists = sorted(perm_dists, key=lambda x: (x[0], x[2]), reverse=True)
    # remove clusters that are close the multiple clusters by picking the
    # combination with the highest jaccard similarty score
    sorted_perm_dists_clusts = list(
        dict([(i[0], i[1]) for i in sorted_perm_dists]).items()
    )

    # recursively update cluster labels at t with cluster labels
    # from t-1 based on the sorted list
    while len(sorted_perm_dists_clusts) > 0:
        most_similar_clusts = sorted_perm_dists_clusts[0]
        timeslice_y[sorted_perm_dists_clusts[0][0]] = timeslice_y.get(
            sorted_perm_dists_clusts[0][1]
        )
        # and pop key with old name!
        timeslice_y.pop(sorted_perm_dists_clusts[0][1], None)
        sorted_perm_dists_clusts.remove(most_similar_clusts)


if __name__ == "__main__":
    logger.info(
        "loading AI genomics DBpedia entities and datasets across all data sources...."
    )
    # load entities
    patent_ents, crunchbase_ents, gtr_ents, oa_ents = (
        get_ai_genomics_patents_entities(),
        get_crunchbase_entities(),
        get_gtr_entities(),
        get_openalex_ai_genomics_entities(),
    )
    logger.info("loaded AI genomics DBpedia entities")

    patents = (get_ai_genomics_patents()
           .query('in_scope == True'))
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

    oa = (load_s3_data(bucket_name, 'outputs/openalex/ai_genomics_openalex_works.csv')
     .query('genomics_in_scope == True'))
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
    logger.info("timesliced entities on a yearly basis from 2010 onwards.")

    # embed and reduce ents and generate lookup
    all_ents = list(set(list(itertools.chain(*list(ents_per_date.values())))))
    ent_embeds_lookup = generate_embed_lookup(
        entities=all_ents, model=CONFIG["embed"]["model"], reduce_embedding=True
    )
    # save ent_embeds_lookup that is json serializable
    save_to_s3(
        bucket_name,
        {k: v.tolist() for k, v in ent_embeds_lookup.items()},
        "outputs/analysis/tag_evolution/dbpedia_tags_reduced_embed.json",
    )

    logger.info("generated and saved reduced entity embedding lookup.")

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
        clust_dict = {f"{c}_{year}": [] for c in clust}
        for i, c in enumerate(clust):
            clust_dict[f"{c}_{year}"].append(ents[i])
        ents_per_date_clusts[year] = clust_dict
    logger.info("clustered entities at each timestamp using best ks.")

    #### propogate cluster names across timeslices
    years = list(ents_per_date_clusts.keys())
    for year in range(len(years) - 1):
        timeslice_x = ents_per_date_clusts[years[year]]
        timeslice_y = ents_per_date_clusts[years[year + 1]]
        propagate_labels(timeslice_x, timeslice_y)
    logger.info("propagated labels across timeslices.")

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
