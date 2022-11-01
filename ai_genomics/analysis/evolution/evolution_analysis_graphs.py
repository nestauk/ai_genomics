"""
Script to generate graphs from evolution analysis

python ai_genomics/analysis/evolution/evolution_analysis_graphs.py
"""
from ai_genomics import bucket_name, logger
from ai_genomics.getters.data_getters import load_s3_data
from ai_genomics.utils import plotting as pu
from ai_genomics.utils.save_plotting import AltairSaver

import altair as alt
import numpy as np
import pandas as pd
import itertools
import random
from sklearn.feature_extraction.text import TfidfVectorizer
from typing import List, Dict
import statistics
from collections import Counter
import random

import importlib 
importlib.reload(pu); 

altair_saver = AltairSaver()
alt.data_transformers.disable_max_rows()

#load cluster evolution output
EVOLVED_CLUSTERS = load_s3_data(bucket_name, "outputs/analysis/tag_evolution/dbpedia_clusters_timeslice_embed.json")
#load reduced embeddings
REDUCED_ENT_EMBEDS = load_s3_data(bucket_name, "outputs/analysis/tag_evolution/dbpedia_tags_reduced_embed.json")

ALL_CLUSTERS = list(set(list(itertools.chain(*[list(i.keys()) for i in list(EVOLVED_CLUSTERS.values())])))) 
    
def get_tfidf_top_features(
    list_of_ents: List[str], 
    n_top: int = 3) -> str:
    #https://stackoverflow.com/questions/34232190/scikit-learn-tfidfvectorizer-how-to-get-top-n-terms-with-highest-tf-idf-score
    """Gets top n features using TF-ID."""
    tfidf_vectorizer = TfidfVectorizer(stop_words="english")
    tfidf = tfidf_vectorizer.fit_transform(list_of_ents)
    importance = np.argsort(np.asarray(tfidf.sum(axis=0)).ravel())[::-1]
    tfidf_feature_names = np.array(tfidf_vectorizer.get_feature_names_out())
    return '-'.join(tfidf_feature_names[importance[:n_top]])

def get_cluster_sizes(cluster: str, cluster_name_mapper: Dict[str, str], evolved_clusters: Dict[str, List[str]] = EVOLVED_CLUSTERS) -> List:
    """Helper function to get the cluster, cluster name, year and cluster size"""
    clust_size = []
    for year, clust_info in evolved_clusters.items():
        clust = clust_info.get(cluster)
        if clust:
            clust_size.append((cluster, cluster_name_mapper.get(cluster), year, len(clust)))
    return clust_size

def get_final_cluster_list(clust: str, evolved_clusters: Dict[str, List[str]] = EVOLVED_CLUSTERS):
    """Helper function go get list of entities at its latest year for a given cluster"""
    final_year = max([int(year) for year, clust_info in evolved_clusters.items() if clust_info.get(clust)])
    return final_year, evolved_clusters[str(final_year)][clust]

def make_unique_cluster_names(all_clusters: List[str] = ALL_CLUSTERS) -> Dict[str, str]:
    """Generates cluster names based on clusters entity lists
        at the latest timestamp. Insures cluster name uniqueness
        by adding numbers at end of cluster name if cluster name count is
        more than 1. 
    
    Args:
        all_clusters: List of all clusters across timestamps
    
    Returns dictionary where key is cluster label and value is 
        cluster name.
    """
    cluster_name_mapper = dict()
    for cluster in all_clusters:
        final_year, final_cluster_list = get_final_cluster_list(cluster)
        cluster_name = get_tfidf_top_features(list_of_ents=final_cluster_list)
        cluster_name_mapper[cluster] = cluster_name
    
    #make sure cluster names are unique
    cluster_name_mapper_counts = Counter(cluster_name_mapper.values()).most_common()
    unique_cluster_name_mapper = dict()
    for cluster_name, cluster_name_count in cluster_name_mapper_counts:
        if cluster_name_count > 1:
            name_mapper_indxs = [list(cluster_name_mapper.keys())[i] for i, j in enumerate(list(cluster_name_mapper.values())) if j == cluster_name]
            unique_names = [f"{cluster_name}-{str(i)}" for i in range(cluster_name_count)]
            unique_cluster_name_mapper.update((dict(zip(name_mapper_indxs, unique_names))))
        unique_cluster_name_mapper[list(cluster_name_mapper.keys())[list(cluster_name_mapper.values()).index(cluster_name)]] = cluster_name
    
    return unique_cluster_name_mapper

def make_timestamp_df(
    year: str, 
    cluster_name_mapper: Dict[str, str],
    clusters_to_drop: List[str] = [],
    reduced_ent_embeds: Dict[str, List[int]] = REDUCED_ENT_EMBEDS,
    evolved_clusters: Dict[str, List[str]] = EVOLVED_CLUSTERS) -> pd.DataFrame:
    """Create timestamped pd.DataFrame based on evolved clusters at year

    Args:
        year: Year to subset evolved clusters
        clusters_to_drop: List of cluster labels to remove from pd.DataFrame
        cluster_name_mapper: Unique cluster name mapper 
        reduced_ent_embeds: Entity embedding mapper
        evolved_clusters: Dictionary of clusters at each timestamp 

    Returns timestamped pd.DataFrame with cluster names, entities, year 
        cluster was first introduced and reduced X and Y coordinates. 
    """

    return (pd.concat({k: pd.Series(v) for k, v in evolved_clusters[year].items()})
        .reset_index()
        .rename(columns={'level_0': 'cluster', 0: 'entity'})
        .query(f'~cluster.isin({clusters_to_drop})')
        .assign(coords = lambda df: df.entity.map(reduced_ent_embeds))
        .assign(x = lambda df: df.coords.apply(lambda x: x[0]))
        .assign(y = lambda df: df.coords.apply(lambda x: x[1]))
        .assign(year_cluster_introduced = lambda df: df.cluster.apply(lambda x: x.split('_')[1]))
        .assign(cluster_name = lambda df: df.cluster.map(cluster_name_mapper))
        .drop(['level_1', 'coords'], axis=1))

def generate_entity_cluster_timestamp_plot(
    cluster_name_mapper: Dict[str, str], 
    year: str,
    clusters_to_drop: List[str] = [],
    reduced_ent_embeds: Dict[str, List[int]] = REDUCED_ENT_EMBEDS,
    evolved_clusters: Dict[str, List[str]] = EVOLVED_CLUSTERS
    ):
    """Create timestamped entity scatter plot based on timestamped pd.DataFrame.

    Args:
        year: Year to subset evolved clusters
        clusters_to_drop: List of cluster labels to remove from pd.DataFrame
        cluster_name_mapper: Unique cluster name mapper 
        reduced_ent_embeds: Entity embedding mapper
        evolved_clusters: Dictionary of clusters at each timestamp 

    """
    timestamp_df = make_timestamp_df(year, 
                                        clusters_to_drop=clusters_to_drop,
                                    cluster_name_mapper=cluster_name_mapper,
                                    reduced_ent_embeds=reduced_ent_embeds,
                                    evolved_clusters=evolved_clusters)
    return alt.Chart(timestamp_df, width=500, height=500).mark_circle(size=60).encode(
        x='x',
        y='y',
        color=alt.Color('cluster_name', title='Cluster', legend=alt.Legend(columns=3, symbolLimit=0)), 
        tooltip=['entity', 'cluster_name', 'year_cluster_introduced']
    )

if __name__ == "__main__":
    
    cluster_name_mapper = make_unique_cluster_names()
    logger.info('created unique cluster names look up.')

    #Generate 3x timestamped graph
    early_timestamp = generate_entity_cluster_timestamp_plot(year='2010', cluster_name_mapper=cluster_name_mapper)
    mid_timestamp = generate_entity_cluster_timestamp_plot(year='2015', cluster_name_mapper=cluster_name_mapper)
    final_timestamp = generate_entity_cluster_timestamp_plot(year='2021', cluster_name_mapper=cluster_name_mapper)

    across_timestamps = early_timestamp | mid_timestamp | final_timestamp
    colors = ["#%06x".upper() % random.randint(0, 0xFFFFFF) for _ in range(len(ALL_CLUSTERS))]
    across_timestamps = across_timestamps.configure_range(
        category=alt.RangeScheme(colors)
    )
    chart_title = ['Macro entities at three different timestamps']
    chart_subtitle = ['The macro entity space in 2010, 2015 and 2021']
    configured_across_timestamps = pu.configure_plots(across_timestamps, chart_title, chart_subtitle)
    altair_saver.save(configured_across_timestamps, 'entities_across_timestamps')
    logger.info('saved entities across timestamp graph')

    #Persistent cluster analysis
    cluster_ages = [[len([year for year, clust_info in EVOLVED_CLUSTERS.items() if clust_info.get(cluster)])][0] for cluster in ALL_CLUSTERS]
    logger.info(f"The average number of years a cluster persists is {statistics.mean(cluster_ages)}")
    logger.info(f"The median number of years a cluster persists is {statistics.median(cluster_ages)}")      
    logger.info(f"The maximum number of years a cluster persists is {max(cluster_ages)}")      

    most_common_persists = Counter(cluster_ages).most_common()
    logger.info([f"there were {persist[1]} clusters that lasted {persist[0]} years" for persist in most_common_persists])

    chart_title = ['# of Macro Entities Persistent Over Time']
    chart_subtitle = ['Most Macro Entities survive 1-2 years but some have persisted for up to 12 years.']
    most_common_persists_df = pd.DataFrame(most_common_persists, columns=['# of Years persisted', '# of Clusters'])
    chart = alt.Chart(most_common_persists_df).mark_bar().encode(
        x='# of Years persisted',
        y='# of Clusters'
    ).configure_mark(
        opacity=0.5,
        color=pu.NESTA_COLOURS[1]
    )
    chart_configured = pu.configure_plots(chart, chart_title, chart_subtitle)
    altair_saver.save(chart_configured, 'persistent_cluster_count')
    logger.info('saved persistent cluster count graph')

    all_cluster_sizes = []
    for cluster in ALL_CLUSTERS:
        years_persistent = [year for year, clust_info in EVOLVED_CLUSTERS.items() if clust_info.get(cluster)]
        if len(years_persistent) >= 10:
            cluster_sizes = get_cluster_sizes(cluster, cluster_name_mapper=cluster_name_mapper)
            all_cluster_sizes.extend(cluster_sizes)
    all_cluster_sizes_df = pd.DataFrame(all_cluster_sizes, columns=['Cluster', 'Cluster Name', 'Year', 'Size'])
    chart_title = ['Persistent Cluster Size over Time']
    chart_subtitle = ['The most persistent clusters have largely grown over time.']

    chart = alt.Chart(all_cluster_sizes_df).mark_line().encode(
        x='Year',
        y='Size',
        color=alt.Color('Cluster Name', legend=alt.Legend(labelLimit=0))
    )
    chart_configured = pu.configure_plots(chart, chart_title, chart_subtitle)
    altair_saver.save(chart_configured, 'persistent_cluster_over_time')
    logger.info('saved persistent cluster growth over time graph')