"""This script clusters an AI genomics DBepdia coocurrance network at successive 
timestamps and propagates the cluster labels across timestamps.
Used PEC-style approach
It also saves out the propagated cluster labels to s3. 
"""
from collections import Counter
import networkx as nx
import itertools
import pandas as pd

from ai_genomics import bucket_name, logger
from ai_genomics.getters.data_getters import load_s3_data, save_to_s3
from ai_genomics.getters.patents import (
    get_ai_genomics_patents,
    get_ai_genomics_patents_entities,
)
from ai_genomics.getters.crunchbase import get_crunchbase_entities
from ai_genomics.getters.gtr import get_gtr_entities
from ai_genomics.getters.openalex import get_openalex_ai_genomics_works

# move this to a utils eventually
from ai_genomics.analysis.tag_evolution.tag_evolution_embeds import filter_data
import ai_genomics.utils.network_time_series as nts

min_weight = 10


def clean_entities(ents):
    """helper function remove entity scores from entity lists"""
    return {k: [i[0] for i in v] for k, v in ents.items()}


if __name__ == "__main__":
    logger.info(
        "loading AI genomics DBpedia entities and datasets across all data sources...."
    )
    logger.info("loading and cleaning entity lookups...")
    patent_ents, crunchbase_ents, gtr_ents, oa_ents = (
        clean_entities(get_ai_genomics_patents_entities()),
        clean_entities(get_crunchbase_entities()),
        clean_entities(get_gtr_entities()),
        # temporarily as I wait for another PR to be merged
        clean_entities(
            load_s3_data(
                bucket_name,
                "outputs/entity_extraction/oa_ai_genomics_lookup_clean.json",
            )
        ),
    )

    logger.info("loading dfs...")
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

    # prepare data
    all_ents = dict(
        pair
        for d in [patent_ents, crunchbase_ents, gtr_ents, oa_ents]
        for pair in d.items()
    )
    all_dfs = pd.concat(
        [
            patents_filtered[["id", "date"]],
            crunchbase_filtered[["id", "date"]],
            gtr_filtered[["id", "date"]],
        ],
        axis=0,
    ).dropna()
    logger.info("concatenated entities across data sources")

    expanded_ents = itertools.chain(
        *[tuple(itertools.combinations(d, 2)) for d in list(all_ents.values())]
    )
    # Sort and count the combinations so that A,B and B,A are treated the same
    weighted_expanded_ents = Counter([tuple(sorted(d)) for d in expanded_ents])

    # remove pairs with edgeweight less than 10
    weighted_expanded_ents = Counter(
        ent_pair
        for ent_pair in weighted_expanded_ents.elements()
        if weighted_expanded_ents[ent_pair] > min_weight
    )

    ent_pairs = {ent_pair: [] for ent_pair in weighted_expanded_ents.keys()}
    for ent_pair, weight in weighted_expanded_ents.items():
        for k, v in all_ents.items():
            if set([ent_pair[0], ent_pair[1]]).issubset(set(v)):
                ent_pairs[ent_pair].append(k)

    ent_pair_years = {
        ent_pair: sorted(list(all_dfs[all_dfs.id.isin(ids)].date), reverse=False)
        for ent_pair, ids in ent_pairs.items()
    }
    logger.info("extracted date entity pair first appeared across all data sources")

    # create network
    G = nx.Graph()
    for ent_pair, ent_pair_info in ent_pair_years.items():
        if ent_pair_info != []:
            G.add_edge(
                ent_pair[0],
                ent_pair[1],
                first_introduced=[int(x.year) for x in ent_pair_info][0],
                weight=weighted_expanded_ents.get(ent_pair),
            )
    logger.info("generated graph")

    # cluster and timeslice network
    timeslices = nts.timeslice_pair_coo_graph(G, 1, 2015)
    subgraph_communities = nts.cluster_timeslice_pair_coo_graph(timeslices)
    logger.info("timesliced graph and clustered communities at each timeslice")

    # sanitise clusters
    for i in range(len(subgraph_communities) - 1):
        timeslice_x = "G_timeslice_" + str(i)
        timeslice_y = "G_timeslice_" + str(i + 1)
        nts.sanitise_clusters(
            subgraph_communities[timeslice_x],
            subgraph_communities[timeslice_y],
            min_jaccard_score=0.7,
        )
    logger.info("sanitised clusters")

    # color and name clusters
    subgraph_communities = nts.add_cluster_colors(subgraph_communities)
    subgraph_communities = nts.add_cluster_names(subgraph_communities, 3)
    logger.info("added cluster name and cluster color as node attributes")

    # save output
    save_to_s3(
        bucket_name,
        subgraph_communities,
        "outputs/analysis/tag_evolution/dbpedia_clusters_timeseries_network.pkl",
    )
