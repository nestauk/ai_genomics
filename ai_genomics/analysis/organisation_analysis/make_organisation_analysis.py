"""
Script to conduct additional analysis on company and
    institution representitiveness in OpenAlex AI in Genomics
    document clusters.

python ai_genomics/analysis/organisation_analysis/make_organisation_analysis.py
"""
from ai_genomics.getters.data_getters import save_to_s3
from ai_genomics.getters.clusters import get_doc_cluster_interp, get_id_cluster_lookup
from ai_genomics.getters.openalex import instit_metadata


from ai_genomics import logger, bucket_name
from ai_genomics.analysis.influence.make_influence_analysis import (
    get_openalex_institutes_temp,
)
from ai_genomics.utils.save_plotting import AltairSaver

import pandas as pd
import numpy as np
import altair as alt

if __name__ == "__main__":

    saver = AltairSaver()

    logger.info("loading relevant data...")

    # load updated openalex cluster number to name look up
    cluster_num_name_dict = get_doc_cluster_interp()
    # get rid of nas...
    cluster_num_name_dict = {
        k: v for k, v in cluster_num_name_dict.items() if v is not np.nan
    }

    # load information about OpenAlex institutions incl. inst id and type
    inst_metadata = instit_metadata()
    # load AI in Genomics papers incl. their id and their associated institute info
    oa_insts = get_openalex_institutes_temp().query("ai_genomics == True")

    # load look up between OA work id and assigned cluster
    oa_cluster_names = get_id_cluster_lookup()

    logger.info("wrangling relevant data...")
    # wrangle data to add cluster names, a company flag etc.
    oa_insts_metadata = (
        pd.merge(oa_insts, inst_metadata, left_on="inst_id", right_on="id")
        .rename(columns={"id_x": "id"})
        .drop(["id_y"], axis=1)
        .assign(cluster_name=lambda df: df.id.map(oa_cluster_names))
        .dropna()
        .assign(
            company_flag=lambda df: df.type.map(
                lambda x: True if x == "company" else False
            )
        )
        .drop_duplicates(subset=["id", "display_name"])
    )  # drop duplicates to only include unique institution names per id

    logger.info("analysis of private participation per cluster...")

    overall_private_df = (
        pd.DataFrame(
            oa_insts_metadata.groupby("cluster_name").company_flag.value_counts(
                normalize=True
            )
            * 100
        )
        .rename(columns={"company_flag": "overall_private_percent"})
        .reset_index()
        .query("company_flag == True")
    )

    min_priv_participation = overall_private_df.loc[
        overall_private_df.overall_private_percent.idxmin()
    ]
    max_priv_participation = overall_private_df.loc[
        overall_private_df.overall_private_percent.idxmax()
    ]

    print(
        f"the cluster with the least private participation is {min_priv_participation.cluster_name} at {min_priv_participation.overall_private_percent}"
    )
    print(
        f"the cluster with the most private participation is {max_priv_participation.cluster_name} at {max_priv_participation.overall_private_percent}"
    )

    work_comp_flag_over_time = (
        pd.DataFrame(
            oa_insts_metadata.groupby(
                ["cluster_name", "year"]
            ).company_flag.value_counts(normalize=True)
            * 100
        )
        .rename(columns={"company_flag": "private_participation_percent"})
        .reset_index()
        .query("company_flag == True")
        .drop(columns=["company_flag"])
        .assign(year=lambda df: pd.to_datetime(df.year, format="%Y"))
        .reset_index(drop=True)
    )

    priv_participation_graph = (
        alt.Chart(work_comp_flag_over_time)
        .mark_line()
        .encode(
            x=alt.X("year", title="Year"),
            y=alt.Y(
                "private_participation_percent",
                title="Private Participation Percent (%)",
            ),
            color=alt.Color("cluster_name", legend=None),
        )
        .facet(facet=alt.Facet("cluster_name", title="cluster"), columns=4)
    )

    saver.save(priv_participation_graph, "private_participation_chart")

    logger.info("generating stats for top companies per doc cluster...")

    top_comps_per_cluster = (
        oa_insts_metadata.query("company_flag == True")
        .groupby("cluster_name")
        .display_name.value_counts()
    )

    n = 5
    for cluster_name in cluster_num_name_dict.values():
        print(
            f"the top {n} companies based on paper count in {cluster_name} are: {list(top_comps_per_cluster[cluster_name][:n].index)}"
        )
        print("---")

    logger.info("looking at top instition per cluster...")

    insts_type_cluster_df = (
        pd.DataFrame(
            oa_insts_metadata.query('type != "archive" & type != "other"')
            .groupby(["cluster_name", "display_name"])
            .display_name.count()
        )
        .rename(columns={"display_name": "count"})
        .reset_index()
    )

    top_insts_type_cluster_df = (
        insts_type_cluster_df.groupby(["cluster_name"])
        .apply(lambda x: x.nlargest(5, "count"))
        .rename(columns={"cluster_name": "top_cluster_name"})
        .reset_index()
        .drop(columns=["top_cluster_name", "level_1"])
    )

    # save df to write up company names
    save_to_s3(bucket_name, top_insts_type_cluster_df, "outputs/data/top_insts.csv")
