# Script to produce prototype thematic analysis of GtR data

import pandas as pd
import altair as alt
import logging
from umap import UMAP
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from ai_genomics.getters.data_getters import load_s3_data
from ai_genomics.getters.gtr import get_ai_genomics_project_table
from ai_genomics import PROJECT_DIR
from ai_genomics.utils.altair_save_utils import (
    google_chrome_driver_setup,
    save_altair,
    altair_text_resize,
)

OUT_PATH = f"{PROJECT_DIR}/outputs/figures"


wd = google_chrome_driver_setup()


def get_gtr_sampled():

    return load_s3_data("ai-genomics", "inputs/gtr/processed/gtr_projects_sampled.csv")


def get_gtr_vectors():

    return load_s3_data("ai-genomics", "inputs/gtr/processed/gtr_vectors.pickle")


def gtr_cluster_analysis(reproduce: bool = True):
    """Runs the gtr analysis
    Args:
        reproduce: if we are reproducing JMG's local analysis.
            If we don't reproduce the analyssi then we sample a new set
            of baseline projects and create their vector representations. This
            takes ca. 1 hr.
    """

    if reproduce:
        logging.info("Fetching processed files")

        gtr_sampled = get_gtr_sampled()
        gtr_vectors = get_gtr_vectors()

        ai_gen_ids, ai_ids, gen_ids = [
            set(gtr_sampled.query(f"{var}==True")["id"])
            for var in ["ai_genomics", "ai", "genomics"]
        ]

    else:

        logging.info("Loading GtR data")
        gtr_table = get_ai_genomics_project_table(local=False)

        ai_gen_ids, ai_ids, gen_ids = [
            set(gtr_table.query(f"{var}==True")["id"])
            for var in ["ai_genomics", "ai", "genomics"]
        ]

        from sentence_transformers import SentenceTransformer

        model = SentenceTransformer("allenai-specter")
        gtr_sampled = pd.concat(
            [
                gtr_table.query("ai_genomics==True"),
                gtr_table.query("ai==True").sample(500),
                gtr_table.query("genomics==True").sample(500),
            ]
        ).reset_index(drop=True)

        gtr_text = [
            title + "[SEP]" + abstract
            for title, abstract in zip(
                gtr_sampled["title"].values, gtr_sampled["abstract_text"].values
            )
        ]
        gtr_vectors = model.encode(gtr_text)

    logging.info("Clustering data")

    # Umap projection
    um = UMAP()
    um_proj = um.fit_transform(gtr_vectors)

    doc_proj_df = (
        pd.DataFrame(um_proj, columns=["x", "y"])
        .assign(doc_id=gtr_sampled["id"])
        .assign(
            category=lambda df: [
                "ai_genomics"
                if _id in ai_gen_ids
                else "ai"
                if _id in ai_ids
                else "genomics"
                for _id in df["doc_id"].values
            ]
        )
        .assign(
            title=lambda df: df["doc_id"].map(
                gtr_sampled.drop_duplicates("id").set_index("id")["title"]
            )
        )
        .drop_duplicates("doc_id")
        .reset_index(drop=True)
    )

    proj_chart = (
        alt.Chart(doc_proj_df)
        .mark_point(filled=True)
        .encode(
            x=alt.X("x", scale=alt.Scale(zero=False)),
            y=alt.Y("y", scale=alt.Scale(zero=False)),
            color="category",
            tooltip=["doc_id", "title"],
        )
    )

    save_altair(altair_text_resize(proj_chart), "umap_projection", driver=wd)

    logging.info("Finding provisionally optimal number of clusters")
    results = []

    for n in range(5, 100, 5):

        for _ in range(5):
            km = KMeans(n_clusters=n)
            clust = km.fit_predict(um_proj)
            score = silhouette_score(um_proj, clust)
            results.append([n, score])

    sil_results = pd.DataFrame(results, columns=["clust_n", "score"])

    sil_box = (
        alt.Chart(sil_results)
        .mark_boxplot()
        .encode(x="clust_n", y=alt.Y("score", scale=alt.Scale(zero=False)))
    )
    save_altair(altair_text_resize(sil_box), "silh_boxplot", driver=wd)

    logging.info("Run clustering to continue analysis")
    cluster_assign = {
        _id: cl
        for _id, cl in zip(
            gtr_sampled["id"],
            KMeans(n_clusters=50, random_state=123).fit_predict(um_proj),
        )
    }

    doc_proj_df = doc_proj_df.assign(
        cluster=lambda df: df["doc_id"].map(cluster_assign)
    )

    # Cluster distribution
    cluster_distr = (
        alt.Chart(doc_proj_df)
        .mark_bar()
        .encode(
            x=alt.X(
                "cluster:O",
                sort=alt.EncodingSortField("cluster", op="count", order="descending"),
            ),
            y="count()",
            color="category",
        )
    ).properties(width=550, height=200)
    cluster_distr

    save_altair(altair_text_resize(cluster_distr), "cluster_distribution", driver=wd)

    # Cluster content
    cluster_summary = []

    # We remove cluster 16, a bunch of repeated projects
    for cluster in set(cluster_assign.values()) - {16}:

        sel_df = doc_proj_df.query(f"cluster=={cluster}")

        ai_genomic_n = sel_df.query("category == 'ai_genomics'")

        if len(ai_genomic_n) == 0:
            ai_genom_titles = " "
        elif len(ai_genomic_n) <= 5:
            ai_genom_titles = ", ".join(
                ai_genomic_n.sample(len(ai_genomic_n))["title"].values
            )
        else:
            ai_genom_titles = ", ".join(ai_genomic_n.sample(5)["title"].values)

        cluster_summary.append(
            {
                **{
                    "cl_id": cluster,
                    "n_docs": len(sel_df),
                    "n_ai_g": len(ai_genomic_n),
                    "examples": ai_genom_titles,
                },
                **sel_df["category"].value_counts(normalize=True).to_dict(),
            }
        )
    cluster_table = (
        pd.DataFrame(cluster_summary)
        .fillna(0)
        .sort_values("ai_genomics", ascending=False)
        .query("n_ai_g>5")[["cl_id", "n_docs", "n_ai_g", "ai_genomics", "examples"]]
    )

    logging.info(cluster_table.head(10))
    cluster_table.round(3).to_markdown(f"{OUT_PATH}/gtr_clusters.md", index=False)

    # Topic evolution analysis
    selected_clusters = (
        cluster_table.query("n_ai_g > 5")
        .sort_values("n_ai_g", ascending=False)["cl_id"]
        .tolist()
    )

    doc_proj_df = (
        doc_proj_df.assign(
            year=lambda df: df["doc_id"].map(
                gtr_sampled.drop_duplicates("id").set_index("id")["start"]
            )
        )
        .query("year!='Not found'")
        .assign(year=lambda df: df["year"].str.split("-").str[0].astype(int))
    )

    clust_evol = doc_proj_df.groupby(["cluster", "year"]).size()

    clust_evol_plot = clust_evol.reset_index(name="n").merge(
        doc_proj_df.groupby(["cluster", "year"])["category"]
        .value_counts(normalize=True)
        .reset_index(name="ai_g_share")
        .query("category=='ai_genomics'"),
        on=["cluster", "year"],
    )

    evol_bubble = (
        alt.Chart(
            clust_evol_plot.loc[clust_evol_plot["cluster"].isin(selected_clusters)]
        )
        .mark_square(filled=True, stroke="black", strokeWidth=0.2)
        .encode(
            x="year:O",
            y=alt.Y("cluster:O", sort=selected_clusters),
            color=alt.Color(
                "ai_g_share",
                scale=alt.Scale(scheme="Redblue", domainMid=0.5),
                sort="descending",
            ),
            size="n",
        )
    ).properties(width=600, height=400)

    save_altair(altair_text_resize(evol_bubble), "cluster_evolution", driver=wd)

    # Analysis of "emergence"
    clust_year_table = (
        doc_proj_df.loc[doc_proj_df["cluster"].isin(selected_clusters)]
        .query("category=='ai_genomics'")
        .groupby(["cluster", "year"])
        .size()
        .unstack()
        .fillna(0)
    )

    clust_year_recency = (
        clust_year_table.apply(lambda x: x / x.sum(), axis=1).loc[:, 2020:].sum(axis=1)
    )
    clust_year_share = (
        clust_year_table.loc[:, 2020:].sum(axis=1)
        / clust_year_table.loc[:, 2020:].sum(axis=1).sum()
    )

    clust_emergence_comb = (
        pd.concat([clust_year_recency, clust_year_share], axis=1)
        .rename(columns={0: "recency", 1: "significance"})
        .reset_index(drop=False)
    )

    # Plot
    emergence_chart = (
        alt.Chart(clust_emergence_comb)
        .mark_point(filled=True, size=50, stroke="black", strokeWidth=0.5)
        .encode(
            y="recency",
            x="significance",
            size=alt.Size("significance", legend=alt.Legend(format="%")),
            color=alt.Color("recency", legend=alt.Legend(format="%")),
        )
    ).properties(height=350, width=600)

    labels = (
        alt.Chart(clust_emergence_comb)
        .mark_text(color="black", size=9)
        .encode(
            y=alt.Y("recency", title="Recency", axis=alt.Axis(format="%")),
            x=alt.X("significance", title="Significance", axis=alt.Axis(format="%")),
            text="cluster",
        )
    )

    hor = (
        alt.Chart(
            clust_emergence_comb.assign(
                sig_median=lambda df: df["significance"].median()
            )
        )
        .mark_rule(strokeDash=[3, 1])
        .encode(x="sig_median")
    )

    vert = (
        alt.Chart(
            clust_emergence_comb.assign(rec_median=lambda df: df["recency"].median())
        )
        .mark_rule(strokeDash=[3, 1])
        .encode(y="rec_median")
    )

    em_chart_joined = emergence_chart + hor + vert + labels

    save_altair(altair_text_resize(em_chart_joined), "emergence_scatter", driver=wd)


if __name__ == "__main__":
    gtr_cluster_analysis()
