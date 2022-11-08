# Generate integrated analysis of evolution of genomics activity

import logging
from toolz import pipe
from itertools import chain
from collections import Counter
from typing import List, Dict
import pandas as pd
import numpy as np
import altair as alt
from ai_genomics.utils.save_plotting import AltairSaver
from ai_genomics.utils.plotting import configure_plots
from ai_genomics.getters.clusters import (
    get_id_cluster_lookup,
)
from ai_genomics.getters.patents import (
    get_ai_genomics_patents,
)
from ai_genomics.getters.gtr import (
    get_ai_genomics_project_table,
)
from ai_genomics.analysis.influence.make_influence_tables import sample_getter


# Functions


def make_cluster_trend(source: str) -> pd.DataFrame:
    """Create cluster timeline

    Args:
        source: source of data

    Returns:
        Dataframe with cluster activity x year
    """

    if source == "openalex":

        return (
            sample_getter("ai_genomics_openalex_works.csv")
            .drop_duplicates("work_id")
            .assign(cluster=lambda df: df["work_id"].map(get_id_cluster_lookup()))
            .groupby(["cluster", "publication_year"])
            .size()
            .unstack(level=1)
        )

    if source == "patents":

        return pipe(
            get_ai_genomics_patents()
            .drop_duplicates("family_id")
            .assign(
                filed_year=lambda df: [int(x.split("-")[0]) for x in df["filing_date"]]
            )
            .query("filed_year>=2012")
            .assign(
                cluster=lambda df: df["publication_number"].map(get_id_cluster_lookup())
            )
            .groupby(["cluster", "filed_year"])
            .size()
            .unstack(level=1)
            .fillna(0),
            lambda df: df[sorted(df.columns)],
        )

    if source == "gtr":
        return pipe(
            get_ai_genomics_project_table()
            .query("ai_genomics==True")
            .assign(start_year=lambda df: df["start"].str.split("-").str[0].astype(int))
            .assign(cluster=lambda df: df["id"].map(get_id_cluster_lookup()))
            .groupby(["cluster", "start_year"])
            .size()
            .unstack(level=1)
            .fillna(0),
            lambda df: df[sorted(df.columns)],
        )


def make_emergence_indices(
    table: pd.DataFrame, recency_threshold: int = 3
) -> pd.DataFrame:
    """Calculates recency / signficance indices for a table with
        evolution of activity by topic

    Args:
        table: cluster x year activity
        recency_threshold: interval defining recency and significance

    Returns:
        A table with recency (% of all activity in a topic in recent period)
        and significance (% of activity in all topics accounted by a topic)
    """

    recency = table.iloc[:, -recency_threshold:].sum(axis=1) / table.sum(axis=1)
    significance = (
        table.iloc[:, -recency_threshold:].sum(axis=1)
        / table.iloc[:, -recency_threshold:].sum(axis=1).sum()
    )

    return pd.DataFrame({"recency": recency, "significance": significance}).reset_index(
        drop=False
    )


def make_emergence_chart(
    emergence_table: pd.DataFrame,
    source,
    top_terms: int = 5,
    cat_name: str = "cluster_name",
) -> alt.Chart:
    """Returns an emergence plot"""
    # Extract topics with color.
    has_color = set(
        emergence_table.sort_values("significance", ascending=False)[cat_name][
            :top_terms
        ]
    ).union(
        set(
            emergence_table.sort_values("recency", ascending=False)[cat_name][
                :top_terms
            ]
        )
    )

    emergence_plot = (
        emergence_table.copy()
        .assign(median_recency=lambda df: df["recency"].mean())
        .assign(median_significance=lambda df: df["significance"].mean())
        .assign(combined_values=lambda df: df["significance"] * df["recency"])
        .assign(
            title_color=lambda df: [
                x if x in has_color else "Other" for x in df[cat_name]
            ]
        )
    )

    scatter = (
        alt.Chart(emergence_plot)
        .mark_square(filled=True, stroke="black", strokeWidth=1)
        .encode(
            x=alt.X(
                "recency",
                title="Recency",
                scale=alt.Scale(zero=False),
                axis=alt.Axis(format="%"),
            ),
            y=alt.Y(
                "significance",
                title="Significance",
                scale=alt.Scale(zero=False),
                axis=alt.Axis(format="%"),
            ),
            size=alt.Size(
                "significance",
                title="Significance",
                scale=alt.Scale(zero=False),
                legend=None,
            ),
            tooltip=[cat_name],
            color=alt.Color(
                "title_color",
                # scale=alt.Scale(scheme="tableau20"),
                sort=alt.EncodingSortField("recency", order="descending"),
                title="Label",
            ),
        )
    )

    x_line = (
        alt.Chart(emergence_plot)
        .mark_rule(stroke="black", strokeDash=[5, 5])
        .encode(x="median_recency")
    )
    y_line = (
        alt.Chart(emergence_plot)
        .mark_rule(stroke="black", strokeDash=[5, 5])
        .encode(y="median_significance")
    )

    return (scatter + y_line + x_line).properties(title=source, width=350, height=200)


def combined_emergence_chart(
    emergence_tables: List, data_names: List, top_clusters: int = 20
):
    """Creates combined emergence analysis

    Args:
        emergence_tables: list of emergence tables
        data_names: list of data names
        top_clusters: number of clusters to include in the analysis by frequency

    Returns:
        A chart comparing emergence indicators for different data sources
    """

    emergence_combined = pd.concat(
        [t.assign(source=name) for name, t in zip(data_names, emergence_tables)]
    )

    # This gives us the set of top top_cluster clusters by data source
    sorted_clusters = (
        emergence_combined.query("source=='openalex'")
        .sort_values("significance", ascending=False)["cluster"]
        .to_list()
    )

    return (
        alt.Chart(emergence_combined)
        .mark_point(filled=True, size=40, stroke="black", strokeWidth=0.5)
        .encode(
            facet=alt.Facet(
                "cluster", title="Cluster", columns=3, sort=sorted_clusters
            ),
            x=alt.X("recency", axis=alt.Axis(format="%"), title="Recency"),
            size=alt.Y(
                "significance", legend=alt.Legend(format="%"), title="Significance"
            ),
            color=alt.Color("source", title="Data source"),
        )
    ).configure_axis()


def make_chart_heatmap(
    table: pd.DataFrame, columns: list = ["name_1", "name_2", "correlation"]
) -> alt.Chart:
    """
    Plot heatmap of correlations between two variables

    Args:
        table: table with correlations
        columns: columns to use for plotting

    Returns:
        A heatmap chart
    """

    sort_labels = [
        "Openalex: significance",
        "Openalex: recency",
        "Patents: significance",
        "Patents: recency",
        "Gtr: significance",
        "Gtr: recency",
    ]

    heat = (
        alt.Chart(table)
        .mark_rect(stroke="black", strokeWidth=0.5)
        .encode(
            x=alt.X(columns[0], title=None, sort=sort_labels),
            y=alt.X(columns[1], title=None, sort=sort_labels),
            color=alt.Color(
                columns[2], scale=alt.Scale(domainMid=0), sort="descending"
            ),
        )
    )

    text = (
        alt.Chart(table)
        .mark_text()
        .encode(
            x=alt.X(columns[0], title=None, sort=sort_labels),
            y=alt.X(columns[1], title=None, sort=sort_labels),
            text=alt.Text(columns[2], format=".2f"),
            color=alt.condition(
                "datum.correlation > 0.5 | datum.correlation < -0.4 ",
                alt.value("white"),
                alt.value("black"),
            ),
        )
    )

    return (heat + text).properties(width=400, height=250)


if __name__ == "__main__":

    saver = AltairSaver()
    source_names = ["openalex", "patents", "gtr"]

    logging.info("Loading data")
    oalex, pats, gtr = [make_cluster_trend(source) for source in source_names]

    logging.info("Emergence analysis")
    emergence_indices = [
        make_emergence_indices(t, recency_threshold=2) for t in [oalex, pats, gtr]
    ]

    emergence_charts = [
        make_emergence_chart(table, cat_name="cluster", source=source, top_terms=5)
        for table, source in zip(emergence_indices, source_names)
    ]

    saver.save(
        configure_plots(
            alt.vconcat(*emergence_charts).resolve_scale(
                x="shared", color="independent"
            )
        ),
        "emergence_charts",
    )

    saver.save(
        configure_plots(combined_emergence_chart(emergence_indices, source_names)),
        "combined_emergence",
    )

    emergence_indices_labelled = pd.concat(
        [
            t.set_index("cluster").rename(
                columns={
                    "recency": f"{source}_recency",
                    "significance": f"{source}_significance",
                }
            )
            for t, source in zip(emergence_indices, source_names)
        ],
        axis=1,
    )

    emergence_corr = (
        emergence_indices_labelled.corr(method="spearman")
        .stack()
        .reset_index(name="correlation")
        .assign(name_1=lambda df: df["level_0"].str.replace("_", ": ").str.capitalize())
        .assign(name_2=lambda df: df["level_1"].str.replace("_", ": ").str.capitalize())
        .assign(
            correlation=lambda df: [
                c if n1 != n2 else np.nan
                for c, n1, n2 in zip(df["correlation"], df["name_1"], df["name_2"])
            ]
        )
    )
    saver.save(
        configure_plots(make_chart_heatmap(emergence_corr)), "heatmap_indicators"
    )
