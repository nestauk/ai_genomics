# Generate integrated analysis of evolution of genomics activity

from ai_genomics.getters.openalex import (
    get_openalex_ai_genomics_works,
    get_openalex_entities,
    get_openalex_ai_genomics_works_entity_groups,
)
from ai_genomics.getters.patents import (
    get_ai_genomics_patents,
    get_patent_ai_genomics_entity_groups,
)
from ai_genomics.getters.gtr import (
    get_ai_genomics_project_table,
    get_gtr_ai_genomics_project_entity_groups,
)
from ai_genomics.getters.entities import get_entity_cluster_lookup

import altair as alt
import logging
from toolz import pipe
from itertools import chain
from collections import Counter
from typing import List, Dict
import pandas as pd
import numpy as np
from ai_genomics.utils.save_plotting import AltairSaver
from ai_genomics.utils.plotting import configure_plots


# Functions
def make_entity_cluster_names(level: int = 100, top_names: int = 3) -> Dict:
    """Makes the names for entity clusters using openalex entity frequencies
    Args:
        level: the clustering level to use
        top_names: top number of entities to use in the cluster name

    Returns:
        A dict between cluster ids and names
    """

    return (
        pipe(
            get_openalex_entities(),
            lambda _dict: {k: v for k, v in _dict.items() if (len(v) > 0)},
            lambda _dict: {k: [el[0] for el in v] for k, v in _dict.items()},
            lambda _dict: pd.Series(Counter(chain(*[v for v in _dict.values()]))),
        )
        .to_frame()
        .rename(columns={0: "frequency"})
        .assign(cluster=lambda df: df.index.map(get_entity_cluster_lookup(level)))
        .reset_index(drop=False)
        .groupby("cluster")
        .apply(
            lambda df: "_".join(
                df.sort_values("frequency", ascending=False)["index"][:top_names]
            ).lower()
        )
        .to_dict()
    )


def make_doc_meta_dict(data_source: str) -> Dict:
    """Makes a lookup between doc ids and relevant metadata
    (years in the case of openalex and gtr, family id and pub year in the case of patents)

    Args:
        data_source: the data source to use
    """

    if data_source == "openalex":
        return (
            get_openalex_ai_genomics_works()
            .query("ai_genomics==True")
            .set_index("work_id")["publication_year"]
            .to_dict()
        )

    if data_source == "patents":
        return (
            get_ai_genomics_patents()
            .set_index("publication_number")
            .assign(
                pub_year=lambda df: [int(x.split("-")[0]) for x in df["filing_date"]]
            )[["pub_year", "family_id"]]
            .to_dict()
        )

    if data_source == "gtr":

        return (
            get_ai_genomics_project_table()
            .query("ai_genomics==True")
            .set_index("id")
            .assign(
                start_year=lambda df: df["start"].str.split("-").str[0].astype(int)
            )["start_year"]
            .to_dict()
        )


def make_cluster_year_table(
    source: str,
    metadata_lookup: Dict,
    cluster_names: Dict,
    level: int = 100,
) -> pd.DataFrame:
    """
    Creates a df with cluster name per year frequencies:

    Args:
        level: clustering level
        entity_getter: an entity getter for a dataset
        metadata_lookup: a lookup between doc ids and years / patent id in the case of patents
        cluster_names; lookup between cluster numbers and names

    Returns:
        A table with cluster names and frequencies per year
    """

    if source != "patents":

        entity_cluster_getter = (
            get_openalex_ai_genomics_works_entity_groups
            if source == "openalex"
            else get_gtr_ai_genomics_project_entity_groups
        )

        return (
            entity_cluster_getter(level)
            .set_index("id")
            .stack()
            .reset_index(name="has_cluster")
            .assign(year=lambda df: df["id"].map(metadata_lookup))
            .assign(
                cluster_name=lambda df: df["level_1"].astype(int).map(cluster_names)
            )
            .pivot_table(
                index="cluster_name",
                columns="year",
                values="has_cluster",
                aggfunc="sum",
            )
        )

    else:
        # Things get more involved with patents because we
        # need to deduplicate patent families. We assume that any entity cluster present in a
        # patent within a patent family is present in the patent family
        return (
            get_patent_ai_genomics_entity_groups(level)
            .set_index("id")
            .stack()
            .reset_index(name="has_cluster")
            .assign(year=lambda df: df["id"].map(metadata_lookup["pub_year"]))
            .assign(family=lambda df: df["id"].map(metadata_lookup["family_id"]))
            .groupby(["family", "level_1", "year"])["has_cluster"]
            .apply(lambda x: sum(x) >= 1)
            .reset_index(drop=False)
            .pivot_table(
                index="level_1", columns="year", values="has_cluster", aggfunc="sum"
            )
            .reset_index(drop=False)
            .assign(
                cluster_name=lambda df: df["level_1"].astype(float).map(cluster_names)
            )
            .drop(axis=1, labels=["level_1"])
            .set_index("cluster_name")
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
    emergence_table: pd.DataFrame, top_terms: int = 5, cat_name: str = "cluster_name"
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
                x if x in has_color else np.nan for x in df[cat_name]
            ]
        )
    )

    scatter = (
        alt.Chart(emergence_plot)
        .mark_square(filled=True, stroke="black", strokeWidth=0.2)
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
                scale=alt.Scale(scheme="tableau20"),
                sort=alt.EncodingSortField("recency", order="descending"),
                title="Label",
            ),
        )
    )

    x_line = (
        alt.Chart(emergence_plot).mark_rule(stroke="black").encode(x="median_recency")
    )
    y_line = (
        alt.Chart(emergence_plot)
        .mark_rule(stroke="black")
        .encode(y="median_significance")
    )

    return scatter + y_line + x_line


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
    selected_clusters = set(
        emergence_combined.groupby("source")
        .apply(
            lambda x: x.sort_values("significance", ascending=False)["cluster_name"][
                :top_clusters
            ]
        )
        .values
    )

    return (
        alt.Chart(
            emergence_combined.loc[
                emergence_combined["cluster_name"].isin(selected_clusters)
            ].assign(
                short_name=lambda df: [
                    "_".join(k.split("_")[:2]) for k in df["cluster_name"]
                ]
            )
        )
        .mark_point(filled=True, size=40, stroke="black", strokeWidth=0.5)
        .encode(
            facet=alt.Facet(
                "short_name",
                columns=3,
                sort=alt.EncodingSortField(
                    "significance", op="mean", order="descending"
                ),
            ),
            x=alt.X("recency", axis=alt.Axis(format="%"), title="Emergence"),
            size=alt.Y(
                "significance", legend=alt.Legend(format="%"), title="Significance"
            ),
            color=alt.Color("source", title="Data source"),
        )
    ).configure_axis()


if __name__ == "__main__":

    saver = AltairSaver()

    cluster_names = make_entity_cluster_names()

    data_sources = ["openalex", "patents", "gtr"]

    emergence_tables_container = []

    for source in data_sources:
        logging.info(f"Analysing emergence in {source}")

        emergence_table = pipe(
            make_cluster_year_table(source, make_doc_meta_dict(source), cluster_names),
            make_emergence_indices,
        )

        emergence_tables_container.append(emergence_table)

        emergence_chart = pipe(
            make_emergence_chart(emergence_table, top_terms=10), configure_plots
        ).properties(
            width=500, height=400, title=f"{source.capitalize()} Emergence Table"
        )

        saver.save(emergence_chart, f"emergence_{source}")

    emergence_combined = combined_emergence_chart(
        emergence_tables_container, data_sources
    )

    saver.save(emergence_combined, "emergence_combined")
