# Analysis of influence
import altair as alt
import logging
import os
import numpy as np
import pandas as pd
from itertools import chain
from collections import ChainMap
from toolz import pipe
from statsmodels.formula.api import poisson

from ai_genomics import PROJECT_DIR, bucket_name
from ai_genomics.getters.data_getters import load_s3_data
from ai_genomics.getters.clusters import get_id_cluster_lookup
from ai_genomics.getters.patents import get_ai_genomics_patents
from ai_genomics.getters.gtr import get_ai_genomics_gtr_data
from ai_genomics.analysis.influence.make_influence_tables import sample_getter
from ai_genomics.utils.plotting import configure_plots
from ai_genomics.getters.openalex import instit_metadata
from ai_genomics.utils.save_plotting import AltairSaver

SOURCES = ["openalex", "patstat", "gtr"]


def get_influence(source: str, local: bool = True):

    if local:
        return pd.read_csv(f"{PROJECT_DIR}/outputs/data/{source}/influence_scores.csv")
    else:
        return load_s3_data(
            "ai-genomics", f"outputs/analysis/influence/{source}_influence_scores.csv"
        )


def get_influence_table():

    return (
        pd.concat(
            [
                get_influence(s).assign(source=s if s != "patstat" else "patents")
                for s in SOURCES
            ]
        )
        .query("topic_category=='ai'")
        .reset_index(drop=True)
        .drop(axis=1, labels=["topic_category"])
    )


def get_id_year_lookup():

    openalex_year = (
        sample_getter("ai_genomics_openalex_works.csv")
        .set_index("work_id")["publication_year"]
        .to_dict()
    )
    patstat_year = (
        get_ai_genomics_patents()
        .assign(
            priority_year=lambda df: df["priority_date"]
            .str.split("-")
            .str[0]
            .astype(int)
        )
        .set_index("publication_number")["priority_year"]
        .to_dict()
    )
    gtr_year = (
        get_ai_genomics_gtr_data("projects")
        .assign(start_year=lambda df: df["start"].str.split("-").str[0].astype(int))
        .set_index("id")["start_year"]
        .to_dict()
    )

    return dict(ChainMap(*[openalex_year, patstat_year, gtr_year]))


def get_openalex_institutes_temp():
    """Temporary function to get institute metadata"""

    path = f"{PROJECT_DIR}/outputs/data/openalex/openalex_institutes_v2.csv"

    if os.path.exists(path):
        return pd.read_csv(path)

    else:
        logging.info("Getting institute metadata from OpenAlex - This may take a while")
        instit = load_s3_data(bucket_name, "outputs/openalex/openalex_institutes.csv")
        return instit.to_csv(path, index=False)


def make_chart_influence_clusters(infl_df):
    """Compares influence distributions by cluster"""

    sort_clusters = (
        infl_df.groupby("cluster")["disc_influence"]
        .median()
        .sort_values(ascending=False)
        .index.tolist()
    )

    strip = (
        alt.Chart(infl_df)
        .mark_tick()
        .encode(
            y=alt.Y("cluster", sort=sort_clusters),
            x=alt.X("disc_influence", title="AI influence"),
            fill=alt.Fill(
                "disc_influence",
                legend=None,
                scale=alt.Scale(scheme="redblue"),
                sort="descending",
            ),
        )
    )

    med = (
        alt.Chart(infl_df)
        .mark_tick(color="black", thickness=3)
        .encode(
            y=alt.Y("cluster", sort=sort_clusters), x=alt.X("median(disc_influence)"),
        )
    ).properties(width=200)

    return configure_plots(
        alt.layer(strip, med).facet(
            facet=alt.Facet("source", sort=["openalex", "patents", "gtr"])
        )
    )


def make_chart_influence_time(infl_df):

    strip = (
        alt.Chart(infl_df)
        .mark_tick()
        .encode(
            x="year:O",
            y=alt.Y("disc_influence", title="AI influence"),
            fill=alt.Fill(
                "disc_influence",
                legend=None,
                scale=alt.Scale(scheme="redblue"),
                sort="descending",
            ),
        )
    )

    med = (
        alt.Chart(infl_df)
        .mark_tick(color="black", thickness=3)
        .encode(x="year:O", y=alt.Y("median(disc_influence)"))
    )

    return configure_plots(
        alt.layer(strip, med)
        .facet(facet=alt.Facet("source", sort=["openalex", "patents", "gtr"]))
        .resolve_scale(color="independent")
    )


def make_chart_cluster_trends(infl_df, cut_off_year=2017):
    """Visualise cluster trends by cluster"""

    influence_sort = (
        infl_df.query("source!='gtr'")
        .assign(mid=lambda df: df["year"] > cut_off_year)
        .groupby(["cluster", "mid"])["disc_influence"]
        .median()
        .unstack(level=1)
        .assign(change=lambda df: df[True] / df[False])
        .sort_values("change", ascending=False)
        .index.tolist()
    )

    cl_line = (
        alt.Chart()
        .mark_line()
        .encode(
            x=alt.X("year:O", title=None), color="source", y="median(disc_influence)",
        )
    ).properties(width=150, height=100)
    cl_error = (
        alt.Chart()
        .mark_errorband(extent="ci")
        .encode(
            x="year:O", color="source", y=alt.Y("disc_influence", title="AI influence"),
        )
    )

    return configure_plots(
        alt.layer(cl_line, cl_error, data=infl_df.query("source!='gtr'")).facet(
            facet=alt.Facet("cluster", sort=influence_sort), columns=3
        )
    )


def make_chart_company_comp(infl_instits):
    """Create chart comparing companies / no companies"""

    infl_instits_unique = (
        infl_instits.drop_duplicates("doc_id")
        .reset_index(drop=True)
        .assign(
            has_comp=lambda df: df["has_comp"].replace(
                {False: "No company", True: "Company"}
            )
        )
    )

    sort_vars = (
        infl_instits_unique.groupby(["cluster", "has_comp"])["disc_influence"]
        .median()
        .unstack(level=1)
        .assign(ratio=lambda df: df["Company"] / df["No company"])
        .sort_values("ratio", ascending=False)
        .index.tolist()
    )

    strip = (
        alt.Chart(infl_instits_unique)
        .mark_tick()
        .encode(
            y=alt.Y("has_comp", title=None),
            x=alt.X("disc_influence", title="AI influence"),
            fill=alt.Fill(
                "disc_influence",
                legend=None,
                scale=alt.Scale(scheme="redblue"),
                sort="descending",
            ),
        )
    )

    med = (
        alt.Chart(infl_instits_unique)
        .mark_tick(color="black", thickness=3)
        .encode(y="has_comp", x=alt.Y("median(disc_influence)"))
    )

    return configure_plots(
        alt.layer(strip, med).facet(
            facet=alt.Facet("cluster", sort=sort_vars), columns=3
        )
    )


def make_chart_insitutions(infl_inst, top_n=10):
    """Creates plot with discipline influence by org"""
    # Consider 1 inst per doc
    infl_inst_pairs = infl_inst.drop_duplicates(["doc_id", "display_name"]).reset_index(
        drop=True
    )

    # Find the top 10
    top_insts = list(
        chain(
            *[
                infl_inst_pairs.query(f"org_type=='{org}'")["display_name"]
                .value_counts()[:top_n]
                .index.to_list()
                for org in ["education", "company", "government", "healthcare"]
            ]
        )
    )

    # Focus on those in tip institutions
    instit_comp = infl_inst_pairs.loc[
        infl_inst_pairs["display_name"].isin(top_insts)
    ].reset_index(drop=True)

    # Sort organisations by median AI influence
    sort_orgs = (
        instit_comp.groupby("display_name")["disc_influence"]
        .median()
        .sort_values(ascending=False)
        .index.tolist()
    )

    comp_chart = (
        alt.Chart(instit_comp)
        .mark_point(filled=True, stroke="black", strokeWidth=0.2, size=50)
        .encode(
            y=alt.Y("display_name", sort=sort_orgs, title=None),
            shape=alt.Shape("org_type", title="Type of institution"),
            x=alt.X("disc_influence", title="AI influence"),
            tooltip=["display_name", "cluster"],
            color=alt.Color("cluster", scale=alt.Scale(scheme="tableau20")),
        )
    )
    ticks = (
        alt.Chart(instit_comp)
        .mark_tick(stroke="black")
        .encode(
            y=alt.Y("display_name", sort=sort_orgs, title="Institution"),
            x=alt.X("median(disc_influence)", title="AI influence"),
        )
    )

    return (
        configure_plots(comp_chart + ticks)
        .configure_axis(labelLimit=300, labelFontSize=14)
        .properties(width=300)
    )


def rank_citation(
    citation_scores, index, quantiles: list = [0, 0.25, 0.50, 0.75, 0.9, 0.99, 1]
):
    """Generates tables with citation rankings"""

    quant = np.quantile(citation_scores, quantiles)

    return pd.DataFrame(
        [[x >= q for q in quant] for x in citation_scores],
        columns=quantiles,
        index=index,
    )


def make_instits_df(insts):
    """Create a instits df that includes all institution - paper pairs"""

    return (
        insts.rename(columns={"id": "doc_id"})
        .merge(
            inst_meta[["id", "display_name", "type", "country_code"]],
            left_on="inst_id",
            right_on="id",
        )
        .rename(columns={"type": "org_type"})
        .assign(
            has_comp=lambda df: df["doc_id"].map(
                # True if there is at least one company in the paper
                df.groupby("doc_id")["org_type"].apply(
                    lambda org_types: "company" in set(org_types)
                )
            )
        )
        # So we can work with clusters and years
        .assign(cluster=lambda df: df["doc_id"].map(id_cl_lookup))
        .dropna(axis=0, subset=["cluster"])
        .reset_index(drop=True)
    )


def make_citations_df(instits_all, weighted="year"):
    """Creates a df for citation analysis.
    Args:
        weighted: whether citations are year of field/year weighted
    """

    if weighted == "year":

        return (
            pipe(
                instits_all[
                    [
                        "doc_id",
                        "inst_id",
                        "org_type",
                        "display_name",
                        "country_code",
                        "has_comp",
                        "cluster",
                        "year",
                    ]
                ]
                .assign(
                    cited_by_count=lambda df: df["doc_id"].map(
                        ai_genom_works.set_index("work_id")["cited_by_count"]
                    )
                )
                .drop_duplicates(["doc_id", "inst_id"]),
                lambda df: df.merge(
                    (
                        df.groupby("year")
                        .apply(
                            lambda df: rank_citation(df["cited_by_count"], df["doc_id"])
                        )
                        .reset_index(level=0, drop=True)
                        .stack()
                        .reset_index(name="has_score")
                        .rename(columns={"level_1": "citation_threshold"})
                    ),
                    left_on="doc_id",
                    right_on="doc_id",
                ),
            )
            .drop_duplicates(["doc_id", "citation_threshold"])
            .reset_index(drop=False)
        )

    if weighted == "cluster_year":

        return (
            pipe(
                instits_all[
                    [
                        "doc_id",
                        "inst_id",
                        "org_type",
                        "display_name",
                        "country_code",
                        "has_comp",
                        "cluster",
                        "year",
                    ]
                ]
                .assign(
                    cited_by_count=lambda df: df["doc_id"].map(
                        ai_genom_works.set_index("work_id")["cited_by_count"]
                    )
                )
                .drop_duplicates(["doc_id", "inst_id"]),
                lambda df: df.merge(
                    (
                        df.groupby(["year", "cluster"])
                        .apply(
                            lambda df: rank_citation(df["cited_by_count"], df["doc_id"])
                        )
                        .reset_index(level=0, drop=True)
                        .stack()
                        .reset_index(name="has_score")
                        .drop(axis=1, labels=["cluster"])
                        .rename(columns={"level_2": "citation_threshold"})
                    ),
                    left_on="doc_id",
                    right_on="doc_id",
                ),
            )
            .drop_duplicates(["doc_id", "inst_id", "citation_threshold"])
            .reset_index(drop=False)
        )


def make_chart_citation_ranks(cit_df):
    """Citations"""

    cit_shares = (
        cit_df.groupby(["cluster", "citation_threshold"])["has_score"]
        .mean()
        .reset_index(drop=False)
        .assign(equal_prop=lambda df: 1 - df["citation_threshold"])
    )

    sort_cits = (
        cit_shares.query("citation_threshold==0.75")
        .sort_values("has_score", ascending=False)["cluster"]
        .to_list()
    )

    cit_lines = (
        alt.Chart(cit_shares)
        .mark_line(point=True, color="orange")
        .encode(
            y=alt.Y(
                "citation_threshold",
                axis=alt.Axis(format="%"),
                title="citation ranking",
            ),
            x=alt.X("has_score", axis=alt.Axis(format="%"), title="% of papers"),
        )
    )

    cit_diag = (
        alt.Chart(cit_shares)
        .mark_line(stroke="grey", strokeDash=[2, 2])
        .encode(
            y=alt.Y(
                "citation_threshold",
                axis=alt.Axis(format="%"),
                title="citation ranking",
            ),
            x=alt.Y("equal_prop", axis=alt.Axis(format="%")),
        )
    ).properties(width=80, height=80)

    return alt.layer(cit_lines, cit_diag).facet(
        facet=alt.Facet("cluster", sort=sort_cits), columns=3
    )


def make_chart_citation_evol(cit_df):
    """Plot evolution of citations"""

    # Evolution over time
    citation_evol = pipe(
        cit_df.groupby("year")
        .apply(
            lambda df: df.query("has_score==True")
            .groupby("citation_threshold")["cluster"]
            .value_counts(normalize=True)
        )
        .reset_index(name="share"),
        lambda df: df.loc[df["citation_threshold"].astype(str).isin(["0.0", "0.9"])],
    )

    sorted_clusters = (
        citation_evol.assign(
            citation_threshold=lambda df: df["citation_threshold"].astype(str)
        )
        .query("year>=2021")
        .groupby(["cluster", "citation_threshold"])["share"]
        .mean()
        .unstack(level=1)
        .fillna(0)
        .assign(impact=lambda df: df["0.9"] / df["0.0"])
        .sort_values("impact", ascending=False)
        .index.tolist()
    )

    return pipe(
        (
            alt.Chart(citation_evol.query("share>0"))
            .mark_line(point=True)
            .encode(
                x=alt.X("year:O", title=None),
                y=alt.Y(
                    "share",
                    axis=alt.Axis(format="%"),
                    title="% of all",
                    scale=alt.Scale(type="log"),
                ),
                color=alt.Color("citation_threshold:O", title="Citation ranking"),
                facet=alt.Facet("cluster", columns=3, sort=sorted_clusters),
            )
        )
        .resolve_scale(y="shared")
        .properties(width=120, height=100),
        configure_plots,
    ).configure_axis(labelFontSize=11)


def make_chart_citation_comp(cit_df_cluster_weighted):
    """Looks at the respresentation of company papers in different categories"""

    citation_comp = (
        cit_df_cluster_weighted.query("has_score==True")
        .groupby(["cluster", "citation_threshold"])["has_comp"]
        .value_counts(normalize=True)
        .reset_index(name="share")
    )

    sort_cluster = (
        citation_comp.assign(
            citation_threshold=lambda df: df["citation_threshold"].astype(str)
        )
        .loc[citation_comp["citation_threshold"].astype(str).isin(["0.0", "0.9"])]
        .query("has_comp==True")
        .pivot_table(index="cluster", columns="citation_threshold", values="share")
        .assign(overrep=lambda df: df["0.9"] / df["0.0"])
        .sort_values("overrep", ascending=False)
        .index.tolist()
    )

    return pipe(
        (
            alt.Chart(citation_comp)
            .mark_bar(filled=True)
            .encode(
                x=alt.X("citation_threshold:O", title="Citation rank"),
                y=alt.Y("share", axis=alt.Axis(format="%"), title="Share"),
                color=alt.Color("has_comp", title=["Involving", "company"]),
                facet=alt.Facet("cluster", columns=3, sort=sort_cluster),
            )
        ).properties(width=90, height=75),
        configure_plots,
    )


def make_reg_data(cit_df_year):
    """Make dataset for regression analysis"""

    return (
        cit_df_year.drop_duplicates("doc_id")
        .assign(
            n_instits=lambda df: df["doc_id"].map(
                instits_all.drop_duplicates(["doc_id", "inst_id"])
                .groupby("doc_id")["inst_id"]
                .size()
            )
        )
        .assign(n_instits_log=lambda df: np.log(df["n_instits"]))
        .assign(
            ai_influence=lambda df: df["doc_id"].map(
                get_influence("openalex")
                .query("topic_category=='ai'")
                .set_index("doc_id")["disc_influence"]
            )
        )[
            [
                "cited_by_count",
                "year",
                "cluster",
                "n_instits",
                "n_instits_log",
                "ai_influence",
                "has_comp",
            ]
        ]
        .reset_index(drop=True)
    )


def clean_reg_table(reg_result):

    clean_cluster_names = {
        n: f"Cluster: {n.split('T.')[1][:-1]}"
        for n in [n for n in reg_result.params.index if "cluster" in n]
    }

    clean_var_lookup = {
        **{
            "has_comp[T.True]": "Paper with company",
            "n_instits_log": "Number of Institutes (log)",
            "ai_influence": "AI influence",
            "year": "Year",
        },
        **clean_cluster_names,
    }

    return (
        pd.concat(
            [
                reg_result.params.to_frame().rename(columns={0: "coefficient"}),
                reg_result.conf_int(),
            ],
            axis=1,
        )
        .drop(axis=0, labels=["Intercept"])
        .assign(var_name=lambda df: df.index.map(clean_var_lookup))
        .reset_index(drop=True)
        .rename(columns={0: "low", 1: "high"})
    )


def make_chart_regression(reg_results):
    """Plots the regression result"""

    point = (
        alt.Chart(reg_results)
        .mark_point(
            filled=True, color="orange", size=50, stroke="black", strokeWidth=0.5
        )
        .encode(y=alt.Y("var_name", title="variable"), x=alt.X("coefficient"))
    )
    error = (
        alt.Chart(reg_results)
        .mark_errorbar()
        .encode(
            y="var_name",
            x=alt.X("low", title="Regression coefficient"),
            x2=alt.X2("high"),
        )
    )

    zero = alt.Chart(reg_results.assign(z=0)).mark_rule(stroke="black").encode(x="z:Q")

    return configure_plots(point + error + zero).properties(height=350)


if __name__ == "__main__":

    alt.data_transformers.disable_max_rows()

    saver = AltairSaver()

    id_cl_lookup = get_id_cluster_lookup()
    id_year_lookup = get_id_year_lookup()

    logging.info("Semantic influence analysis")
    # Note that this only includes a sample of openalex AI genomics papers, and representive
    # patents from patent families

    infl_df = (
        get_influence_table()
        .assign(cluster=lambda df: df["doc_id"].map(id_cl_lookup))
        .assign(year=lambda df: df["doc_id"].map(id_year_lookup))
        .dropna(axis=0, subset=["cluster"])
        .query("year>=2012")
        .query("year<=2023")
        .reset_index(drop=True)
    )

    saver.save(make_chart_influence_clusters(infl_df), "influence_clusters")
    saver.save(make_chart_influence_time(infl_df), "influence_time")
    saver.save(make_chart_cluster_trends(infl_df), "influence_cluster_evol")

    # Comparison of private companies with academic institutions
    insts = get_openalex_institutes_temp()
    inst_meta = instit_metadata()

    infl_instits = (
        insts.drop(axis=1, labels=["year"])
        .rename(columns={"id": "doc_id"})
        .merge(
            inst_meta[["id", "display_name", "type", "country_code"]],
            left_on="inst_id",
            right_on="id",
        )
        .rename(columns={"type": "org_type"})
        .assign(
            has_comp=lambda df: df["doc_id"].map(
                # True if there is at least one company in the paper
                df.groupby("doc_id")["org_type"].apply(
                    lambda org_types: "company" in set(org_types)
                )
            )
        )
        # So we can work with clusters and years
        .merge(infl_df, left_on="doc_id", right_on="doc_id", how="inner")
    )

    saver.save(make_chart_company_comp(infl_instits), "influence_cluster_instit")

    infl_inst_pairs = infl_instits.drop_duplicates(
        ["doc_id", "display_name"]
    ).reset_index(drop=True)

    saver.save(make_chart_insitutions(infl_instits, top_n=10), "influence_institutions")

    logging.info("Citation influence analysis")

    ai_genom_works = (
        sample_getter("ai_genomics_openalex_works.csv")
        .drop_duplicates("work_id")
        .reset_index(drop=False)
    )

    id_cited_lookup = ai_genom_works.set_index("doc_id")["cited_by_count"].to_dict()
    instits_all = make_instits_df(insts)
    cit_df_year = make_citations_df(instits_all, weighted="year")

    pd.concat(
        [
            cit_df_year.drop_duplicates("doc_id")["cited_by_count"]
            .describe()
            .to_frame()
            .T,
            cit_df_year.drop_duplicates("doc_id")
            .groupby("cluster")["cited_by_count"]
            .describe()
            .sort_values("mean", ascending=False),
        ]
    ).to_csv(
        f"{PROJECT_DIR}/outputs/data/openalex/citation_stats.csv", float_format="%.2f"
    )

    saver.save(
        configure_plots(make_chart_citation_ranks(cit_df_year)), "citation_clusters"
    )
    saver.save(
        configure_plots(make_chart_citation_evol(cit_df_year)), "citation_evolution"
    )

    # Citation comparisons
    cit_df_cluster_weighted = make_citations_df(instits_all, weighted="cluster_year")
    saver.save(make_chart_citation_comp(cit_df_cluster_weighted), "citation_company")

    # Regression analysis
    instits_all.drop_duplicates(["doc_id", "inst_id"]).groupby("doc_id")[
        "inst_id"
    ].size().sort_values()

    reg_data = make_reg_data(cit_df_year)
    mod = poisson(
        "cited_by_count ~ has_comp + n_instits_log + year + ai_influence + C(cluster)",
        data=reg_data.dropna(),
    )
    result = mod.fit(cov_type="HC2")
    reg_results = clean_reg_table(result)
    saver.save(make_chart_regression(reg_results), "citation_regression")
