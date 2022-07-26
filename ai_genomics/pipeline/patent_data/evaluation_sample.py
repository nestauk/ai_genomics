"""ai_genomics/pipeline/patent_data/evaluation_sample.py

Tool for exporting a sample of
"""

import click
import pandas as pd
from pathlib import PosixPath

from ai_genomics import PROJECT_DIR


def ai_genomics_counts(
    labelled_patents: pd.DataFrame,
    percent: bool = True,
) -> str:
    """Creates a 2x2 markdown table matrix of the count or percentage of
    patents that have been manually labelled as belonging to the
    categories of AI and genomics (or not).

    Args:
        labelled_patents: DataFrame with columns `abstract_en` (str), `is_ai`
            (bool) and `is_genomics` (bool).
        percent: If True, returns the percentage of patents in each category.
            Defaults to True.

    Returns:
        Markdown string of count/percentage table.
    """
    labelled_patents = labelled_patents.dropna(subset=["is_ai"])
    cat_counts = pd.pivot_table(
        labelled_patents, index="is_ai", columns="is_genomics", aggfunc="count"
    )["abstract_en"].rename(
        columns={0: "Not Genomics", 1: "Genomics"},
        index={0: "Not AI", 1: "AI"},
    )

    if percent:
        cat_counts = (cat_counts / labelled_patents.shape[0]) * 100
        cat_counts = cat_counts.round(2)

    cat_counts.index.name = None
    cat_counts.columns.name = None

    return cat_counts.to_markdown()


def ai_genomics_read_full_patent(
    labelled_patents: pd.DataFrame,
    percent: bool = True,
) -> str:
    """Creates a 2x2 markdown table matrix of the count or percentage of
    patents that have been manually labelled as belonging to the
    categories of AI and genomics (or not) AND whose full texts were read
    to ascertain which category they belonged to.

    Args:
        labelled_patents: DataFrame with columns `read_full_patent` (bool),
        `is_ai` (bool) and `is_genomics` (bool).
        percent: If True, returns the percentage of patents in each category.
            Defaults to True.

    Returns:
        Markdown string of count/percentage table.
    """
    labelled_patents = labelled_patents.dropna(subset=["is_ai"])
    cat_counts = pd.pivot_table(
        labelled_patents,
        index="is_ai",
        columns="is_genomics",
        values="read_full_patent",
        aggfunc="sum",
    ).rename(
        columns={0: "Not Genomics", 1: "Genomics"},
        index={0: "Not AI", 1: "AI"},
    )

    if percent:
        cat_counts = (cat_counts / labelled_patents.shape[0]) * 100
        cat_counts = cat_counts.round(2)

    cat_counts.index.name = None
    cat_counts.columns.name = None

    return cat_counts.to_markdown()


VERIFIED_PATH = PROJECT_DIR / "outputs/data/patents/ai_genomics_patents_verified.csv"


@click.command()
@click.option("-i", "--input", default=VERIFIED_PATH)
def run(input):
    if type(input) != PosixPath:
        input = PosixPath(input)

    out_path = input.parent

    labelled = pd.read_csv(input)

    cat_counts = ai_genomics_counts(labelled)
    with open(out_path / "ai_genomics_patents_verified_counts.md", "w") as f:
        f.writelines(cat_counts.split("\n"))

    read_full_counts = ai_genomics_read_full_patent(labelled)
    with open(out_path / "ai_genomics_patents_read_full.md", "w") as f:
        f.writelines(read_full_counts.split("\n"))


if __name__ == "__main__":
    run()
