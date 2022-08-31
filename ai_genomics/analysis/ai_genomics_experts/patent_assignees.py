from ai_genomics.getters.patents import get_ai_genomics_patents
from ai_genomics import PROJECT_DIR
import pandas as pd

EXPERTS_PATH = PROJECT_DIR / "outputs/data/experts/"


def assignee_string_to_list(patents: pd.DataFrame) -> pd.DataFrame:
    """Turn string values in column into list
    For example, string "['Massachusetts Institute Of Technology' 'Peck, David, D.']"
    becomes list ['Massachusetts Institute Of Technology', 'Peck, David, D.']
    """
    patents["assignee"] = (
        patents["assignee"]
        .str.replace("'s", "s", regex=False)
        .str.split("'")
        .apply(
            lambda assignee: [
                token for token in assignee if token not in ["[", " ", "\n ", "]"]
            ]
        )
    )
    return patents


def replace_sq_brackets_and_dbl_quotes(patents: pd.DataFrame) -> pd.DataFrame:
    """Replace [] and "" from assignee text
    For example, ["Myriad Womens Health, Inc."]
    becomes Myriad Womens Health, Inc.
    """
    patents["assignee"] = patents["assignee"].str.replace('^\["|"\]$', "", regex=True)
    return patents


def find_top_n_most_assigned_patents(patents: pd.DataFrame, n: int) -> pd.DataFrame:
    """Find top n most assigned to patents assignees

    Args:
        patents: Patents dataframe including columns for
            assignee and publication_number
        n: Number of top most assigned to patents assignees
            to select for

    Returns:
        Top n most assigned to patents assignees
    """
    return (
        patents.explode("assignee")
        .pipe(replace_sq_brackets_and_dbl_quotes)
        .groupby("assignee")["publication_number"]
        .count()
        .sort_values(ascending=False)
        .head(n)
        .reset_index()
        .rename(columns={"publication_number": "patent_pub_number_count"})
    )


if __name__ == "__main__":
    top_20_most_assigned_ai_genomics_patents = (
        get_ai_genomics_patents()
        .dropna(subset=["assignee"])
        .pipe(assignee_string_to_list)
        .pipe(find_top_n_most_assigned_patents, 20)
    )

    EXPERTS_PATH.mkdir(parents=True, exist_ok=True)

    top_20_most_assigned_ai_genomics_patents.to_csv(
        EXPERTS_PATH / "ai_genomics_top_20_patent_assignees.csv", index=False
    )
