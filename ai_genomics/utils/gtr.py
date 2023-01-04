import pandas as pd


def parse_project_dates(projects: pd.DataFrame) -> pd.DataFrame:
    """Replaces start datetime string with datetime object (date only).

    Args:
        projects (pd.DataFrame): GtR projects table.

    Returns:
        pd.DataFrame: Gtr projects table with date objects in `start` column.
    """
    if projects["start"].dtype == "O":
        projects["start"] = pd.to_datetime(
            [t[0] for t in projects["start"].str.split("T")],
            errors="coerce",
        )
    return projects
