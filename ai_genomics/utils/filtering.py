import pandas as pd

def filter_data(
    data: pd.DataFrame, query: str, date_col: str, id_col: str
) -> pd.DataFrame:
    """Filter and rename columns across datasets"""
    return (
        data.query(query)
        .reset_index(drop=True)
        .rename(columns={date_col: "date", id_col: "id"})
        .assign(date=lambda df: pd.to_datetime(df["date"]).dt.date)
    )