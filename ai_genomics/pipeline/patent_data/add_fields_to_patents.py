from ai_genomics.utils.patent_data.get_ai_genomics_patents_utils import est_conn
from ai_genomics.getters.patents import get_ai_genomics_patent_app_numbers
from ai_genomics.getters.data_getters import save_to_s3, s3
from ai_genomics import bucket_name as BUCKET_NAME
from google.cloud import bigquery
import pandas as pd
import numpy as np

SELECT_COLS = (
    "application_number, publication_number, country_code, title_localized, abstract_localized, "
    "publication_date, filing_date, grant_date, priority_date, inventor, "
    "inventor_harmonized, assignee, assignee_harmonized, ipc, cpc, entity_status"
)
AI_GENOMICS_PATENT_APP_NUMBERS = get_ai_genomics_patent_app_numbers()
S3_SAVE_FILENAME = "/inputs/patent_data/processed_patent_data/ai_genomics_patents.csv"
DATE_COLS = ["publication_date", "grant_date", "filing_date", "priority_date"]


def make_ai_genomics_query(select_cols: str = SELECT_COLS) -> str:
    """Make AI in genomics BigQuery sql query"""
    return (
        f"SELECT {select_cols} FROM `patents-public-data.patents.publications` "
        "WHERE application_number IN UNNEST(@ai_genomics_patent_app_numbers)"
    )


def make_bigquery_job_config(
    ai_genomics_patent_app_numbers: list = AI_GENOMICS_PATENT_APP_NUMBERS,
) -> bigquery.job.query.QueryJobConfig:
    """Make BigQuery job config. This enables a parameter to be fed
    into the query to overcome query length limits"""
    return bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter(
                "ai_genomics_patent_app_numbers",
                "STRING",
                ai_genomics_patent_app_numbers,
            ),
        ]
    )


def replace_missing_values_with_nans(ai_genomics_patents: pd.DataFrame) -> pd.DataFrame:
    """Replace missing values in the AI in
    genomics patents dataset with NaNs"""
    return ai_genomics_patents.replace(
        {date_col: 0 for date_col in DATE_COLS},
        np.nan,
    ).mask(ai_genomics_patents.applymap(str).eq("[]"))


def convert_date_columns_to_datetime(ai_genomics_patents: pd.DataFrame) -> pd.DataFrame:
    """Convert date columns to datetime format
    in the AI in genomics patents dataset"""
    for col in DATE_COLS:
        ai_genomics_patents[col] = pd.to_datetime(
            ai_genomics_patents[col], format="%Y%m%d"
        )
    return ai_genomics_patents


if __name__ == "__main__":
    client = est_conn()
    query = make_ai_genomics_query()
    job_config = make_bigquery_job_config()
    ai_genomics_patents_info = (
        client.query(query, job_config)
        .to_dataframe()
        .pipe(replace_missing_values_with_nans)
        .pipe(convert_date_columns_to_datetime)
    )
    save_to_s3(s3, BUCKET_NAME, ai_genomics_patents_info, S3_SAVE_FILENAME)
