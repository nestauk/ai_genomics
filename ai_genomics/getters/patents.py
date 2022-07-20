from fnmatch import fnmatch
from ai_genomics import bucket_name as BUCKET_NAME
from ai_genomics.getters.data_getters import load_s3_data, s3, get_s3_dir_files
import pandas as pd


def get_ai_genomics_patent_num_csv_s3_dirs() -> list:
    """Returns a list of S3 directories containing csvs of AI in genomics
    patent application numbers
    """
    return [
        path
        for path in get_s3_dir_files(
            s3,
            BUCKET_NAME,
            "outputs/patent_data/ai_genomics_id_chunks/golden-shine-355915_genomics_85_chunksize/",
        )
        if fnmatch(path, "*.csv")
    ]


def get_ai_genomics_patent_app_numbers() -> list:
    """Loads chunks of AI in genomics patent application numbers
    from S3. Combines chunks together into one dataframe.
    Returns a list of AI in genomics patent application numbers.
    """
    return list(
        pd.concat(
            [
                load_s3_data(BUCKET_NAME, csv)
                for csv in get_ai_genomics_patent_num_csv_s3_dirs()
            ]
        )
        .drop_duplicates()["application_number"]
        .values
    )


def get_ai_genomics_patents_with_fields() -> pd.DataFrame:
    """From S3 loads dataframe of AI in genomics patents with fields
    such as applicaiton number, patent abstract, publication date, filing date.
    """
    return load_s3_data(
        BUCKET_NAME, "inputs/patent_data/processed_patent_data/ai_genomics_patents.csv"
    )
