from fnmatch import fnmatch
from ai_genomics import bucket_name
from ai_genomics.getters.data_getters import load_s3_data, s3, get_s3_dir_files
import pandas as pd
from boto3_type_annotations.s3 import ServiceResource


def get_ai_genomics_patent_num_csv_s3_dirs(
    s3: ServiceResource = s3, bucket_name: str = bucket_name
) -> list:
    """Gets a list of S3 directories containing csvs of AI in genomics
    patent application numbers

    Args:
        s3: S3 boto resource
        bucket_name: S3 bucket name

    Returns:
        List of S3 directories containing csvs of AI in genomics patent
            application numbers
    """
    return [
        path
        for path in get_s3_dir_files(
            s3,
            bucket_name,
            "outputs/patent_data/ai_genomics_id_chunks/golden-shine-355915_genomics_85_chunksize/",
        )
        if fnmatch(path, "*.csv")
    ]


def get_ai_genomics_patent_app_numbers(bucket_name: str = bucket_name) -> list:
    """Loads chunks of AI in genomics patent application numbers
    from S3. Combines chunks together into one dataframe.

    Args:
        bucket_name: S3 bucket name

    Returns:
        List of AI in genomics patent application numbers
    """
    return list(
        pd.concat(
            [
                load_s3_data(bucket_name, csv)
                for csv in get_ai_genomics_patent_num_csv_s3_dirs()
            ]
        )
        .drop_duplicates()["application_number"]
        .values
    )


def get_ai_genomics_patents_with_fields(
    bucket_name: str = bucket_name,
    file_name: str = "inputs/patent_data/processed_patent_data/ai_genomics_patents.csv",
) -> pd.DataFrame:
    """From S3 loads AI in genomics patents with fields such as applicaiton number,
    patent abstract, publication date, filing date.


    Args:
        bucket_name: S3 bucket name
        file_name: S3 path to filename

    Returns:
        Dataframe of AI in genomics patents
    """
    return load_s3_data(bucket_name, file_name)
