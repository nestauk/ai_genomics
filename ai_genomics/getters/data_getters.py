import pickle
from fnmatch import fnmatch
import boto3
import json
import pandas as pd
from ai_genomics import logger
from typing import Union, List

S3 = boto3.resource("s3")


def get_s3_dir_files(bucket_name: str, dir_name: str) -> List[str]:
    """
    Get a list of all files in bucket directory.

    Args:
        bucket_name: S3 bucket name
        dir_name: S3 bucket directory name

    Returns:
        list_dir: List of file names in bucket directory
    """
    my_bucket = S3.Bucket(bucket_name)
    return [
        object_summary.key
        for object_summary in my_bucket.objects.filter(Prefix=dir_name)
    ]


def load_s3_data(bucket_name: str, file_name: str) -> Union[pd.DataFrame, str, dict]:
    """
    Load data from S3 location.

    Args:
        bucket_name: The S3 bucket name
        file_name: S3 key to load

    Returns:
        Loaded data from S3 location.
    """

    obj = S3.Object(bucket_name, file_name)
    if fnmatch(file_name, "*.csv"):
        return pd.read_csv(f"s3://{bucket_name}/{file_name}")
    elif fnmatch(file_name, "*.tsv.zip"):
        return pd.read_csv(
            f"s3://{bucket_name}/{file_name}",
            compression="zip",
            sep="\t",
        )
    elif fnmatch(file_name, "*.pickle") or fnmatch(file_name, "*.pkl"):
        file = obj.get()["Body"].read()
        return pickle.loads(file)
    elif fnmatch(file_name, "*.txt"):
        file = obj.get()["Body"].read().decode()
        return [f.split("\t") for f in file.split("\n")]
    elif fnmatch(file_name, "*.json"):
        file = obj.get()["Body"].read().decode()
        return json.loads(file)
    else:
        logger.exception(
            'Function not supported for file type other than "*.json", *.txt", "*.pickle", "*.tsv" and "*.csv"'
        )


def save_to_s3(bucket_name: str, output_var, output_file_dir: str):
    """
    Save data to S3 location.

    Args:
        s3: S3 boto3 resource
        bucket_name: The S3 bucket name
        output_var: Object to be saved
        output_file_dir: file path to save object to
    """

    obj = S3.Object(bucket_name, output_file_dir)

    if fnmatch(output_file_dir, "*.pkl") or fnmatch(output_file_dir, "*.pickle"):
        obj.put(Body=pickle.dumps(output_var))
    elif fnmatch(output_file_dir, "*.txt"):
        obj.put(Body=output_var)
    elif fnmatch(output_file_dir, "*.csv"):
        output_var.to_csv("s3://" + bucket_name + "/" + output_file_dir, index=False)
    elif fnmatch(output_file_dir, "*.json"):
        obj.put(Body=json.dumps(output_var))
    else:
        logger.exception(
            'Function not supported for file type other than "*.json", *.txt", "*.pickle", "*.tsv" and "*.csv"'
        )
    logger.info(f"Saved to s3://{bucket_name} + {output_file_dir} ...")
