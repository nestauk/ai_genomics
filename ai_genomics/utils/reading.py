from io import BytesIO
import json
import logging
import pathlib
from typing import Union, Dict, List, Any
import pandas as pd
import boto3
import zipfile


logger = logging.getLogger(__name__)


def read_json(data: Union[pathlib.Path, str]) -> List[Dict]:
    """
    Reads the json file and returns the data
    """
    with open(data) as json_file:
        return json.load(json_file)


def fetch_s3(s3_path) -> Union[pd.DataFrame, Dict]:
    """
    Fetches the s3 file and returns the data
    """
    s3 = boto3.resource("s3")
    bucket = s3.Bucket("ai-genomics")
    obj = bucket.Object(s3_path)
    return pd.read_csv(obj.get()["Body"])


def convert_str_to_pathlib_path(path: Union[pathlib.Path, str]) -> pathlib.Path:
    """Convert string path to pathlib Path"""
    return pathlib.Path(path) if type(path) is str else path


def make_path_if_not_exist(path: Union[pathlib.Path, str]):
    """If the path does not exist, make it"""
    path = convert_str_to_pathlib_path(path)
    if not path.exists():
        path.mkdir(parents=True)


def extractall(bytes: BytesIO, path: Union[pathlib.Path, str]):
    """Extracts a bytes type zip file to a specified path."""
    logger.info(f"Extracting to {path}")

    make_path_if_not_exist(path)
    z = zipfile.ZipFile(bytes)
    z.extractall(path)
