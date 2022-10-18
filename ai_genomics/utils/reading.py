import json
import pathlib
from typing import Union, Dict, List, Any
import pandas as pd
import boto3


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


def _convert_str_to_pathlib_path(path: Union[pathlib.Path, str]) -> pathlib.Path:
    """Converts a path written as a string to pathlib format"""
    return pathlib.Path(path) if type(path) is str else path


def make_path_if_not_exist(path: Union[pathlib.Path, str]):
    """Check if path exists, if it does not exist then create it"""
    path = _convert_str_to_pathlib_path(path)
    if not path.exists():
        path.mkdir(parents=True)
