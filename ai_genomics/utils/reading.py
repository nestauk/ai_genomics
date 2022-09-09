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
