import os
import pickle
from fnmatch import fnmatch
import boto3
import json

import pandas as pd

from ai_genomics import bucket_name

#####################################################################

s3 = boto3.resource("s3")


def get_s3_dir_files(s3, bucket_name, dir_name):
    """
    get a list of all files in bucket directory.
    s3: S3 boto3 resource
    bucket_name: The S3 bucket name
    dir_name: bucket directory name
    """
    dir_files = []
    my_bucket = s3.Bucket(bucket_name)
    for object_summary in my_bucket.objects.filter(Prefix=dir_name):
        dir_files.append(object_summary.key)

    return dir_files


def load_s3_data(bucket_name, file_name):
    """
    Load data from S3 location.
    s3: S3 boto3 resource
    bucket_name: The S3 bucket name
    file_name: S3 key to load
    """

    obj = s3.Object(bucket_name, file_name)
    if fnmatch(file_name, "*.csv"):
        return pd.read_csv(os.path.join("s3://" + bucket_name, file_name))
    elif fnmatch(file_name, "*.tsv.zip"):
        return pd.read_csv(
            os.path.join("s3://" + bucket_name, file_name), compression="zip", sep="\t"
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
        print(
            'Function not supported for file type other than "*.txt", "*.pickle", "*.tsv" and "*.csv"'
        )


def save_to_s3(s3, bucket_name, output_var, output_file_dir):

    obj = s3.Object(bucket_name, output_file_dir)

    if fnmatch(output_file_dir, "*.pkl") or fnmatch(output_file_dir, "*.pickle"):
        byte_obj = pickle.dumps(output_var)
        obj.put(Body=byte_obj)
    elif fnmatch(output_file_dir, "*.csv"):
        output_var.to_csv("s3://" + bucket_name + output_file_dir, index=False)
    else:
        byte_obj = json.dumps(output_var)
    obj.put(Body=byte_obj)
