"""
metadata pipeline
-----------------

A pipeline that takes the venues, concepts and institutions data
from the OpenAlex data dump.
"""
from metaflow import FlowSpec, step, S3
import boto3
import gzip
import json


def get_bucket_file_list(bucket, prefix):
    s3client = boto3.client("s3", region_name="us-east-1")
    objects = s3client.list_objects(Bucket=bucket, Prefix=f"data/{prefix}/")
    file_keys = []
    for item in objects["Contents"]:
        if ".g" in item["Key"]:  # .gz files contain data
            file_keys.append(item["Key"])
        else:
            pass
    return file_keys


def unzip_file(output_list, bucket, key):
    s3client = boto3.client("s3", region_name="us-east-1")
    obj = s3client.get_object(Bucket=bucket, Key=key)
    with gzip.open(obj["Body"], "r") as institutions_jsonl:
        for institution_json in institutions_jsonl:
            output_list.append(json.loads(institution_json))


class OpenAlexFlow(FlowSpec):
    @step
    def start(self):
        """
        Starts the flow, and defines the list of
        areas to be collected.
        """
        self.LIST = ["venues", "concepts", "institutions"]
        self.next(self.process, foreach="LIST")

    @step
    def process(self):
        """
        For each area, loops over the files in the S3 bucket,
        decompresses the json, and appends the a list of dicts.
        Finally, saves the output file.
        """
        file_keys = get_bucket_file_list(bucket="openalex", prefix=self._find_input)
        data = []
        for key in file_keys:
            unzip_file(output_list=data, bucket="openalex", key=key)
        filename = f"{self.input}.json"
        with S3(run=self) as s3:
            data = json.dumps(data)
            s3.put(filename, data)
        self.next(self.dummy_join)

    @step
    def dummy_join(self, inputs):
        """
        Dummy join step.
        """
        self.next(self.end)

    @step
    def end(self):
        """
        Ends the flow.
        """
        pass


if __name__ == "__main__":
    OpenAlexFlow()
