from ai_genomics.pipeline.entity_extraction.postprocess_entities import EntityCleaner
from ai_genomics.getters.data_getters import save_to_s3

from metaflow import FlowSpec, step, retry

import ast
import boto3
import json

s3 = boto3.resource("s3")
client = boto3.client("s3")
path = "outputs/entity_extraction/"
bucket = "ai-genomics"


def get_all_lookups():
    """Get all lookups stored on the S3 bucket 'ai-genomics'
    or in a specific directory.
    Returns:
        list: All lookups.
    """
    batches = [
        obj["Key"]
        for obj in client.list_objects(Bucket=bucket, Prefix=path, Delimiter="/")[
            "Contents"
        ]
        if obj["Key"].endswith("_lookup.json")
    ]
    return batches


class PostprocessEntitiesFlow(FlowSpec):
    @step
    def start(self):
        self.links = get_all_lookups()
        print(self.links)
        self.next(self.postprocess, foreach="links")

    @retry
    @step
    def postprocess(self):
        content_object = s3.Object(bucket, self.input)
        file_content = content_object.get()["Body"].read().decode("utf-8")
        entities = ast.literal_eval(json.loads(json.dumps(file_content)))
        ec = EntityCleaner()
        clean_entities = {
            text_id: ec.filter_entities(entity) for text_id, entity in entities.items()
        }
        filename = f"{self.input.replace('.json', '_clean.json')}"
        save_to_s3(bucket, clean_entities, filename)
        self.next(self.join)

    @step
    def join(self, inputs):
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    PostprocessEntitiesFlow()
