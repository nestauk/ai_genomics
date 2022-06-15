"""
authors pipeline
----------------

A pipeline that takes a file of works, and outputs OpenAlex Author details
"""
import boto3
import json
import requests
from metaflow import FlowSpec, S3, step, Parameter
from sqlalchemy import JSON


s3 = boto3.resource("s3")


class OpenAlexAuthorsFlow(FlowSpec):
    production = Parameter("production", help="Run in production?", default=False)

    @step
    def start(self):
        """
        Starts the flow.
        """
        self.next(self.get_authors)

    @step
    def get_authors(self):
        """
        Get a list of relevant authors.
        """
        content_object = s3.Object(
            "open-jobs-lake",
            "metaflow/data/OpenAlexWorksFlow/1654785372748276/openalex_works_production-True.json",
        )
        file_content = content_object.get()["Body"].read().decode("utf-8")
        json_content = json.loads(file_content)
        authors = []
        for work in json_content:
            for author in work["authorships"]:
                authors.append(
                    author["author"]["id"].replace("openalex", "api.openalex")
                )
        self.authors = list(set(authors))
        self.next(self.retrieve_data)

    @step
    def retrieve_data(self):
        """Returns all results of the API hits"""
        if not self.production:
            self.authors = self.authors[:1000]
        self.outputs = []
        for call in self.authors:
            try:
                req = requests.get(call).json()
                self.outputs.append(req)
            except ValueError:
                pass
        self.next(self.end)

    @step
    def end(self):
        """
        Saves the outputs
        """
        filename = f"openalex-authors_production-{self.production}.json"
        with S3(run=self) as s3:  # relevant output location to be updated
            data = json.dumps(self.outputs)
            s3.put(filename, data)


if __name__ == "__main__":
    OpenAlexAuthorsFlow()
