"""
works pipeline
--------------

A pipeline that takes a list of concept IDs, and outputs OpenAlex API results
"""
import json
import requests
from metaflow import FlowSpec, S3, step, Parameter, batch

CONCEPT_IDS = [
    "C189206191",  # Genome
    "C54355233",  # Genetics
    "C104317684",  # Gene
]
API_ROOT = "https://api.openalex.org/works?filter="


def api_generator(api_root: str, concept_ids: list) -> list:
    """Generates a list of all URLs needed to completely collect
    all works relating to the list of concepts.

    Args:
        api_root : root URL of the OpenAlex API
        concept_ids : list of concept IDs to be queried

    Returns:
        all_pages: list of pages required to return all results
    """
    concepts_text = "|".join(concept_ids)
    page_one = f"{api_root}concepts.id:{concepts_text}"
    total_results = requests.get(page_one).json()["meta"]["count"]
    number_of_pages = -(total_results // -200)  # ceiling division
    all_pages = [
        f"{API_ROOT}concepts.id:{concepts_text}&per-page=200&cursor="
        for i in range(1, number_of_pages + 1)
    ]
    return all_pages


class OpenAlexWorksFlow(FlowSpec):
    production = Parameter("production", help="Run in production?", default=False)

    @step
    def start(self):
        """
        Starts the flow.
        """
        self.next(self.generate_api_calls)

    @step
    def generate_api_calls(self):
        """Generates all API calls, if test, just one page"""
        if not self.production:
            self.api_call_list = [
                f"{API_ROOT}concepts.id:{CONCEPT_IDS[0]}&per-page=200&page=1"
            ]
        else:
            self.api_call_list = api_generator(API_ROOT, CONCEPT_IDS)
        self.next(self.retrieve_data)

    @batch(cpu=2, memory=48000)
    @step
    def retrieve_data(self):
        """Returns all results of the API hits"""
        self.outputs = []
        cursor = "*"  # cursor iteration required to return >10k rsults
        for call in self.api_call_list:
            try:
                req = requests.get(f"{call}{cursor}").json()
                for result in req["results"]:
                    self.outputs.append(result)
                cursor = req["meta"]["next_cursor"]
            except:
                pass
        print(len(self.outputs))
        self.next(self.end)

    @step
    def end(self):
        """Saves the outputs"""
        filename = f"openalex-works_production-{self.production}.json"
        with S3(run=self) as s3:  # relevant output location to be updated
            data = json.dumps(self.outputs)
            s3.put(filename, data)


if __name__ == "__main__":
    OpenAlexWorksFlow()
