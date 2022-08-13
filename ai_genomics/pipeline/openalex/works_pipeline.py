"""
works pipeline
--------------

A pipeline that takes a list of concept IDs and years, and outputs OpenAlex API results.

The thought behind this is to break the results into manageable yearly chunks. For a given year
and high level concept, the output works may be well over 2GB in size when saved to json.
"""
import itertools
import json
import requests
from metaflow import FlowSpec, S3, step, Parameter, retry, batch

# Amend this to your desired concepts/years. OpenAlex allows up to 50 parameters
# per query, so code is included by default to chunk up the concepts into 40s.
CONCEPT_IDS = [
    "C200594392",
    "C154945302",
    "C54355233"
]

YEARS = [
    2022,
    2021,
]

API_ROOT = "https://api.openalex.org/works?filter="

def generate_queries(concepts, years):
    """Generates a list of queries for the list of concepts and
    years required.

    Args:
        concepts : list of concepts to be queried
        years : list of years to be queried

    Returns:
        query_list : list of all queries
    """
    concepts_joined = "|".join(concepts)
    query_list = []
    for year in years:
        query_list.append(f'{concepts_joined},publication_year:{year}')
    return query_list


def api_generator(api_root: str, concept_ids: list) -> list:
    """Generates a list of all URLs needed to completely collect
    all works relating to the list of concepts.

    Args:
        api_root : root URL of the OpenAlex API
        concept_ids : list of concept IDs to be queried

    Returns:
        all_pages: list of pages required to return all results
    """
    concepts_text = concept_ids
    page_one = f"{api_root}concepts.id:{concepts_text}"
    print(page_one)
    total_results = requests.get(page_one).json()["meta"]["count"]
    print(total_results)
    number_of_pages = -(total_results // -200)  # ceiling division
    all_pages = [
        f"{API_ROOT}concepts.id:{concepts_text}&per-page=200&cursor="
        for i in range(1, number_of_pages + 1)
    ]
    return all_pages

def get_chunks(_list, chunksize):
    """
    Chunks a list.
    """
    chunks = [_list[x : x + chunksize] for x in range(0, len(_list), chunksize)]
    return chunks


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
        # If production, generate all pages
        if self.production:
            concept_list = CONCEPT_IDS
            year_list = YEARS
        else:
            concept_list = CONCEPT_IDS[:1]
            year_list = YEARS[:1]
        # Generate chunks of concepts
        concept_chunks = get_chunks(concept_list, 40) # 40 is the max number of concepts per query
        # Get lists of queries for each chunk of concepts
        output_lists = []
        for chunk in concept_chunks:
            output_lists.append(generate_queries(chunk, year_list))
        # Flatten list of lists
        self.merged = list(itertools.chain.from_iterable(output_lists))
        print(len(self.merged))
        self.next(self.retrieve_data, foreach="merged")

    @retry()
    @batch(cpu=2, memory=32000)
    @step
    def retrieve_data(self):
        """Returns all results of the API hits"""
        # Get list of API calls
        api_call_list = api_generator(API_ROOT, self.input)
        # Get all results
        outputs = []
        cursor = "*"  # cursor iteration required to return >10k results
        for call in api_call_list:
            try: # catch transient errors
                req = requests.get(f"{call}{cursor}").json()
                for result in req["results"]:
                    outputs.append(result)
                cursor = req["meta"]["next_cursor"]
            except:
                pass
        # Define a filename and save to S3
        year = self.input.split(":")[-1] # not ideal for multiple concepts, but works for now
        concept = self.input.split(",")[0]
        filename = f"openalex-works_production-{self.production}_concept-{concept}_year-{year}.json"
        with S3(run=self) as s3:
            data = json.dumps(outputs)
            s3.put(filename, data)
        self.next(self.dummy_join)

    @step
    def dummy_join(self, inputs):
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    OpenAlexWorksFlow()
