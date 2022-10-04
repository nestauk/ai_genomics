# AI and genomics pipeline

## OpenAlex

In order to reproduce the OpenAlex analysis:

Download the FastText language identification model with

```bash
wget --directory-prefix inputs/models/ https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.ftz
```

Download the OpenAlex institutions file from s3 with
`aws s3 cp s3://ai-genomics/inputs/openalex/institutions.json inputs/openalex/`

Run `python ai_genomics/pipeline/make_year_summary.py` to collect and parse the OpenAlex data. The outputs are a collection of csv tables and json objects that will be saved in `inputs/data/openalex`. Note, this step takes quite a long time (4+ hours on an M1 mac).

Run `python ai_genomics/pipeline/augment_work_metadata.py` to augment the work (article) data with language and abstract presence data.

Run `python ai_genomics/pipeline/fetch_papers_with_code.py` to fetch the Papers with Code data we use to label the OpenAlex data.

Run

```bash
mkdir inputs/data/arxiv
aws s3 cp s3://ai-genomics/inputs/miscellaneous/arxiv_article_categories.csv inputs/data/arxiv/arxiv_article_categories.csv
aws s3 cp s3://ai-genomics/inputs/openalex/concepts.json inputs/openalex/concepts.json
```

To fetch other article category data we use to label the OpenAlex data.

Run `python ai_genomics/analysis/openalex_definition.py` to implement the definitions and generate results. This includes printing stats in the console and saving relevant tables in `inputs/data`. This will also save figures to `outputs/figures`, the provisional ai genomics OpenAlex dataset to `outputs/ai_genomics_provisional_dataset.csv` and examples from the dataset to `outputs/openalex_examples.md`.

The AI and genomics OpenAlex works can be loaded using `ai_genomics.getters.openalex.get_openalex_ai_genomics_works`.

## CrunchBase

Run `python ai_genomics/analysis/crunchbase_definitions.py` to test the impact of different definition strategies on the results. Data are fetched from S3 but not stored locally.

The AI and genomics Crunchbase organisation ids can be loaded using `ai_genomics.getters.crunchbase.get_ai_genomics_crunchbase_org_ids`.

Run `python ai_genomics/pipeline/crunchbase_data/make_crunchbase_data.py` to produce a table with CrunchBase AI + genomics companies and AI / genomics baselines.

You can get the data locally or from S3 (if e.g. you haven't run the script above) using `ai_genomics.getters.crunchbase.get_ai_genomics_orgs`

## Gateway to Research

Run `python ai_genomics/analysis/gtr_definitions.py` to print a summary of AI / genomics projects in the GtR data. This also saves a list of project in the intersection of AI and genomics in `inputs/data/gtr/gtr_ai_genomics_projects.json`.

The AI and genomics GtR projects and publications can be loaded using `ai_genomics.getters.gtr.get_ai_genomics_gtr_data`.

## Validation

This pipeline produces samples of data for expert validation. Specifically, it produces samples of patent and publication abstracts and the OpenAlex concepts and CPC patent codes used to generate those datasets. The output samples vary in format. The samples for patent and publication abstracts are partially overlapping, such that each validation contributor assesses a small sample of identical abstracts and a larger number of different ones. This is designed to maximise the breadth of coverage, while gathering information to calculate inter-contributor agreement. The CPC patent code samples are also overlapping samples but the OpenAlex concepts are provided in full as there are so few.

First, run `python ai_genomics/pipeline/patent_data/cpc_lookup.py` to create a lookup of CPC codes, including information about their parent codes.

Next, run `python ai_genomics/pipeline/validation/abstracts.py` and `python ai_genomics/pipeline/validation/definitions.py` to produce the validation outputs. These must then be formatted in a spreadsheet for subject matter expert contributions.
