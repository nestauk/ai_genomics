# AI and genomics pipeline

## OpenAlex

In order to reproduce the OpenAlex analysis:

Download the FastText language identification model with

```bash
wget --directory-prefix inputs/models/ https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.ftz
```
Download the OpenAlex institutions file from s3 with
`aws s3 cp s3://ai-genomics/inputs/openalex/institutions.json inputs/openalex/`
Run `python ai_genomics/pipeline/make_year_summary.py` to collect and parse the OpenAlex data. The outputs are a collection of csv tables and json objects that will be saved in `inputs/data/openalex`.

Run `python ai_genomics/pipeline/augment_work_metadata.py` to augment the work (article) data with language and abstract presence data.

Run `python ai_genomics/pipeline/fetch_papers_with_code.py` to fetch the Papers with Code data we use to label the OpenAlex data.

Run

```bash
mkdir inputs/data/arxiv
aws s3 cp s3://ai-genomics/inputs/miscellaneous/arxiv_article_categories.csv inputs/data/arxiv/arxiv_article_categories.csv
aws s3 cp s3://ai-genomics/inputs/openalex/concepts.json inputs/openalex/concepts.json

To fetch other article category data we use to label the OpenAlex data.

Run `python ai_genomics/analysis/openalex_definition.py` to implement the definitions and generate results. This includes printing stats in the console and saving relevant tables in `inputs/data`.

## CrunchBase

Run `python ai_genomics/analyss/crunchbase_definitions.py` to test the impact of different definition strategies on the results. Data are fetched from S3 but not stored locally.
