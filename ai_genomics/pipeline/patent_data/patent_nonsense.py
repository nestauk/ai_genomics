# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     comment_magics: true
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.1
#   kernelspec:
#     display_name: ai_genomics
#     language: python
#     name: ai_genomics
# ---

# %%
from ai_genomics import PROJECT_DIR
import glob
import pandas as pd

# %% [markdown]
# potential patent data pipeline defined from technical spec:
#
# 1. Use the USPTO (US Patent and Trademark Office ) Artificial Intelligence Patent Dataset, a database of AI patents filed with the USPTO, to identify AI patents.
# 2. Query the USPTO API to collect additional information about those patents and label as “genomics” those with IPC (International Patent Classification) codes related to genomics or research in the biological sciences.

# %% [markdown]
# ### Patent Data Pipeline

# %% [markdown]
# ### 0. Load parameters and USPTO AI data
#
# The first data file identifies United States (U.S.) patents issued between 1976 and 2020 and pre-grant publications (PGPubs) published through 2020 that contain one or more of several AI technology components (including machine learning, natural language processing, computer vision, speech, knowledge processing, AI hardware, evolutionary computation, and planning and control).
#
# NOTES:
# ZIPPED the data is 1.11 GB...

# %% [markdown]
# ##### 0.0 parameters

# %%
uspto_dir = PROJECT_DIR / "inputs/data/uspto"

# %% [markdown]
# ##### 0.1 data

# %%
uspto_data = pd.read_csv(
    glob.glob(str(uspto_dir) + "/*.tsv")[0], sep="\t"
)  # sounds like the predictions.tsv file is the dataset

# %%
# uspto_patent_ids = uspto_data[uspto_data.flag_patent == 1].doc_id

# %% [markdown]
# ### 1. quick EDA
#
# - What's in the data? any columns of interest? etc.

# %%
uspto_data.shape  # very large - over 13 million rows
uspto_data.doc_id.is_unique  # looks like every row is a unique document id. From the website, sounds like each row refers to either a patent issued between 1976-2020 or a pre-grant publication published through 2020
uspto_data.appl_id.is_unique  # meanwhile, it looks like you can have multiple documents associated to an application id
uspto_data.isnull().sum()  # data is very complete - only 1 missing value in publication_date
uspto_data = uspto_data.dropna()  # drop single na

# %%
uspto_data.pub_dt = pd.to_datetime(uspto_data.pub_dt, errors="coerce")
uspto_data.pub_dt.min(), uspto_data.pub_dt.max()  # dates are between 1970 and 2020


# %%
# looks like there is a flag_patent column so we can subset the data for JUST patents, not pre-grant publications
uspto_data.flag_patent.value_counts()  # looks like its half patent, half not patent
uspto_patents = uspto_data[uspto_data.flag_patent == 1]

# %% [markdown]
# looks like you've got: 1) a document id, a unique identifier per doc, 2) a binary patent flag to flag whether the document refers to a patent or not, 3) pub_dt, the date of document publication between 1970 and 2020, then looks like you've got a series of flags PER AI area (predict50_, flag_train_, ai_score) across machine learning, nlp, computer vision, speech, knowledge processing, AI hardware, evolutionary computation and planning and control.
#
# Oustanding qs:
# - what does the ai_score refer to? what's this predict50 business? looks like its a flag also.

# %% [markdown]
# ### 2. Identifiy "genomics" classifications
# - there are a number of different classification systems: CPC, USPC, IPC - query classifications for "genomics, "genome, genomic"

# %% [markdown]
# **From Harry:** New, emerging and predicted capabilities to analyse and understand the human genome (genomic analysis) (we are less interested in capabilities to edit the genome (genomic editing), or to analyse the genome of non-human lifeforms (except where this has direct relevance to genomic analysis in humans)
# We are interested in the capability of AI to improve of the upstream requirements for genomics, as well as some of the downstream applications, such as the use of NLP to interpret phenotype data and to assist genetic counseling, respectively. However, it would be good to have a clear distinction between these sorts of applications (and trends relating to them) and applications and trends relating to more the more direct effects of AI on the capabilities of genomics science.
# The development of genomic analysis capabilities in medical research, and in non-healthcare research settings.
# The deployment and application of genomic analysis capabilities in both clinical, healthcare and non-healthcare settings.
#

# %% [markdown]
# ### 3. Query API
# - either to determine if an AI patent is also related to genomics OR to subset for AI genomics patents
# - looks like there are multiple options for USPTO APIs - need to determine which one is best
# - UPDATE: looks like PatentsView is the API to use here
# but the API is deeply throttled: Usage Limits
# Each application is allowed to make 45 requests/minute. If your application exceeds this limit, you will receive a “429 Too many Requests” response to your API query. See Swagger Documentation for details.
# The current version of the PatentsView API delivers data on patents granted through September 30, 2021.
# - what about google patents?

# %%
# so...if we used PatentsView....:
mins = round(len(uspto_patents) / 45)
days = round(mins / 1440)
print(
    f"it would approx. take...{mins} minutes...or {days} days...if each request was a single patent id"
)

# what about google patent search? they have all uspto from 1970 according to website

# %% [markdown]
# ## OTHER OPTION: query google's patent database?

# %%
from google.cloud import bigquery
from bq_helper import BigQueryHelper
import os

if not "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
    os.environ[
        "GOOGLE_APPLICATION_CREDENTIALS"
    ] = "/Users/india.kerlenesta/Projects/ai_genomics/ai_genomics/analysis/data_scoping/patent_data/patents-353409-a259d201c505.json"

client = bigquery.Client()

cpc_codes = BigQueryHelper(active_project="patents-public-data", dataset_name="cpc")
cpc_fields = list(cpc_codes.table_schema("definition")["name"])

# %%
cpc_fields

# %% [markdown]
# #### bigquery and bigqueryhelper nonsense

# %%
unnested_fields = ", ".join(
    [
        field.split(".")[0] + "__u." + field.split(".")[1]
        for field in cpc_fields
        if "target" in field or "description" in field
    ]
)

# %%
unnest = ", ".join(
    list(
        set(
            [
                f"UNNEST({field.split('.')[0].replace('__u', '')}) {field.split('.')[0]}"
                for field in unnested_fields.split(", ")
            ]
        )
    )
)

# %%
query1 = f"SELECT {unnested_fields} FROM patents-public-data.cpc.definition_202201, {unnest} LIMIT 10;"
query_job = pd.read_gbq(query1)
query_job

# %%
keywords = ["genomics", "genes", "ploughs"]
for keyword in keywords:
    print(
        f"if(SEARCH(childGroups__u.description, '{keyword}') = TRUE)  as {keyword + '_flag'}"
    )

# %%
genomics_keywords = keywords
genomics_keywords_query = " \n ".join(
    [
        f"SEARCH(childGroups__u.description, '{keyword}') as {keyword + '_flag'},"
        for keyword in genomics_keywords
    ]
)
genomics_cpc_query = f"SELECT childGroups__u, {genomics_keywords_query} FROM `patents-public-data.cpc.definition_202201`, UNNEST(childGroups) childGroups__u LIMIT 10;"

query_job = client.query(genomics_cpc_query)


# %%
query1 = """
SELECT
  childGroups__u
FROM
  `patents-public-data.cpc.definition_202201`,
  UNNEST(childGroups) childGroups__u
LIMIT
    20;
"""

query_job = client.query(query1)

# %%
features = ", ".join(["publication_number", "application_number", "family_id"])
cpc_codes = ", ".join([C12, C11])

# %%
cpc_query = f"SELECT {features} FROM patents-public-data.patents.publications, UNNEST(cpc) AS cpc__u WHERE cpc__u.code IN ({cpc_codes}) LIMIT 10"

# %%
# Construct a BigQuery client object.

query = """
    SELECT 
  publication_number, 
  application_number,
  description_localized,
  publication_date,
  filing_date,
  priority_date,
  priority_claim,
  cpc__u.code,
  inventor,
  assignee_harmonized,
  assignee
FROM
  `patents-public-data.patents.publications`,
  UNNEST(assignee_harmonized) AS assignee_harmonized__u,
  UNNEST(cpc) AS cpc__u
WHERE
  cpc__u.code LIKE ("C12%") 
LIMIT
    10
"""
query_job = client.query(query)  # Make an API request.
