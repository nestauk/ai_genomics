# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     comment_magics: true
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.8
#   kernelspec:
#     display_name: ai_genomics
#     language: python
#     name: ai_genomics
# ---

# %% [markdown]
# # Querying Google Patents
#
# **Goal:** Query Google Patents with patent IDs from USPTO for documents that have been tagged as relating to AI and extract their patent codes.
#
# **Objectives**
# - Investigate whether we can retrieve a sufficient coverage of codes
# - Determine the rate or data limit for queries

# %%
from ai_genomics import PROJECT_DIR
import glob
import matplotlib.pyplot as plt
import pandas as pd

# %% [markdown]
# ## Load Data

# %% [markdown]
# ### USPTO

# %%
USPTO_DIR = PROJECT_DIR / "inputs/patent_data/uspto"

# %%
uspto_sample = pd.read_csv(USPTO_DIR / "ai_model_predictions.tsv", sep="\t", nrows=50)

# %%
uspto_sample.columns

# %%
uspto_sample.head(20)

# %% [markdown]
# ### Google Patents

# %%
from google.cloud import bigquery
from google.oauth2 import service_account

# %%
credentials = service_account.Credentials.from_service_account_file(
    PROJECT_DIR / "credentials/ai-genomics.json"
)

project_id = "ai-genomics"

client = bigquery.Client(credentials=credentials, project=project_id)

# %% [markdown]
# #### Tables

# %%
dataset_id = "patents-public-data.patents"
tables = client.list_tables(dataset_id)

print([t.full_table_id for t in tables])

# %% [markdown]
# #### Schema

# %%
q = """
SELECT column_name, is_nullable
FROM `patents-public-data.patents.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name='publications'
"""

client.query(q).to_dataframe()

# %% [markdown]
# #### Data Sample

# %%
q = (
    f"SELECT {', '.join(fields)} "
    "FROM `patents-public-data.patents.publications` "
    "ORDER BY RAND() "
    "LIMIT 1000"
)

gp_sample_df = client.query(q).to_dataframe()

# %%
gp_sample_df.head()

# %% [markdown]
# - Patents have international coverage
# - Coverage is not full or the same for the different patent codes

# %%
fig, ax = plt.subplots(figsize=(10, 5))
ax = gp_sample_df["application_number"].str[:2].value_counts().plot.bar()

# %%
for code in ["cpc", "uspc", "ipc"]:
    missing = (gp_sample_df[code].apply(len) == 0).sum() / gp_sample_df.shape[0]
    print(f"Proportion of {code} missing: {missing}")

# %% [markdown]
# - Using USPC looks pointless, CPC ok and IPC very good.

# %% [markdown]
# ## Querying Patents

# %% [markdown]
# ### Basic Query
#
# In USPTO, the application numbers are digits only, whereas Google Patents has a country prefix and a suffix (not sure what this is atm).
#
# 1. Take a chunk of application IDS from USPTO data
# 2. Query patent data by:
#     1. Using regex to extract core digit from Google Patents application number
#     2. Joining USPTO IDs chunk for WHERE query

# %%
# %%time

fields = ["publication_number", "application_number", "cpc", "uspc", "ipc"]
ids = "'" + "', '".join([str(i) for i in uspto_sample["appl_id"]]) + "'"

q = (
    f"SELECT {', '.join(fields)} "
    "FROM `patents-public-data.patents.publications` "
    f"WHERE REGEXP_EXTRACT(application_number, r'[0-9]+') IN ({ids})"
)

appl_ids_chunk = client.query(q).to_dataframe()

# %%
appl_ids_chunk

# %% [markdown]
# - Patents from different countries share the same core application number
#     - Are these the same patent but in different jurisdictions? (it doesn't look like it from the associated patent codes)
#     - Are the patents in the USPTO data US only?
#     - Do we want to search internationally?

# %% [markdown]
# ## Next Steps
#
# ### Data Limiting
#
# - Query Google Patents with an increasingly large chunk of USPTO application numbers (might need to sleep in between queries)
# - Also measure response times to establish time to complete whole task
#
# ### Rate Limiting
#
# - Once largest possible query is established, see how frequently it can be pulled.
#
# **Notes:**
#
# - Could sort of combine the two pieces above by e.g. querying for each chunk size 5 times in succession.
# - Can we just query by patent code? Might be tricky as they are in a list?
#

# %%
