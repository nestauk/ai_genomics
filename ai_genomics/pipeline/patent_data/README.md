# Patent Data Pipeline

## Getting AI and genomics- related IPC and CPC codes

IPC and CPC classificaion code data comes from two sources. For IPC classification codes, data is downloaded from the [World Intellectual Property Organizaion](https://www.wipo.int/classifications/ipc/en/ITsupport/Version20220101/index.html). For the CPC classification codes, data is downloaded from the [European Patent Office](https://www.epo.org/searching-for-patents/helpful-resources/first-time-here/classification/cpc.html).

Codes are defined as 'genomics' related if genomics keywords are in the descriptions of classification codes. They are then manually verified and 'bad' codes (i.e. codes related to genetic algorithms) are removed or 'good' codes are also added.

Codes are defined as "ai" related if ai keywords are are in the descriptions of classification codes. They are then manually verified and 'good' codes are also added.

To generate AI and genomics related CPC and IPC codes, run:

`python ai_genomics/pipeline/patent_data/get_ai_genomics_codes.py`

## Getting AI genomics patents

To run `get_ai_genomics_patents.py`, you must first have a service account credentials .json. To create a service account credentials file, [follow these instructions.](https://cloud.google.com/iam/docs/creating-managing-service-accounts)

Before you run the `get_ai_genomics_patents.py` script, you will need to export your credentials path as an global variable every time you want to run the script. To do so, (in your terminal):

`export GOOGLE_APPLICATION_CREDENTIALS="/path/to/credentials.json"`

Alternatively, you could also set these credentials as an environmental variable in `.bashrc`. Your credentials will need specific permissions to pull data from BigQuery and you will get a permissions error should you not have access. If not, reach out to India Kerle to be granted table access.

To identify and save AI and genomics related patents to s3, run:

`python ai_genomics/pipeline/patent_data/get_ai_genomics_patents.py`

This will get unique patents from: 1. ai_genomics: the AI and genomics table; 2. ai_sample: A sample of 10% of patents with AI codes; 3. genomics_sample: A sample of 3% of patents with genomics codes;

in BigQuery and save them to s3.

These tables were first created and verified for data download limitations in BigQuery's front end. Functions to generate the queries used to generate these tables are in the script.

Finally, to add the full list of CPC and IPC codes associated to a publication number, run:

`python ai_genomics/pipeline/patent_data/add_full_cpc_ipc_codes.py`

This will add the full list of CPC and IPC codes associated to a patent document across AI genomics, a sample of AI and a sample of genomics related patents. These updated tables are also to s3.
