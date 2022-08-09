# Patent Data Pipeline

## Getting AI and genomics- related IPC and CPC codes

IPC and CPC classificaion code data comes from two sources. For IPC classification codes, data is downloaded from the [World Intellectual Property Organizaion](https://www.wipo.int/classifications/ipc/en/ITsupport/Version20220101/index.html). For the CPC classification codes, data is downloaded from the [European Patent Office](https://www.epo.org/searching-for-patents/helpful-resources/first-time-here/classification/cpc.html).

Codes are defined as 'genomics' related if genomics keywords are in the descriptions of classification codes. They are then manually verified and 'bad' codes (i.e. codes related to genetic algorithms) are removed or 'good' codes are also added.

Codes are defined as "ai" related if ai keywords are are in the descriptions of classification codes. They are then manually verified and 'good' codes are also added.

To generate AI and genomics- related CPC and IPC codes, run:

`python ai_genomics/pipeline/patent_data/get_ai_genomics_codes.py`

## Getting AI genomics patents

To run `get_ai_genomics_patents.py`, you must first have a service account credentials .json. To create a service account credentials file, [follow these instructions.](https://cloud.google.com/iam/docs/creating-managing-service-accounts)

Before you run the `get_ai_genomics_patents.py` script, you will need to export your credentials path as an global variable every time you want to run the script. To do so, (in your terminal):

`export GOOGLE_APPLICATION_CREDENTIALS="/path/to/credentials.json"`

Alternatively, you could also set these credentials as an environmental variable in `.bashrc`.

Finally, to identify and save AI- and genomics- related patents to s3, run:

`python ai_genomics/pipeline/patent_data/get_ai_genomics_patents.py`

You can pass a the name of the table to query unique ai and genomics patents from. If the table does not exist in bigquery, it will create the table then pull unique ai and genomics patents from it. If you are going to create a table, please create one in `golden-shine-355915.genomics`.

### Quota limits

If you have a free account, you will hit up against quota limits. Simply wait a few hours to re-run `get_ai_genomics_patents.py`
