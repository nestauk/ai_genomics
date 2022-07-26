# Patent Data Pipeline

## Getting genomics-related IPC and CPC codes

IPC and CPC classificaion code data comes from two sources. For IPC classification codes, data is downloaded from the [World Intellectual Property Organizaion](https://www.wipo.int/classifications/ipc/en/ITsupport/Version20220101/index.html). For the CPC classification codes, data is downloaded from the [European Patent Office](https://www.epo.org/searching-for-patents/helpful-resources/first-time-here/classification/cpc.html#:~:text=The%20Cooperative%20Patent%20Classification%20(CPC,%2C%20groups%20and%20sub%2Dgroups.).

Codes are defined as 'genomics' related if genomics keywords are in the descriptions of classification codes.

## Running get_ai_genomics

To get patent IDs at the intersection of AI and genomics, we rely on USPTO AI patent IDs and CPC/IPC classification codes related to genomics.

The USPTO AI patents dataset was constructed using machine learning models for each of eight AI component technologies covering areas such as natural language processing, AI hardware, and machine learning.  

The script `get_ai_genomics_patents.py` queries bespoke genomics-related patent ID tables (golden-shine-355915.genomics.ipc and golden-shine-355915.genomics.cpc) with USPTO AI patent IDs to identify USPTO AI patent IDs that also have genomics-related cpc or ipc codes.

The bespoke genomics-related patent ID tables were generated based off of patents in `google-patent-data.publications` that have any genomics-related cpc or ipc codes related to them. These codes were extracted based on exact matching genomics-related keywords in classification code descriptions.

To run `get_ai_genomics_patents.py`, you must first have a service account credentials .json. To create a service account credentials file, [follow these instructions.](https://cloud.google.com/iam/docs/creating-managing-service-accounts)

Once you have credentials generated, you also need permissions to access the bespoke genomics-related patent ID tables - speak to [India Kerle](mailto:india.kerle@nesta.org.uk) to make sure your email associated to your service account credentials has permissions to query the table.

Finally, before you run the `get_ai_genomics_patents.py` script, you will need to export your credentials path as an global variable every time you want to run the script. To do so, (in your terminal):

`export GOOGLE_APPLICATION_CREDENTIALS="/path/to/credentials.json"`

Alternatively, you could also set these credentials as an environmental variable in `.bashrc`.  

then:

`python ai_genomics/pipeline/patent_data/get_ai_genomics_patents.py`

The final output is a series of .csvs with USPTO patent IDs that have been identified as AI-related and that have a genomics cpc or ipc code related to them.

### Quota limits

The script splits AI patent IDs into query chunks because of a) bytes quota limit and b) SQL query length. It outputs each ai-genomics patent id chunk as its own separate file. If the query chunk is too long, the script will break and you will need to change the n_chunk int in config. If querying breaks due to quota limits, wait several hours to rerun. When the script is rerun, it will restart at the index of the last query chunk of n chunksize saved to s3.
