# Patent Data Pipeline

## Running get_ai_genomics

To get patent IDs at the intersection of AI and genomics, we are relying on USPTO AI patent IDs and CPC/IPC classification codes related to genomics.

The script `get_ai_genomics_patents.py` queries bespoke genomics-related patent ID tables (golden-shine-355915.genomics.ipc and golden-shine-355915.genomics.cpc) with USPTO AI patent IDs to identify USPTO AI patent IDs that also have genomics-related patent ids.

The bespoke genomics-related patent ID tables were generated based off of patents in `google-patent-data.publications` that have any genomics-related cpc or ipc codes related to them. These codes were extracted based on exact matching genomics-related keywords in code descriptions.

To run `get_ai_genomics_patents.py`, you must first have a service account credentials .json. To create a service account credentials file, [follow these instructions.](https://cloud.google.com/iam/docs/creating-managing-service-accounts)

Once you have credentials generated, you also need permissions to access the bespoke genomics-related patent ID tables - speak to [India Kerle](mailto:india.kerle@nesta.org.uk) to make sure your email associated to your service count credentials has permissions to query the table.

Finally, before you run the `get_ai_genomics_patents.py` script, you will need to export your credentials path as an global variable every time you want to run the script. To do so, (in your terminal):

`export GOOGLE_APPLICATION_CREDENTIALS="/path/to/credentials.json"`

then:

`python get_ai_genomics_patents.py`

The final output is a series of .csvs with USPTO patent IDs that have been identified as AI-related and that have a genomics cpc or ipc code related to them.

## Quota limits

The script splits AI patent IDs into query chunks because of a) bytes quota limit and b) SQL query length. It outputs each ai-genomics patent id chunk as its separate file.

The script will likely break due to quota limits and will need to be rerun after waiting several hours/once the quota has been replenished. When the script is rerun, it will restart at the index of the last query chunk saved to s3.
