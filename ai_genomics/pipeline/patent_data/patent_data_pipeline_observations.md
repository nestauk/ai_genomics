#### Potential Patent Data Pipeline: Steps

1. To get AI patents from USPTO, the data is in s3 in ai-genomics/inputs/patent_data/uspto/ai_model_predictions.tsv.zip. The data itself is very large (over 13 million rows). You can subset the data to only include patents, and not pre-grant publications by only including rows with flag_patent == 1. The only helpful field here is doc_id where you can get the USPTO patent id.

2. To investigate whether the AI patent ID is also about genomics, enrich the data with information about which classification the patent ID falls within. There are a number of different classification systems that would be relevant for us - the Cooperative Patent Classification (CPC), International Patent Classification (IPC) or USPC. To enrich the data, I used Google's patents data - you can query their database using BigQuery. You will need to connect to Google BigQuery and then query the data - here's a simple tutorial to do so: https://www.rudderstack.com/guides/how-to-access-and-query-your-bigquery-data-using-python-and-r/

3. You could either: 1) query the database based on the AI patent IDs to enrich the data with classification codes (CPC, IPC, USPC) and then search the classification code descriptions to see if they contain keywords like "genome, genetics, genomics" OR you could scrap USPTO's patent IDs and just extract patents that fall within classifications for BOTH AI and genomics (i.e. have multiple classifications pertaining to both spaces). I'd just examine both.

I spoke with Edward who used patsnap analytics to identify some Genomics and AI related classification codes that would be relevant for us. I've uploaded them to our shared drive in the Docs folder (file is called cpc_codes_ai_genomics.xlsx). I'd save the outputs to s3 - we've created a bucket ai-genomics.

RISKS, ASSUMPTIONS AND OUTSTANDING QS

- We can make big query calls willy nilly
- The AI patents from USPTO are in Google Patent Data and have IDs we can match on
- What patent data fields do we want to extract?
- We don't capture AI + genomics patent data from the top-down approach or from the bottom-up approach
