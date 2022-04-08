# OpenAlex Investigation

## Overview

This directory houses initial investigation of OpenAlex data.

OpenAlex contains the following tables:

- Works (papers, books, datasets etc., [example](https://api.openalex.org/works/W2741809807))
- Authors (who create works, [example](https://api.openalex.org/authors/A2208157607))
- Venues (journals/repos that contains works, [example](https://api.openalex.org/venues/V1983995261))
- Institutions (orgs/institutions affiliated with a work, [example](https://openalex.org/I114027177))
- Concepts (tags works with topics [example](https://openalex.org/C2778407487))

The main ways of interacting with dataset are via the API, or through database snapshots.

The method of interaction is likely to be based on the size of interaction, as OpenAlex politely ask to limit API hits to under 100,000 per day (which we ideally should adhere to). The DB snapshot is very large (~200GB), so would require some underlying infrastructure, but has no such limits.

## Data Quality

From OpenAlex:

```
Our dataset is still very young, so there's not a lot of systematic research comparing OpenAlex to peer databases like MAG, Scopus, Dimensions, etc. We're currently working on publishing some research like that ourselves. Our initial finding are very encouraging...we believe OpenAlex is already comparable in coverage and accuracy to the more established players--but OpenAlex is 100% open data, built on 100% open-source code. We think that's a really important feature. We will also continue improving the data quality in the days, weeks, months, and years ahead! 
```

Further to the above, there's also some discussion [here](https://bibliometrie.info/downloads/webinarslides/2022_03_25_Open_Data_Open_Science.pdf).

To roughly conclude though, after the retirement of MAG (which OpenAlex is largely based on), this seems to be the best open dataset of this nature available.

## Further Issue Questions

What is its timeliness?

The data is updated every 2 weeks from all its component sources

How will we collect/store it?

Dependant on the size (discussed above), but stored on S3 in (big) CSVs in all likelihood
We may want to move to holding it in a DB at some point with iterative updates, but it feels overboard for this project, and the above would be a good precursor to this if needed in the future.

How will we find AI / genomics papers in it?

Assumedly via `Concepts`, we will probably need to do some NLP analysis of concepts we know, and potentially looking at their 'closest' concepts.
Also, if relevant - MeSH tags exist for works found in PubMed.

How can we enrich institution data with GRID?

Fortunately, instuitutions come with a GRID ID as a variable, so this shouldn't require fuzzy matching.

## Other: Known Issues

OpenAlex have a page detailing their [known issues](), none of which look like 'breaking' issues.
