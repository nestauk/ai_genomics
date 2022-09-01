# Data source: Text Enrichment

## Overview

We would like to measure the purpose / goal of AI + genomics research. This would for example help us identify the disease areas that researchers focus on and the disease areas that they (perhaps) neglect as well as how this focus has evolved over time, and differences between countries and organisation types.

In order to do this, we can leverage a data pipeline developed by Luca Bonavita and Sam Doogan which annotates text documents (e.g. abstracts) with their [DBPedia entities](https://www.dbpedia.org/resources/ontology/) and the level of confidence. Some of these entities are linked to Medical Subject Headign ([MeSH](https://www.nlm.nih.gov/mesh/meshhome.html)), an ontology developed by the US National Institute of Health to label biomedical research.

## Data provenance and quality

This [page](https://www.dbpedia-spotlight.org/publications) contains links to publications from the DBPedia team about the methods they use for entity extraction.

Luca and Sam have noticed that some DBPedia terms do not seem to have MeSH terms, and have highlighted the risk of MeSH mislabelling if e.g. the id for a MeSH term in DBPedia goes out of data. Given this, it might be desirable to use DBPedia entities as our primary ontology. We could use specific branches of the DBPedia ontology such as e.g. [disease](https://www.wikidata.org/wiki/Q12136) to focus our annotation and analysis on policy relevant categories.

## Content

Varies depending on the type of entity

## Further questions

- Do we use DBPedia or MeSH terms?
- What categories do we focus on?
- I noticed that some DBPedia terms are linked to [OpenAlex IDs](https://www.wikidata.org/wiki/Property:P10283). What are the relationships between then / which one do we use?
- What does the pipeline look like? How long does it take to run for corpora of different sizes?

## Role in the project

- Enriching documents (e.g. article abstract, company descriptions) with homogenised terms so that we can answer queries such as:
  - In what disease areas is AI + genomics overrepresented and in which is it underrepresented?
  - In what disease areas do companies doing AI genomics focus? What are the differences between countries?
- We also discussed the idea of using spotlight to annotate institutions with their DBPedia entities (e.g. [Max Planck Society](https://dbpedia.org/page/Max_Planck_Society) although this migth not be necessary because OpenAlex provides GRID id for its institutions
