# AI Genomics expert people and organisations

The python files in this directory, generate csvs of AI genomics related people and organisations from the AI genomics processed data sources. The AI and genomics definitions for the datasets we have used is provisional and could change.

## OpenAlex

We have queried our provisional AI and genomics OpenAlex corpus to identify the top 50 most active researchers in the whole corpus and also focusing on the authors of publications with high year-normalised citations. We only include in our list authors who are affiliated to an UK institution in their most recent publication.

- The AI and genomic corpus we use to generate this list is provisional. It for example includes plant and genomics research which might be less relevant for the project.
- The AI and genomic corpus we are using doesn't include 2022 data. This also means that we might be including non-UK based researchers that changed affiliations in 2022.
- Our corpus includes many multi-author papers. This means that we might be including researchers with peripheral involvement in many papers, and researchers who were not involved in the "AI" component of a genomics paper.
- We have included links to researchers OrcID profiles and OpenAlex links to make it possible to inspect their publications and OpenAlex concepts associated with them.

## Patents

We have used the AI and genomics patents dataset to identify the assignees who are related to the most patent publication numbers.

## Crunchbase people and organisations

We have used the AI and genomics Crunchbase dataset to identify:

- investor organisations
- people related to the investor organisations
- top funded companies
- people related to the top funded companies

Each of these tables has a column `cb_url` which links to the Crunchbase website which can provide additional information about the person or organisation.

## Gateway to Research people

Unfortunately, the AI and genomics Gateway to Research projects we have found do not have GtR organisations relating to them. There is also not many people relating to the projects.
We have therefore prepared a table of the AI and genomics GtR projects with added people information where possible.
