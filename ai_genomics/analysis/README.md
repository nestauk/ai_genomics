# AI and genomics analysis

## Reading the results

You can find individual report sections, figures and a pdf with a rendered version of the report in `outputs/figures`.

Run `bash make_report.sh REPORT_DATE_HERE` to render the report.

## Finding AI Genomics related people and organisations

Output csvs will be saved in the `outputs/data/experts` folder.

### OpenAlex data -- influential researchers

Run `python ai_genomics/analysis/researchers/influential_researchers.py` to generate two csvs of active / influential researchers based on the OpenAlex data.

### Patents data -- top assignees

Run `python ai_genomics/analysis/ai_genomics_experts/patent_assignees.py` to generate csv of the assignees who have been assigned the most AI and genomics patents.

### Crunchbase data -- investors, top most funded companies and related people

Run `python ai_genomics/analysis/ai_genomics_experts/cb_orgs_and_people.py` to generate four csvs of AI and genomics Crunchbase investors, top most funded companies and related people.

### GtR data -- people related to projects

Run `python ai_genomics/analysis/ai_genomics_experts/gtr_people.py` to generate a csv of people that have worked on AI and genomics projects (only projects with people information are included)

## GtR data -- cluster analysis

Run `python ai_genomics/analysis/gtr/gtr_cluster_analysis.py` to reproduce the prototype cluster analysis of GtR clusters. Note that this analysis uses the same sampled dataset as reported by JMG.

You can run change the `reproduce` parameter to re-run the analysis from scratch. This includes creating vector representations of all sampled projects (which takes around 1hr locally).

## Analysis of influence

### If you want to re-run the whole pipeline

Run `python ai_genomics/analysis/influence/make_influence_tables.py` to calculate influence scores for documents in key datasets. This works as follows:

1. Train topic model on corpus including AI genomics, AI, and Genomics research (AI, genomics are sampled to include the same number of articles)
2. Calculate a discipline weight that captures the share of activity in a topic accounted by AI or Genomics e.g. if 80% of the top documents in a topic are in genomics, then it's weight is 0.8 (and the AI weight is 0.2)
3. Calculate overall influence of a discipline (AI or Genomics) in a document by weighting each topic weight in a document by its discipline weight and aggregating them over disciplines
4. Save results

This will save `influence_scores` locally for follow-on analysis.

Run `python ai_genomics/analysis/influence/make_influence_analysis.py` with `get_influence(local=False)` to reproduce the analysis in the report with new influence scores. This includes an analysis of influence scores and an analysis of influence via citations.

### If you want to reproduce the analysis in the report (the default)

Run `python ai_genomics/analysis/influence/make_influence_analysis.py` with `get_influence(local=True)`. This will read the original set of influence scores from s3 and reproduce the analysis with that.

All charts are saved in `outputs/figures/png`.

### If you want to

## All data sources -- Analysis of emergence

Run `python ai_genomics/analysis/integrated_emergence/make_emergence_analysis.py` to perform an emergence analysis of document clusters in the OpenAlex, Patent and GtR data, and to integrate the results across datasets. This works as follows:

1. We calculate document cluster-year frequencies by dataset

2. We calculate the "recency" and "significance" of each cluster in the table.

- Recency captures the % of all activity in a cluster happening in recent years i.e. the extent to which it skews towards the past or the present
- Significance captures the % of activity in all clusters in a cluster i.e the extent to which the cluster is important within the population of clusters

3. We visualise the above in a two by two matrix which identifies different "types" of clusters based on their emergence / significance values
4. We combine these indicators for all data sources and compare their emergence and significance
5. We have also produced a correlation matrix between measures of recency and significance inside / across all datasets.

All charts are saved in `figures/outputs/png`
