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

Run `python ai_genomics/analysis/gtr/gtr_cluster_analysis.py` to reproduce the prototype cluster analysis of GtR clusters. Note that this analysis uses the same sampled dataset as reported by JMG. You can run change the `reproduce` parameter to re-run the analysis from scratch. This includes creating vector representations of all sampled projects (which takes around 1hr locally).
