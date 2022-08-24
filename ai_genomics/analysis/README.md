# AI and genomics analysis

## Reading the results

You can find individual report sections, figures and a pdf with a rendered version of the report in `outputs/figures`.

Run `bash make_report.sh REPORT_DATE_HERE` to render the report.

## Finding influential researchers in the OpenAlex data

Run `python ai_genomics/analysis/researchers/influential_researchers.py` to generate two tables of active / influential researchers based on the OpenAlex data. The tables will be saved in the `outputs/data/experts` folder.
