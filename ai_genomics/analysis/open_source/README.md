# Data source: Open source software and AI-related research information (Papers with Code, GitHub)

## Overview

AI research papers often include links to the code developed by the authors. This is done in order to enable reproducibility of results and to encourage the adoption of techniques. By looking at this, we would be able measure what AI + genomics techniques are being implemented and released for others to use, compare the levels of open source activity in different topics and communities as well as the strategies adopted by different organisations and countries.

AI researchers also rely on open `benchmarking` datasets where they can train their data and evaluate their models.

Here we consider two data sources that might be relevant for the analysis of code and data resources in the field of AI and Genomics.

### Papers with Code

[Papers with Code](https://paperswithcode.com/) is a repository of information about AI research. It contains...

- Links to the GitHub repos linked from a paper
- Information about the techniques used in the paper
- Information about the benchmarks used in a paper
- Performance of the paper against standard benchmarks

PwC data is available as a collection of data dumps that can be downloaded [here](https://paperswithcode.com/about).

### GitHub

[GitHub](https://www.github.com) is a widely used platform for collaborative coding. It contains information about...

- Open source software projects
- Organisations and individuals who participate in open source software development
- The development process (e.g. content of contributions and their sequence etc.)

GitHub data can be collected from a [RESTful API](https://docs.github.com/en/rest) and a snapshot of the GitHub corpus is available from [Google BigQuery](https://console.cloud.google.com/bigquery?project=ghtorrent-bq&page=dataset&d=ght&p=ghtorrent-bq&redirect_from_classic=true) (it seems that these data are ca. 5 years old).

We are not planning to collect GitHub data at scale for the project although it might be possible to use its API to extract information about e.g. the popularity of repos associated with different papers / organisations / topics, which we would identify via PwC. Therefore, we focus the rest of the note on PwC rather than GitHub.

## Data provenance and quality

This [Medium blog](https://towardsdatascience.com/papers-with-code-arxiv-reproducible-organized-research-f5404eb6a22e) outlines PwC's methodology. JMG exchanged emails with PwC for a project some time ago. This is what he was told about provenance:

| Question                                                                                                                                                                                  | Answer                                                                                                                                                                                                                                                                         |
| :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| How did you identify AI papers for this table? Is this based on arXiv categories, an analysis of the text in abstracts, submissions to key conferences or a combination of all the above? | These are arXiv papers from AI-related categories (i.e. cs.CV, cs.AI, cs.LG, stat.ML, etc), plus major conferences (acl conferences, neurips, cvpr, iccv, iclr, icml), plus papers submitted by authors (if we missed them), so some ieee/acm but those are not fully covered. |

|How did you extract the links to github repositories?
Do github links only capture repos with code developed for a paper or do they also include links to repos that the authors mentioned/used but perhaps not developed? | We have an NLP-based classifier with the aim of finding 1) official implementations, 2) re-implementations. Official implementations are of high precision (>99%) and recall (>80%), while the re-implementation have lower precision and recall (also because what counts as re-implementation can be defined in various ways).|

The above suggests that PwC's methodology is geared towards capturing specialist machine learning / AI papers rather than applications of AI in other fields. Having said this, we find a number of datasets in the corpus which might be relevant for the project including:

- 218 [medical datasets](https://paperswithcode.com/datasets?mod=medical&page=1)
- 53 [biomedical datasets](https://paperswithcode.com/datasets?mod=biomedical)
- 24 [biological datasets](https://paperswithcode.com/datasets?mod=biology&page=1)

A search for datasets that mention `genomics` in their title or description reveals 30 datasets. We also find 14 datasets that mention `gene`.

A search of papers that mention `genetics` in their title or description reveals < 100-200 suggesting that this dataset has limited coverage of AI and genomics research.

## Content

See above. PwC includes metadata about papers, datasets, open source repositories linked from papers etc.

## Further questions

The coverage of AI genomics research in Papers with Code seems rather limited so I suggest not including it in the project.

Once we have created a corpus of AI / genomics research from e.g. OpenAlex, perhaps we can check how many papers there are present in PwC, the data they use and the code that they develop etc. but this would be a stretch goal.

## Role in the project

NA
