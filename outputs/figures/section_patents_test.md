# Patents

The [Google patent
dataset](https://cloud.google.com/blog/topics/public-datasets/google-patents-public-datasets-connecting-public-paid-and-private-patent-data) contains bibliographic information on more than 90 million patent publications from 17 countries, obtained form IFI CLAIMS Patent Services, which is itself a patents aggregator. Information includes (but is not limited to) the patent number, application number, patent title, patent abstract, assignee (person or organisation) country code,
publication date, ipc and cpc codes and inventor name(s).

The data is available to access via writing BigQuery queries to their data warehouse (although there are limits to downloading data with a free account).

## CPC/IPC codes

The [International Patent Classification
(IPC)](https://www.wipo.int/classifications/ipc/en/) is a system of codes that groups inventions according to different technical areas. Similarly, the [Cooperative Patent Classification
(CPC)](https://www.epo.org/searching-for-patents/helpful-resources/first-time-here/classification/cpc.html)
system serves the same purpose and is an extension of the IPC. Both systems were used as part of a keyword strategy to identify genomics and AI-related codes because a single system is not always used to classify a given patent. To ensure completeness, we identify relevant codes in the descriptions of both classification systems.

## Method for finding relevant genomics and AI codes

A keyword approach to identifying genomics-related CPC and IPC codes was taken. If any of the keywords "genome", "dna", "gene", "genomics" or "genetic" appeared in the description of CPC and IPC codes, the code was deemed to be relevant to "genomics". The list of codes was then manually reviewed and codes that were not relevant to genomics (such as codes related to genetic algorithms) were removed or additional codes added.

This resulted in approximately 360 CPC and 30 IPC codes identified as relevant.

@tbl:genom_codes summarises the most common CPC and IPC codes at the 4-digit level:

| CPC/IPC | 4-Digit Code | Code Description                                                                                                                                                                                  | Code Count |
| ------- | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| CPC     | C12N         | MICROORGANISMS OR ENZYMES; COMPOSITIONS THEREOF; PROPAGATING, PRESERVING, OR MAINTAINING MICROORGANISMS; MUTATION OR GENETIC ENGINEERING; CULTURE MEDIA                                           | 284        |
| CPC     | C12Y         | ENZYMES                                                                                                                                                                                           | 13         |
| CPC     | Y10S         | TECHNICAL SUBJECTS COVERED BY FORMER USPC CROSS-REFERENCE ART COLLECTIONS [XRACs] AND DIGESTS                                                                                                     | 11         |
| IPC     | C12N         | MICROORGANISMS OR ENZYMES; COMPOSITIONS THEREOF; PROPAGATING, PRESERVING, OR MAINTAINING MICROORGANISMS; MUTATION OR GENETIC ENGINEERING; CULTURE MEDIA (microbiological testing media C12Q 1/00) | 18         |
| IPC     | G16B         | BIO-INFORMATICS, c. to d. INFORMATION AND COMMUNICATION TECHNOLOGIES (ICT) SPECIALLY ADAPTED FOR THE PROCESSING OF GENETIC OR PROTEIN DATA IN COMPUTER-BASED MOLECULAR BIOLOGY                    | 5          |
| IPC     | C40B         | COMBINATORIAL CHEMISTRY; LIBRARIES, e.g. CHEMICAL LIBRARIES, IN SILICO LIBRARIES                                                                                                                  | 2          |

: Top CPC / IPC codes about genomics {#tbl:genom_codes}

A similar approach was taken to identify codes related to artificial intelligence. A conservative keyword approach was taken: if the key phrases "machine learning", "artificial intelligence" or "neural network" were in the descriptions of the CPC and IPC codes, the codes were deemed to be relevant to AI. The codes were then also manually reviewed and additional codes were added: this resulted in approximately 55 potential CPC and 35 IPC codes. The most common CPC codes at the 4-digit level therefore include [G06N](https://www.uspto.gov/web/patents/classification/cpc/html/cpc-G06N.html), [G05B](https://www.uspto.gov/web/patents/classification/cpc/html/cpc-G05B.html) and [Y10S](https://www.uspto.gov/web/patents/classification/cpc/html/cpc-Y10S.html). Meanwhile, IPC codes at the 4-digit level include
[G06N](https://www.wipo.int/classifications/ipc/en/ITsupport/Version20190101/transformations/ipc/20190101/en/htm/G06N.htm#G06N),
[G06F](https://www.wipo.int/classifications/ipc/en/ITsupport/Version20190101/transformations/ipc/20190101/en/htm/G06F.htm#G06F) and
[G16B](https://www.wipo.int/classifications/ipc/en/ITsupport/Version20190101/transformations/ipc/20190101/en/htm/G16B.htm#G16B).

Similarly, the codes were manually reviewed although no codes were deemed irrelevant so no pruning occurred.

## Methods for finding AI and genomics patents - CPC/IPC

A given patent was considered related to AI and genomics if it had at least one genomics- related CPC/IPC code AND one AI- related CPC/IPC code associated with it.

## Method for finding AI and genomics patents - USPTO

The United States Patent and Trademark Office (USPTO) have created a dataset called the [Artificial Intelligence Patent Dataset
(AIPD)](https://www.uspto.gov/ip-policy/economic-research/research-datasets/artificial-intelligence-patent-dataset). The paper relating to this work can be found
[here](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3866793).

Each patent in the database has a predicted probability of belonging to subdomains of AI (knowledge processing, speech, AI hardware, evolutionary computation, natural language processing, machine learning, computer vision, planning/control).

We trialled using this database to identify AI related patents instead of using CPC/IPC codes. A patent was then considered relating to AI and genomics if it had at least one genomics related CPC/IPC code AND had a predicted probability greater than a specified threshold in one of the subdomains of AI. We included only the relevant subdomains of AI -- speech, evolutionary computation, natural language processing, machine
learning and computer vision.

## Evaluation

To evaluate the different approaches, we took random samples of 50 patents identified as AI and Genomics patents. We then manually labelled these patents as being relevant to AI, genomics, AI and genomics.

Using CPC/IPC codes to identify both AI and genomics patents found the most relevant patents. Results of the different approaches can be seen in @tbl:pat_evaluation:

| Method for AI patents           | Method for genomics patents     | AI Genomics precision | % Labelled AI Genomics patents from other method | Number of patents found |
| ------------------------------- | ------------------------------- | --------------------- | ------------------------------------------------ | ----------------------- |
| IPC/CPC codes - manual cleaning | IPC/CPC codes - manual cleaning | 0.9                   | 0.42                                             | 13,716                  |
| AIPD (threshold 0.9+)           | IPC/CPC codes                   | 0.67                  |                                                  | 1771                    |
| AIPD (threshold 0.95+)          | IPC/CPC codes                   | 0.52                  |                                                  | 1140                    |

:Patent classification evaluation {#tbl:pat_evaluation}

When manually evaluating the AI in Genomics patents at the intersection of the AIPD for AI patents and the IPC/CPC codes for genomics, the patents appear to be related to genomics but not about using AI. One cause of this could be that these patents sometimes contain words in their abstracts such as neuron/neural, classifier, clustering, data which are words that can relate to AI but in this context relate to genomics only. For example "_...preparing the cholinergic neurons provided in the present invention enables not only production of the cholinergic neurons with high purity, but also rapid production of the cholinergic neurons with the same traits, it can be widely used for effectively treating degenerative cranial nerve diseases..._"

The random samples of patents and our labelling can be found
[here](https://docs.google.com/spreadsheets/d/1XVBKDPLFdG9jry1uYw2izXEyMJ_My6sKdJRXBilYj0o/edit#gid=1087628594).
