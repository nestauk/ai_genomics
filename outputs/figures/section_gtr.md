# Gateway to Research

The Gateway to Research (GtR) is a database with information about UKRI (UK Research Council + Innovate UK) funded projects since around 2006. The database includes information about:

- The projects including starting and ending data, lead funder, amount of funding provided, title and abstract
- The organisations and individuals who participated in the projects
- The outputs from the projects including, among other things, publications, patents, products and databases, and software.

We are in the processing of collecting the most up-to-date version of the GtR database through their API. At this points this includes information about 67,180 projects, 28,845 organisations and 383,758 publications linked to GtR projects. We estimate that there are still another 10,000 projects to be collected.

## Topic search

There are two potential strategies to identify relevant projects in the GtR data: to analyse the topics with which these projects have been labelled and to look for relevant terms in their abstracts. We have implemented both of them.

### Identifying relevant topics

There are 764 topics in the GtR data. We have identified those that could be relevant for the project usign a simple string search (see @tbl:topics for their distribution).

| Topic                         | Number of projects |
| :---------------------------- | :----------------- |
| Genetics & development        | 615                |
| Artificial Intelligence       | 350                |
| Population Genetics/Evolution | 190                |
| Genomics                      | 146                |
| Environmental Genomics        | 97                 |
| Epigenetics                   | 85                 |
| Functional genomics           | 81                 |
| Genome organisation           | 63                 |

: Topic distribution in relevant topics {#tbl:topics}

Having done this, we have extracted all projects that have been labelled with this topics. This yields a total of 350 projects labelled with the Artificial Intelligence topic and 343 projects labelled with genomics / genetics related topics. We only find 6 projects in the intersection of both categories.

### Abstract search

We also search for terms related to AI and genomics using a similar list of terms to what we reported in @tbl:ex. This strategy yields a much larger number of relevant projects than the topics-based approach: we find almost 4,000 projects mentioning AI-related terms in their abstracts, and 2,182 projects mentioning genomics related terms.

### Combined results

We tag as "AI and genomics" any projects that have been tagged as AI via concepts or abstracts, and as genomics via concepts or abstracts. This results in a final list of 117 projects. We present five random examples in @tbl:gtr_examples.

| Project title                                                                                                                                 | Abstract (truncated)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| :-------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Deep Learning for Behavioural Genomics                                                                                                        | Microbiota present a promising and underexploited potential source of novel neuroactive compounds for the treatment of neurological and psychiatric disease in humans. However, due to their diverse and complex interactions with the host nervous system, identifying causal strains and the molecules they produce remains challenging. Here, I address this challenge in the nematode worm, Caenorhabditis elegans, as it is a simple bacterivore with a small nervous system, yet displays a variety of bacteria-influenced behaviours that are observable in the laboratory and governed by conserved neural signalling pathways. I perform phenotypic screening and animal tracking to investigate the behavioural re... |
| Deriving an actionable patient phenome from healthcare data                                                                                   | Translating routinely collected health data into knowledge is a requirement of a &quot;learning health system&quot;. Since joining the Biomedical Research Centre at the South London and Maudsley Hospital, Kings College London, my research has been focused on developing 'CogStack and SemEHR'. This is an integrated health informatics platform which aims to to unlock unstructured health records and assist in clinical decision making and research. The system does much to surface the deep data within the NHS, for example through providing a patient-centric search on semantically annotated clinical notes to support studies such as the recruitment of patients for Genomics England's 100,000 Genomes ... |
| Improving The Longevity Of New Infectious Disease Therapeutics Using Machine Learning / Artificial Intelligence In Early Stage Drug Discovery | Drugs against diseases caused by viruses, bacteria and parasites have transformed human health and saved millions of lives. Nevertheless, their widespread use and misuse has led to the emergence of antimicrobial resistance (AMR) that poses a potentially catastrophic threat to public health. The increasing power of genomic sequencing is offering new ways to rapidly detect and respond to the development of antimicrobial resistance. The availability of this wealth of data, along with the latest developments in artificial intelligence / machine learning (AI/ML) techniques, allows the development of sophisticated approaches that can fully leverage this data to pre-empt the effects of potential re... |
| Investigating RNA regulation during embryonic and germline development in zebrafish                                                           | This MRC-funded doctoral training partnership (DTP) brings together cutting-edge molecular and analytical sciences with innovative computational approaches in data analysis to enable students to address hypothesis-led biomedical research questions. This is a 4-year programme whose first year involves a series of taught modules and two laboratory-based research projects that lead to an MSc in Interdisciplinary Biomedical Research. The first two terms consist of a selection of taught modules that allow students to gain a solid grounding in multidisciplinary science. Students also attend a series of masterclasses led by academic and industry experts in areas of molecular, cellular and tissue dy... |
| Artificial intelligence applied to blood cancer diagnosis                                                                                     | The project will apply artificial intelligence to the diagnosis of blood cancers, principally non-Hodgkin lymphomas, which are cancers that cause substantial morbidity and early loss of life across the population. It is a collaboration between a University research group working in data science and artificial intelligence and the haematological diagnostic laboratory at St James University Hospital. It will employ genome sequence, gene expression and pathology image data for a substantial retrospective patient cohort to tackle a range of important diagnostic problems, including the transformation of indolent cancers to aggressive form, discovery of sub-groups where standard therapy fails and ... |

: Random examples of AI and genomics projects in GtR {#tbl:examples}

The linked nature of the GtR data also makes it possible for us to extract information about the publications linked to these projects. We find 349 publications from the 117 AI and genomics projects in the data - it might be possible to use the abstracts in these outputs to compare the thematic focus of UKRI-funded AI genomics research with our wider corpus.
