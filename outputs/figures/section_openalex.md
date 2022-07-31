# Research data: OpenAlex

## Motivation

We include open research activities in our analysis of AI in genomics for three reasons:

1. Research (in particular preprints) is likely to provide a timelier indicator of current and emerging trends in AI genomics than laggy patent and business activity data.
2. Earlier-stage, emerging trends that might be relevant in the future are more likely to be present in research data than other sources that tell us about the techniques that are being applied today.
3. There are strong traditions of open publishing in genomics and AI research

We recognise that publication data would, on its own, provide a skewed view of the situation and evolution of AI genomics R&D (e.g. not helping to distinguish between theoretical and applied activities, and missing R&D activities that businesses choose not to publish for commercial reasons). We will address these gaps by incorporating patent and business data into the project.

#### Collection

We use [OpenAlex](https://www.openalex.org) as our core research data source. OpenAlex is an open scientometric database developed to replace Microsoft Academic Graph, a database of academic publications that was recently discontinued. OpenAlex includes information about:

- Works (papers, books, datasets etc., [example](https://api.openalex.org/works/W2741809807))
- Authors (who create works, [example](https://api.openalex.org/authors/A2208157607))
- Venues (journals/repos that contains works, [example](https://api.openalex.org/venues/V1983995261))
- Institutions (organisations/institutions affiliated with a work, [example](https://openalex.org/I114027177))
- Concepts (tags works with topics based on a machine learning analysis of the MAG corpus [example](https://openalex.org/C2778407487))[^1]

[^1]: See [here](https://docs.google.com/document/d/1OgXSLriHO3Ekz0OYoaoP_h0sPcuvV4EqX7VgLLblKe4/edit) for additional information about OpenAlex' tagging methodology.

OpenAlex is a new database but current discussions suggest that it is the best open dataset available, and that it's coverage and accuracy is already comparable to established players such as Scopus or Dimensions.

We have collected all the OpenAlex `works` (papers) tagged with the concepts "Artificial intelligence" (around 3 million documents) and "Genetics" (453K documents) and filtered them further through the steps that we describe below:

## Generic processing

We have removed from our corpora all works missing an abstract and applied the [FastText language classifier](https://fasttext.cc/docs/en/language-identification.html) to titles in order to identify those in English, and remove the rest. This leaves us with 1.33 million AI articles and 315K Genetics articles.

## Artificial intelligence corpus processing

A visual inspection of a subset of the AI corpus suggests that there is a non-trivial number of false positives in the data. In particular, we have noticed a number of systematic misclassifications of papers in the following topics:

- Education: This includes papers about learning and learners, which get confused with machine learning.
- Neuroscience: This includes papers about biological neural networks, which get confused with artificial neural networks used in AI research.
- Telecommunications: This includes papers about telecommunications and ICT networks that get confused with artificial neural networks and deep networks used in AI research.
- Linguistics: This includes papers about language, translation etc., which get confused with computational linguistics and natural language papers.

We have adopted two strategies to remove false positives from the corpus: First, we have implemented a heuristic filter that removes from the corpus any papers that include terms related to the categories above such as "learning", "neural network" or "language" but no terms related to machine learning and artificial intelligence (see @tbl:terms).

| Terms                                                                                                                                                                                                                                                                                                                                                                                |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| accuracy, ai, ann, artificial, artificial intelligence, bayes, classifier, clustering, convolution, deep, dnn, encoder, federat, gan, generative, gnn, machine, machine learning, natural language processing, nlp, pattern recognition, predict, rbf, reinforcement, representation, rnn, state of the art, state-of-the-art, statistical, supervis, train, transformer, unsupervis |

: Terms related to AI and machine learning in our heuristic filter {#tbl:terms}

Second, we have excluded articles with concept scores below a threshold value for key concepts. More specifically, we have only retained articles which have either a score above 0.4 in "Artificial intelligence" and/or a score above 0.3 in "Machine learning" and/or a score above 0.6 in "Deep learning".[^2]

[^2]: We note that the resulting corpus will include AI and machine learning papers that use statistical machine learning techniques such as random forests, support vector machines and gradient boosting which the literature suggests have played an important role in the biological sciences and genomics.

We have selected these thresholds after evaluating the impact of various combinations of values on a subset of the data (comprising the years 2012, 2017 and 2021) which we have labelled with information about 1702 preprints submitted to top AI conferences (including NEURIPS, ICML, CVF, ECCV, AAAI, PMLR, SIGKKD and IJCAI) identified in the [Papers with Code](https://paperswithcode.com/) corpus and 2256 preprints from the arXiv that are not labelled with AI related categories (cs.AI, cs.CL, cs.CV, cs.IR, cs.LG, cs.NE, cs.SO, math.ST, q-bio.QM, stat.ML), and which we assume are not-related to AI or Machine Learning.

The aforementioned combination performs best in terms of the F1-Score (which combines precision and recall i.e. the ability of the definition to generate accurate positive predictions while reducing the numer of erroneous negative predictions), with a score of 0.64.[^3] This is underpinned by 55% predictive accuracy and 78% recall. We could easily increase the predictive accuracy of the model at the expense of lower recall. For example, our top parameter combination retains 329,764 articles in the subset of our AI corpus. We could alternatively opt for a a set of thresholds of 0.6, 0.5 and 0.3 in order to increase predictive accuracy to 70% but this would bring down recall to 30% and the size of the corpus to 121,877 articles.

@tbl:examples presents some examples of papers which are correctly / incorrectly classified with our current definition. The "word soup" paper has been misclassified because the OpenAlex system assigned it "Speech recognition" and "Natural Language Processing" tags. In the false negative case of the reinforcement learning paper, we notice that the AI score is below our threshold but (unsurprisingly) the reinforcement score is very high (0.7) suggesting potential avenues to improve our classification by including higher-granularity concepts into our selection procedure.

| OpenAlex ID                      | Status         | Title                                                                                    |
| -------------------------------- | -------------- | ---------------------------------------------------------------------------------------- |
| https://openalex.org/W2099547633 | True positive  | A Survey on Language Modeling using Neural Networks                                      |
| https://openalex.org/W2158878127 | False positive | Transcribing handwritten text images with a word soup game                               |
| https://openalex.org/W2605758806 | True negative  | Pedagogical models of concordance use: correlations between concordance user preferences |
| https://openalex.org/W3202124232 | False negative | Dimension-Free Rates for Natural Policy Gradient in Multi-Agent Reinforcement Learning.  |

:Examples of papers in different categories {#tbl:examples}

[^3]: More formally, precision is defined as the percentage of entities which are predicted to be in a class (in our case AI papers) that are in fact in that class; recall is defined as the percentage of instances that are in a class that are predicted in that class.

When we apply all our filters to the data, including the heuristic filters and the concept-based filters, this results in a corpus with 837K observations. Although this is a substantial decrease from the initial 1.9m corpus, it is still likely to overestimate the number of AI papers for reasons noted above. However, given our interest in AI research in the intersection with biological sciences and genomics, we believe that it is at this point desirable to adopt an inclusive definition of AI to avoid losing potentially relevant research activities. We note that the current definition is provisional and subject to further improvement and refinement. Some options to do this include:

- Further processing downstream when we start analysing the AI genomic corpus
- Using lower-level concepts in the OpenAlex taxonomy
- Using alternative strategies such as topic modelling or keyword search of abstracts instead of relying on OpenAlex concepts

We will also be able to assess the robustness of our findings to adopting more / less strict definitions of AI research.

## Genomics corpus processing

OpenAlex papers obtained from the [PubMed](https://pubmed.ncbi.nlm.nih.gov/) library of biomedical research include their [MeSH](https://www.nlm.nih.gov/mesh/meshhome.html) (Medical Subject Headings) category as well as their OpenAlex concepts. This gives us two parallel avenues to operationalise the genomics definition with different pros and cons: MeSh tags are assigned by human experts and are therefore more likely to be reliable than OpenAlex concepts based on a predictive model. Their downside is that they are not available for all papers. We explore their issues below:

### OpenAlex

We have extracted all OpenAlex concepts that mention the term "genom". This yields the list portrayed in @fig:concepts.

![Genome / genomics related concepts in the OpenAlex taxonomy](/genomics_concepts.png){#fig:concepts}

Having done this, we have selected all papers in our genetics corpus with at least one genomics concept, which leaves us with 112,271 papers. An initial exploration of the data suggests a lower level of noise / false positives than was the case with the Artificial Intelligence corpus. Unfortunately, it seems that our challenge with the genomics corpus is the opposite than we faced there, which is low recall: many articles in genomics related concepts that we might have expected to find nested within the higher level genetics category that we have already collected are not present there. As an example, when we query our final AI corpus for papers with genomics-related concepts, we find 2900 papers that are not present in the genetics corpus. This suggests that we might have to collect additional data for those concepts at a finer level of resolution.

### MeSH

38% of the papers in the genetics corpus include MeSH terms. We have identified all MeSH headings in that corpus that include the term "genom", which yields 40 terms that are included in 47K papers in the corpus (see @tbl:mesh).

| MeSH                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Achaete-Scute Complex Genome Region, Comparative Genomic Hybridization, Epigenome, Epigenomics, Genome, Genome Components, Genome Size, Genome-Wide Association Study, Genome_Archaeal, Genome_Bacterial, Genome_Chloroplast, Genome_Fungal, Genome_Helminth, Genome_Human, Genome_Insect, Genome_Microbial, Genome_Mitochondrial, Genome_Plant, Genome_Plastid, Genome_Protozoan, Genome_Viral, Genomic Imprinting, Genomic Instability, Genomic Islands, Genomic Library, Genomic Medicine, Genomic Structural Variation, Genomics, Human Genome Project, Metagenome, Metagenomics, National Human Genome Research Institute (U.S.), Nutrigenomics, Pharmacogenomic Testing, Pharmacogenomic Variants, Proteogenomics, Segmental Duplications_Genomic, Viral Genome Packaging, Whole Genome Sequencing, t-Complex Genome Region |

: Genomics related MeSH terms in our the genetics OpenAlex corpus {#tbl:mesh_genom}

We find that 70% of the papers with MeSH genomics-related terms also have OpenAlex genomic-related concept. In total, we find 126,894 papers in our data with a MeSH or OpenAlex genomics-related terms/concepts (and 1,760 in the AI corpus).

### Crude text search

We have also performed a crude search of the term "genom" within the abstracts in our provisional AI corpus. This reveals 4,437 papers, which is a substantially higher number than the aforementioned 1,760 and suggests gaps in coverage of genomics research when using expert-driven (MeSH) and predictive taxonomies. One option going forward is to combine MeSh labels, OpenAlex concepts and texts from abstracts in order to delineate AI genomics applications. This might also be necessary to remove from the analysis areas of genomics research that are less relevant for this project, such as for example research on animal or plant genomics.
