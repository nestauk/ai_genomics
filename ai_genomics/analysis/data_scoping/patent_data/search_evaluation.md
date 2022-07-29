# Patent Search Evaluation

## Approach 1

### Collection

Patent publication numbers (`doc_id`) were selected from USPTO AI data by filtering for those which are patents (`patent_flag == 1`) and those which are predicted to relate to AI (`predict50_any_ai == 1`). These publication numbers were then used to query a database which is a subset of patents with IPC or CPC codes relating to genomics. These codes were identified using simple partial keyword matching. This resulted in ~8.5k patents.

### Evaluation

One hundred examples were randomly sampled from the data and inspected to determine whether they related to AI or genomics (`outputs/data/patents/ai_genomics_patents_verified.csv`). Their title and abstract were read to see if they contained content relating to those subjects and binary indicators were used to denote whether or not this was the case. In some cases, the title and abstract were ambiguous, and the patent was searched for on Google Patents and inspected in further detail. This was typically done by skim reading and searching for keywords belonging to each topic (e.g. gene, genetic, dna, nucleic, machine learning, classify, cluster, detect). The results should be interpreted with some degree of error as it is often quite difficult to identify whether a patent relates to the topics of interest due to vague language, non-expert knowledge in genomics and the length of some patents.

The table below shows the counts of projects in the sample that are and are not related to AI and genomics. It is immediately clear that there are many projects that are not related to the two subjects - 66 are related to only one of them, and 7 are related to neither. The content of the titles, abstracts and patent texts suggests some reasons as to why this may be.

|        | Not Genomics | Genomics |
| :----- | -----------: | -------: |
| Not AI |            7 |       41 |
| AI     |           25 |       27 |

One reason is that the definitions of AI and genomics are overly broad. The current filtering mechanism includes patents predicted to relate to machine learning, natural language processing, computer vision, speech, knowledge processing, AI hardware, evolutionary computation, and planning and control. Several patents covered topics such as new silicon chip architectures and automated manufacturing processes, which are more tangentially related to enabling and employing AI. In addition, there were several patents that covered genetic algorithms, but did not touch upon genomics. Upon closer inspection, the patent codes for these documents often referred to 'genetic computing'.

Another possible reason is poor classification of AI topics in the USPTO data. Several examples in the dataset discussed topics that contained vocabulary associated with AI and machine learning but that were being used in other contexts. For example, one patent referred to 'clustering' several times but was in fact referring to the physial aggregation of biological particles. In several cases, the word 'data' is used throughout, but the methods described to use the data are not based on machine learning. There were also some patents that used conventional computer vision, reliant on conventional image processing rather than AI-like technologies. It is not immediately clear which part of the pipeline used to predict AI patents that these errors originate from.

In addition to counting the number of patents that related to our two areas of interest, the number of patents that had ambiguous tites or abstracts were recorded. We can see that in the minority of cases where Only 27 that are both AI and genomics. 66 are only one and 7 are neither. This suggests that we should be careful when performing text analysis (e.g. topic modelling) on the patent abstracts as they do not reveal all of the information about the patent that might be most relevant for our purpose.

|        | Not Genomics | Genomics |
| :----- | -----------: | -------: |
| Not AI |            0 |       21 |
| AI     |           11 |       23 |

### Suggested improvements

First we should refine the IPC/CPC patent codes used for our definition of genomics. This could be done manually (e.g. by pruning topics such as 'genetic algorithms') or by using a more sophisticated for creating the definition (e.g. embeddings instead of keywords). As the first method is simpler, this should be attempted first. We should also ensure the IPC/CPC codes used are as expansive as possible to make sure that we also capture genomics related topics that might not have appeard in the results from this query. A combined keyword search and manual filtering approach is probably suitable for this.

Second, we should also refine the columns used from the USPTO data for our definition of AI. We could drop AI hardware, planning and control and knowledge processing to be left with machine learning, natural language processing, computer vision evolutionary computation and speech.

## Approach 2

### Collection

The same approach as above is used, except that the USPTO data is filtered by specific AI flags, rather than for all 'AI related' work. These are machine learning, evolutionary computation, computer vision, speech and natural language processing (using the columns prefixed by `predict50_`). This yielded ~3.5k patents.

### Evaluation

In this case, a random sample of 50 patents (`outputs/data/patents/ai_genomics_patents_ml_evo_cv_nlp_sp_verified.csv`) were evaluated in the same way described above. As a proportion, we see improved recall on patents related to AI, but a decrease in those related to genomics. Annecdotally, there is a high representation of patents describing genetic algorithms, but with no relationship to genomics.

There is also some noise in the USPTO data resulting in false positives, which contribute to the 20 patents assessed as _Not AI_.

|        | Not Genomics | Genomics |
| :----- | -----------: | -------: |
| Not AI |            6 |       14 |
| AI     |           19 |       11 |

### Suggested improvements

- The next step would be to expand and refine the patent code definition of genomics and re-run the evaluation before advancing on to any more sophisticated methods.
- Another step would be to further refine the data in the USPTO AI patents by setting a higher threshold on the prediction probability. The `predict50_` fields set a minimum threshold of 0.5 on the prediction probability for a patent to be classed as AI related. Setting this higher (e.g. 0.9) would eliminate some false positives. A larger sample of labelled data would be needed to determine whether the benefits gained in precision would offset the loss in recall.

## Next steps

- [ ] Expand and refine genomics patent codes.
- [ ] Explore the possibility of creating an AI patent code list to complement the predictions in the USPTO data.
- [ ] Investigate the distribution of prediction probabilities in the USPTO data for manually labelled patents to determine a new minimum threshold for inclusion.
- [ ] Explore the distribution of patent codes across the resultant dataset.
