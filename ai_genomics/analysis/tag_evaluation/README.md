# Tagging Evaluation

Two different types of tagging methods were explored to extract tags from texts across datasets (OpenAlex, crunchbase, patents, GtR): **DBpedia** and **Open Alex tagger**.

To compare the two different tagging methods, four different datasets of 100 texts per dataset were generated: 100 crunchbase company descriptions, patent abstracts, GtR abstracts and OpenAlex, book chapter/papers. OpenAlex and DBpedia tags were then extracted per text. Only DBpedia tags with a score of at least 60 were generated.

Finally, a sample of a sample was generated (80 texts and their associated tags per tag method) and then labelled (40 texts and their associated tags per tag method) for false positives and false negatives per tagging method.

Across the four datasets, 18 crunchbase texts were labelled; 13 patents were labelled; 3 gtr texts were labelled; and 6 openalex datasets were labelled. False positives were identified if they had nothing to do with the text at hand. Therefore, (where relevant) vaguer tags such as computer science and biology were considered true positives. Meanwhile, false negatives were derived from skimming the text and comparing the two tag lists.

### Results

Some high level results from labelling the validation set are in the table below:

| tagging_method | # of false positives | # of false negatives | texts with at least 1 false positive (%) | texts with at least 1 false negative (%) | median # of tags |
| -------------- | -------------------- | -------------------- | ---------------------------------------- | ---------------------------------------- | ---------------- |
| OpenAlex       | 52                   | 42                   | 62.5                                     | 60                                       | 7                |
| DBpedia        | 9                    | 22                   | 12.5                                     | 42.5                                     | 4                |

Examples of **OpenAlex false positives** include:

avatar, unicorn, Class (philosophy), world wide web, gerontology

Examples of **DBpedia false positives** include:

Zhou_dynasty, Master_of_Business_Administration, Microsoft_Edge, Marvel_Super_Heroes_Advanced_Set

Examples of **OpenAlex false negatives** include:

gene, microbiology, neural network, artificial intelligence, biomarkers

Examples of **DBpedia false negatives** include:

novel medicine, synthetic biology, precision oncology, pairwise comparison, precision medicine

Note that the much higher OpenAlex false positive % should be taken with a pinch of salt given no thresholding occurred with this tag method.

### Conclusions and considerations

Although using DBpedia with a threshold of 0.6 results in fewer tags per text, the tags are more complete (few false negatives) and qualitatively, tend to be more granular. I suggest moving forward with DBpedia with the caveat that DBpedia often extracts places and institutions while OpenAlex did not. If we opt for DBpedia, we may want to deal with extracted place names and institutions by i.e. using spaCy's POS tags to filter them out.
