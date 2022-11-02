# DBpedia entity clustering pipeline

This directory contains the scripts needed to create entity clusters, evolved entity clusters and to name entity clusters using embeddings of extracted DBepdia entities across all four key datasets: OpenAlex, GtR, Crunchbase and patents.

To create AI in genomics entity clusters at different resolutions k and generate document vectors, run:

`python ai_genomics/pipeline/entity_cluster/create_entity_clusters.py`

This will save as output both the entity clusters for different resolutions of k (100, 200, 500 and 10000) and sparse document vectors.

To create AI in genomics entity clusters at successive time points, run:

`python ai_genomics/pipeline/entity_cluster/create_entity_clusters_over_time.py`

This will cluster AI in genomics entity embeddings at successive, cumulative time points where clusters at the final timestamp will be equivalent to AI in genomics entity clusters at resolution k=100. Labels are propagated across timestamps based on jaccard similarity between entity clusters at successive timestamps.

To generate entity cluster names for both the entity clusters at different resolutions k and for eveolved entity clusters, run:

`python ai_genomics/pipeline/entity_cluster/create_entity_clusters_names.py`

This will generate unique cluster names using the top 3 TF-IDF terms per entity cluster. To insure entity cluster names are unique, a number is appended to each non-unique cluster name. Entity cluster names can also be accessed using getters in entities.
