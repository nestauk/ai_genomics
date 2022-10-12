# Evolution of AI Genomics tags over time

The scripts in this directory focus on clustering AI Genomics tags across all datasets over time in two approaches: 1) a semantic approach and 2) a tag co-occurance network based approach.

In both instances, DBpedia entities are cumulatively 'timesliced' based on founded, publication or granted dates across the data sources. Cluster names are propagated based on a minimum jaccard similarity score of at least 0.5 for given clusters t and t+1.

## Semantic Approach

For the semantic approach, we generate vector representations of extracted DBpedia entities using HuggingFace’s sentence embedding all-MiniLM-L6-v2 model. We then reduce its dimensionality using UMAP (Uniform Manifold Approximation and Projection).

We cluster at each cumulative timeslice using a K-means algorithm, where we define the best K at each timeslice as the K that maximises the silhouette score for different values of K. Finally, we propagate cluster labels across each cumulative timeslice based on a minimum jaccard similarity score threshold between entity clusters.

to run this pipeline:

`python ai_genomics/analysis/tag_evolution/tag_evolution_embeds.py`

## Network Approach

Meanwhile, we follow a very similar approach with a co-occurance network. Here, we create a entity pair co-occurance network and add the year the pair first appeared across the corpus of data as an edge attribute.

We identify communities at cumulative timeslices using the leiden algo and propagate community labels across cumulative timeslices based on a minimum jaccard similarity score threshold, comparing lists of entities.

to run this pipeline:

`python ai_genomics/analysis/tag_evolution/tag_evolution_network.py`

## A few high observations across both approaches

- Via a network based approach, we identified ~38 clusters total throughout the timeslices and 14 clusters at the final timestamp. Two clusters from the first timeslice (2015) persist: data-algorithm-learning and urine-salvia.

- Via an embedding approach, we identify ~64 clusters total throughout the timeslices and 46 at the final timestamp. Four clusters persist throughout and they include cluster numbers 38, 34, 27 and 16: 38 appears to be related to personalised medicine and water; 34 appears related to lipids and blood; 27 has clustered animals and fish and 16 appears related to neuroscience.

## etc.

- how to filter open alex entities so its not so slow and can be included
- from the network analysis, the clusters in the network are pretty large - have a stricter merging threshold or cluster into subclusters?
- from the embedding approach, how to deal with topic combinations
