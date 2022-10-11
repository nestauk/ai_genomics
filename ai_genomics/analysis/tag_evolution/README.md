# Evolution of AI Genomics tags over time

The scripts in this directory focus on clustering AI Genomics tags across all datasets over time in two approaches: 1) a semantic approach and 2) a tag co-occurance network based approach.

In both instances, DBpedia entities are cumulatively 'timesliced' for the last 5 years based on founded, publication or granted dates across the data sources. Cluster names are propagated based on a minimum jaccard similarity score of at least 0.5 for given clusters a t and t+1 to be considered the same. 

## Semantic Approach 

For the semantic approach, we generate vector representations of extracted DBpedia entities using HuggingFace’s sentence embedding all-MiniLM-L6-v2 model. We then reduce its dimensionality using UMAP (Uniform Manifold Approximation and Projection).  

We cluster at each cumulative timeslice using a K-means algorithm, where we define the best K at each timeslice as the K that maximises the silhouette score for different values of K. Finally, we propagate cluster labels across each cumulative timeslice based on a minimum jaccard similarity score threshold between entity clusters.   

to run this pipeline:

`python ai_genomics/analysis/tag_evolution/tag_evolution_embeds.py`

## Network Approach

Meanwhile, we follow a very similar approach with a co-occurance network. Here, we create a entity pair co-occurance network and add the year the pair first appeared across the corpus of data as an edge attribute.

We identify communities at cumulative timeslices using the leiden algo and propagate community labels across cumulative timeslices based on a minimum jaccard similarity score threshold, comparing lists of nodes. 

to run this pipeline:

`python ai_genomics/analysis/tag_evolution/tag_evolution_network.py`

## To Dos 
 - add openalex entities