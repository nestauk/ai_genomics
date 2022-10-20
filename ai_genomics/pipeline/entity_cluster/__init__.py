from itertools import chain
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans

from typing import Any, Dict, Mapping, Sequence

from ai_genomics.utils.text_embedding import embed


def embed_entities(
    entities: Mapping[str, Sequence[str]],
    model: str,
) -> pd.DataFrame:
    """_summary_

    Args:
        entities (Mapping[str, Mapping[str, Union[str, str]]]): Entities
            (without scores) for a set of documents.
        model (str): Name of a sentence transformer model.

    Returns:
        pd.DataFrame: Dataframe where rows are entities and columns are
            embedding values.
    """
    entities_unique = list(set(chain(*entities.values())))
    embeddings = embed(entities_unique, model)

    return pd.DataFrame(embeddings, index=entities_unique)


def create_entity_embedding_clusters(
    entity_embeddings: pd.DataFrame,
    cluster_params: Mapping[str, Any],
) -> Dict[str, int]:
    """Clusters entity embeddings using K means and returns a lookup between
    entities and their cluster ID.

    Args:
        entity_embeddings (pd.DataFrame): Embeddings for a set of entities.
        cluster_params (Mapping[str, Any]): A dictionary or other mapping of
            kwargs to pass into the clusterer.

    Returns:
        Dict[str, int]: A lookup between entities and their cluster IDs.
    """
    km = KMeans(**cluster_params)
    km.fit(entity_embeddings)

    return dict(zip(entity_embeddings.index, [int(l) for l in km.labels_]))


def create_doc_vectors(
    entities: Mapping[str, Sequence[str]],
    entity_to_clusters_lookup: Mapping[str, int],
) -> pd.DataFrame:
    """Create document vectors that represent the presence of an entity cluster
    within a document.

    This works like a count vectorisation. The number of entities in each
    document belonging to an entity cluster is summed. The result is a sparse
    matrix where rows are documents and columns represent the number of times
    an entity cluster was identified in a document.

    Args:
        entities (Mapping[str, Sequence[str]]): Entities (without scores) for
            a set of documents.
        entity_to_clusters_lookup (Mapping[str, int]): A lookup between
            entities and their cluster.

    Returns:
        pd.DataFrame: The document vectors.
    """

    d = max(entity_to_clusters_lookup.values()) + 1

    doc_vectors = []
    for ents in entities.values():
        entity_groups = [
            entity_to_clusters_lookup[e]
            for e in ents
            if entity_to_clusters_lookup.get(e) is not None
        ]
        doc_vectors.append(np.bincount(entity_groups, minlength=d))

    doc_vectors = pd.DataFrame(np.array(doc_vectors), index=entities.keys())
    doc_vectors.index.name = "id"
    return doc_vectors
