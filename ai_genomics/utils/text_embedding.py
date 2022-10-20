import numpy as np
from sentence_transformers import SentenceTransformer
from toolz.itertoolz import partition_all

from numpy.typing import NDArray
from typing import Optional, Sequence
import umap


def embed(
    texts: Sequence[str], model: str, chunk_size: Optional[int] = None,
) -> NDArray:
    """Fetches a transformer model and applies it to a sequence of texts to
    generate text embeddings.

    Args:
        texts (Sequence[str]): A sequence of texts to embed.
        model (str): A text transformer model from https://www.sbert.net/.
        chunk_size (int): If specified, the sequence of texts will be split
            into chunks of this size and embedded sequentially. Only needed
            if memory limits are an issue.

    Returns:
        NDArray: Embeddings of the texts wher m is the number of texts and n is
        the dimension of a single embeddings, which will depend on the specific
        transformer used.
    """
    model = SentenceTransformer(model)
    return model.encode(texts)


def reduce(embeds: NDArray) -> NDArray:
    """Reduces text embeddings to 2-dimensions using a Uniform Manifold 
        Approximation and Projection algorithm.

    Args:
        embeds (NDArray):  Embeddings of the texts wher m is the number of texts and n is
            the dimension of a single embeddings, which will depend on the specific
            transformer used.
    
    Returns:
        NDArray: Reduced embeddings of texts to 2-dimensions
    """
    reducer = umap.UMAP()
    return reducer.fit_transform(embeds)
