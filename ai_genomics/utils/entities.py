"""Utilities for working with entities."""
from collections import Counter
from itertools import chain
import numpy as np

from typing import Mapping, Optional, Union


def strip_scores(entities):
    """Strips the scores from DBpedia entities to leave only a lookup between
    documents and entity tags."""
    return {k: [e[0] for e in v] for k, v in entities.items()}


def filter_entities(
    entities: Mapping[str, Mapping[str, Union[str, str]]],
    min_entity_freq: Optional[Union[int, float]] = None,
    max_entity_freq: Optional[Union[int, float]] = None,
) -> Mapping[str, Mapping[str, Union[str, str]]]:
    """_summary_

    Args:
        entities (Mapping[str, Mapping[str, Union[str, str]]]): DBpedia entities
            for a set of documents without scores.
        min_entity_freq (Optional[Union[int, float]], optional): The minimum
            frequency for an entity. Any entities with a frequency below this
            will be filtered. If a float is passed, the mininum frequency will
            be the value at this percentile in the frequency distribution.
            Defaults to None.
        max_entity_freq (Optional[Union[int, float]], optional): The maximum
            frequency for an entity. Any entities with a frequency above this
            will be filtered. If a float is passed, the maximum frequency will
            be the value at this percentile in the frequency distribution.
            Defaults to None.

    Returns:
        Mapping[str, Mapping[str, Union[str, str]]]: Filtered entities.
    """
    entity_freqs = Counter(chain(*entities.values()))

    if type(min_entity_freq) == float:
        min_entity_freq = np.percentile(list(entity_freqs.values()), min_entity_freq)
    elif min_entity_freq is None:
        min_entity_freq = min(entity_freqs.values())

    if type(max_entity_freq) == float:
        max_entity_freq = np.percentile(list(entity_freqs.values()), max_entity_freq)
    elif max_entity_freq is None:
        max_entity_freq = max(entity_freqs.values())

    return {
        k: [e for e in v if min_entity_freq <= entity_freqs[e] <= max_entity_freq]
        for k, v in entities.items()
    }
