import re
from typing import List


def strip_punct(s: str) -> str:
    """Strips punctuation from acronyms."""
    remove_chars = r"[,|/|\?|\(|\)|\:|\;|\.]"
    space_chars = r"[\-|_|]"
    stripped = re.sub(remove_chars, "", s)
    return re.sub(space_chars, " ", stripped)


def jaccard_similarity(list1: List[str], list2: List[str]) -> float:
    """Calculates jaccard similarity between two lists of strings."""
    s1 = set(list1)
    s2 = set(list2)
    return float(len(s1.intersection(s2)) / len(s1.union(s2)))
