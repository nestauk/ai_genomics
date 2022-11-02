import re
from typing import List
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np


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


def get_top_terms(list_of_ents: List[str], n_top: int = 3) -> str:
    # https://stackoverflow.com/questions/34232190/scikit-learn-tfidfvectorizer-how-to-get-top-n-terms-with-highest-tf-idf-score
    """Gets top n features using TF-IDF."""
    tfidf_vectorizer = TfidfVectorizer(stop_words="english")
    tfidf = tfidf_vectorizer.fit_transform(list_of_ents)
    importance = np.argsort(np.asarray(tfidf.sum(axis=0)).ravel())[::-1]
    tfidf_feature_names = np.array(tfidf_vectorizer.get_feature_names_out())
    return "-".join(tfidf_feature_names[importance[:n_top]])
