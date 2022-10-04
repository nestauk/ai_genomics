import re
from typing import List, Optional, Dict, Any, Tuple

import toolz.curried as t
from gensim.models import Phrases
from gensim.models.phrases import FrozenPhrases
from pandas import DataFrame
import tomotopy as tp


from string import punctuation

PUNCT = "|\\".join([x for x in punctuation])


def remove_symbols(doc: str):
    """Remove symbols from a document"""

    return re.sub("\n", " ", re.sub(PUNCT, "", doc.lower()))


def remove_stop_punct(doc):
    """Remove stop words and punctuation"""

    return [d.lower_ for d in doc if (d.is_punct is False) & (d.is_stop is False)]


def build_ngrams(
    documents: List[List[str]], n: int = 2, phrase_kws: Optional[Dict[str, Any]] = None
) -> Tuple:
    """Create ngrams using Gensim's phrases.
    Args:
        documents: List of tokenised documents.
        n: The `n` in n-gram.
        phrase_kws: Passed to `gensim.models.Phrases`.
    Returns:
        List of n-grammed documents.
    """
    if n < 2:
        return (documents, None)

    def_phrase_kws = {
        "scoring": "npmi",
        "threshold": 0.25,
        "min_count": 2,
        "delimiter": "_",
    }
    phrase_kws = t.merge(def_phrase_kws, phrase_kws or {})

    def step(documents, n):
        print(f"N-gram: {n}")
        bigram = FrozenPhrases(Phrases(documents, **phrase_kws))
        return bigram[documents], bigram

    for n in range(2, n + 1):
        documents, bigram = step(documents, n)

    return documents, bigram

    # return reduce(step, range(2, n + 1), documents)


def train_lda(docs: List[str], k: int = 50, top_remove: int = 500, verbose=False):
    """Train an LDA model on a list of tokenised documents"""
    mdl = tp.LDAModel(tw=tp.TermWeight.ONE, min_cf=3, rm_top=top_remove, k=k)
    for n, doc in enumerate(docs):
        idx = mdl.add_doc(doc)
        if n != idx:
            print(n)
        # mdl.add_doc(doc)
        # if idx < 0:
        #     print(n)
        #     print("Document not added")
    mdl.burn_in = 100
    mdl.train(0)

    if verbose is True:
        print(
            "Num docs:",
            len(mdl.docs),
            ", Vocab size:",
            len(mdl.used_vocabs),
            ", Num words:",
            mdl.num_words,
        )
    print("Removed top words:", mdl.removed_top_words)
    print("Training...", flush=True)
    for i in range(0, 1000, 10):
        mdl.train(10)
        # print("Iteration: {}\tLog-likelihood: {}".format(i, mdl.ll_per_word))

    # mdl.summary()

    if verbose is True:
        for k in range(mdl.k):
            print("Topic #{}".format(k))
            for word, prob in mdl.get_topic_words(k):
                print("\t", word, prob, sep="\t")

    return mdl


def create_topic_names(mdl: tp.LDAModel, k: int, n_words=10) -> list:
    """Create a list of topic names"""

    return [
        "_".join([el[0] for n, el in enumerate(mdl.get_topic_words(n)) if n < n_words])
        for n in range(k)
    ]


def create_doc_topics(mdl: tp.LDAModel, topic_names: list, doc_ids: list) -> DataFrame:
    """Make a list of topic probabilities for each document"""

    return DataFrame(
        [mdl.docs[n].get_topic_dist() for n in range(len(mdl.docs))],
        columns=topic_names,
        index=doc_ids,
    )
