import re


def strip_punct(s: str) -> str:
    """Strips punctuation from acronyms."""
    remove_chars = r"[,|/|\?|\(|\)|\:|\;|\.]"
    space_chars = r"[\-|_|]"
    stripped = re.sub(remove_chars, "", s)
    return re.sub(space_chars, " ", stripped)
