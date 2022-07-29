"""ai_genomics/utils/text.py

Non-domain specific utility functions for processing strings and tokenised text data.
"""


def contains_non_ascii(s: str) -> bool:
    """Flags if a string contains characters outside of the ASCII range."""
    for c in s:
        if ord(c) > 127:
            return True
    return False
