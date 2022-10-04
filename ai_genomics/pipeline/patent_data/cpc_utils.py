from bs4 import BeautifulSoup
import json
import os
from typing import Dict, Union
from pathlib import Path


def make_cpc_lookup(scheme_dir: Union[Path, str], out_path: Union[Path, str]):
    """Creates a lookup between CPC codes, their descriptions and
    their parents.

    Args:
        scheme_dir: Directory of CPC scheme XML files.
        out_path: Output path for the lookup.
    """
    code_lookup = {}

    for file in os.listdir(scheme_dir):
        if not file.endswith(".xml"):
            continue

        with open(scheme_dir / file, "rb") as f:
            scheme = f.read()

        soup = BeautifulSoup(scheme, features="xml")
        code_lookup.update(parse_scheme(soup))

    with open(out_path, "w") as f:
        json.dump(code_lookup, f)


def parse_scheme(scheme: BeautifulSoup) -> Dict:
    """Extracts the description and parent code for each CPC code
    in the scheme.

    Args:
        scheme: A soup of an XML CPC scheme.

    Returns:
        lookup: Dictionary where keys are CPC codes and values
            are dictionaries containing the code's description
            and the code for its parent.
    """
    lookup = {}
    for element in scheme.find_all("classification-item"):
        if element.has_attr("sort-key"):
            key = element.find("classification-symbol").text
            description = element.find("title-part").text

            parent = element.parent
            parent_key = parent.find("classification-symbol").text
            if (
                parent_key == key
            ):  # workaround for handful of top level cases where they return own code as parent
                if len(parent_key) <= 4:
                    parent_key = None
                else:
                    parent_key = parent.parent.find("classification-symbol").text

            lookup[key] = {
                "description": description,
                "parent": parent_key,
            }

    return lookup


def find_description(code: str, lookup: Dict) -> str:
    """Finds description of a patent code."""
    return lookup[code]["description"]


def find_context(
    code: str,
    lookup: Dict,
    min_level: int = 1,
) -> str:
    """Finds a contextual description of a patent code from its parents."""
    contexts = []
    while True:
        parent = lookup[code]["parent"]
        if parent in lookup:
            contexts.append(lookup[parent]["description"])
            code = parent
        else:
            break
    context = "; ".join(contexts[min_level::-1])
    return context
