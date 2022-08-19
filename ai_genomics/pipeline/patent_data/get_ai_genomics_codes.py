"""
Simple script to extract cpc and ipc codes related to genomics and AI via
exact match and manual pruning/adding.

To run: python get_ai_genomics_codes.py
"""
import string

from ai_genomics import bucket_name, config, PROJECT_DIR, logger
from ai_genomics.getters.data_getters import (
    get_s3_dir_files,
    load_s3_data,
    save_to_s3,
)
from ai_genomics.utils.patent_data.get_ai_genomics_patents_utils import clean_ipc_codes

from typing import List, Dict
from toolz.functoolz import pipe
import re

AI_KEYWORDS = ["machine learning", "artificial intelligence", "neural network"]

GENOMICS_KEYWORDS = ["genome", "dna", "gene", "genetic"]

GOOD_GENOMICS_CPC_CODES = ["C12Q1/6869", "G16B20/20", "G16B5/20"]

GOOD_AI_CPC_CODES = [
    "Y10S706/902",
    "Y10S706/908",
    "Y10S706/911",
    "Y10S706/916",
    "Y10S706/919",
    "Y10S706/92",
    "Y10S706/921",
    "Y10S706/922",
    "Y10S706/923",
    "Y10S706/932",
    "Y10S706/934",
    "G16B40/20",
    "G16B40/30",
    "G06V10/762",
    "G06V10/7635",
    "G06V10/764",
    "G06V10/77",
    "G06V10/86",
]

BAD_GENOMICS_CPC_CODES = [
    "H01J49/0409",
    "H04L9/0866",
    "C12P",
    "G10H2220/386",
    "C12C2200/01",
    "A01H1/06",
    "A01K11/003",
    "A01K2217/15",
    "A01K2267/0306",
    "A23C19/0326",
    "A23C2220/202",
    "A23V2300/21",
    "C40B10/00",
    "C40B40/08",
    "G05B23/0229",
    "G05B2219/32091",
    "G05B2219/32333",
    "G05B2219/33041",
    "G05B2219/35041",
    "G05B2219/40384",
    "G05B2219/40473",
    "G05B2219/42145",
    "G05B2219/42147",
    "G06F2111/06",
    "G06K7/1482",
    "G06K9/6229",
    "G06N3/086",
    "G06N3/126",
    "G10H2250/011",
    "G10K2210/3029",
    "G10L25/39",
    "G01N23/20",
    "G06N3/12",
    "G06N3/123",
    "G06N3/00",
    "G06N3/002",
]

BAD_GENOMICS_IPC_CODES = [
    "G06N0003120000",
    "G10L0025390000",
    "G10L0025390000",
    "G06F0111060000",
]


def clean_class_code(code_text: str) -> str:
    """Cleans definitions by:
            - lowercasing;
            - replacing values;
            - removing punctuation;

    Inputs:
        code_text: definition to clean

    Outputs:
        code_text: clean definition
    """
    return (
        code_text.replace("\r", "")
        .lower()
        .translate(str.maketrans("", "", string.punctuation))
    )


def make_keywords_regex_pattern(keywords: List[str]) -> str:
    """Makes regex pattern given a list of keywords or phrases"""
    return "|".join(f"\\b{k}\\b" for k in keywords)


def get_classification_codes(
    class_codes: List[str],
    bad_codes: List[str],
) -> Dict[str, list]:
    """Get keyword-related classification codes per classification system.

    Inputs:
        class_codes: List of patent classification codes and descriptions of a given classification system.
        bad_codes: List of 'bad' patent classification codes of a given classification system
    """
    classification_codes = {label: [] for label in ("genomics", "ai")}
    ai_pattern, genomics_pattern = (
        make_keywords_regex_pattern(AI_KEYWORDS),
        make_keywords_regex_pattern(GENOMICS_KEYWORDS),
    )

    for i, code in enumerate(class_codes):
        code_clean = clean_class_code(code[1])
        if re.findall(genomics_pattern, code_clean):
            if class_codes[i][0] not in bad_codes:
                classification_codes["genomics"].append(class_codes[i][0])
        if re.findall(ai_pattern, code_clean):
            classification_codes["ai"].append(class_codes[i][0])

    return classification_codes


if __name__ == "__main__":

    class_code_files = get_s3_dir_files(bucket_name, config["patent_class_codes_path"])

    cpc_codes = []
    ipc_codes = []
    for class_code in class_code_files:
        if "txt" in class_code:
            if "cpc" in class_code:
                cpc_codes.extend(load_s3_data(bucket_name, class_code))
            elif "ipc" in class_code:
                ipc_codes.extend(load_s3_data(bucket_name, class_code))

    cpc_codes_not_empty, ipc_codes_not_empty = (
        [[i[0], i[2]] for i in cpc_codes if len(i) == 3],
        [[i[0], i[1]] for i in ipc_codes if len(i) == 2],
    )

    cpc_codes = get_classification_codes(cpc_codes_not_empty, BAD_GENOMICS_CPC_CODES)
    # manually add a few more codes
    cpc_codes["genomics"] = cpc_codes["genomics"] + GOOD_GENOMICS_CPC_CODES
    cpc_codes["ai"] = cpc_codes["ai"] + GOOD_AI_CPC_CODES

    ipc_codes = get_classification_codes(ipc_codes_not_empty, BAD_GENOMICS_IPC_CODES)

    save_to_s3(bucket_name, cpc_codes, "outputs/patent_data/class_codes/cpc.json")
    save_to_s3(
        bucket_name,
        {k: clean_ipc_codes(v) for k, v in ipc_codes.items()},
        "outputs/patent_data/class_codes/ipc.json",
    )
