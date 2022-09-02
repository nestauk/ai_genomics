"""
Script to extract cpc and ipc codes related to genomics and AI via
exact match and manual pruning/adding.

To run: python ai_genomics/pipeline/patent_data/get_ai_genomics_codes.py
"""
import string
from ai_genomics import bucket_name, config
from ai_genomics.getters.data_getters import (
    get_s3_dir_files,
    load_s3_data,
    save_to_s3,
)
from ai_genomics.utils.patents import (
    clean_ipc_codes,
    clean_code_definition,
    make_keywords_regex_pattern,
    AI_KEYWORDS,
    GENOMICS_KEYWORDS,
    GOOD_GENOMICS_CPC_CODES,
    GOOD_AI_CPC_CODES,
    BAD_GENOMICS_CPC_CODES,
    BAD_GENOMICS_IPC_CODES,
)
from typing import Mapping, List
import re


def get_classification_codes(
    class_codes: List[str],
    bad_ai_codes: List[str] = [],
    bad_genomics_codes: List[str] = [],
    good_ai_codes: List[str] = [],
    good_genomics_codes: List[str] = [],
) -> Mapping[str, Mapping[str, str]]:
    """Get keyword-related classification codes per classification system.

    Args:
        class_codes: List of patent classification codes and
            descriptions of a given classification system.
        bad_ai_codes: List of 'bad' ai patent classification codes
            of a given classification system
        bad_genomics_codes: List of 'bad' genomics patent classification codes
            of a given classification system
        good_ai_codes: List of 'good' genomics patent classification codes
            of a given classification system not found via keyword search
        good_genomics_codes: List of 'good' genomics patent classification codes
            of a given classification system not found via keyword search

    Returns:
         classification_codes: A dictionary containing keys for genomics and ai with values
            of a dictionary of classification codes and their respective definitions
    """
    classification_codes = {}
    ai_pattern, genomics_pattern = (
        make_keywords_regex_pattern(AI_KEYWORDS),
        make_keywords_regex_pattern(GENOMICS_KEYWORDS),
    )

    classification_codes = dict()

    ai_codes = dict(
        [x for x in class_codes if re.findall(ai_pattern, x[1])]
        + [x for x in class_codes if x[0] in good_ai_codes]
    )
    genomics_codes = dict(
        [x for x in class_codes if re.findall(genomics_pattern, x[1])]
        + [x for x in class_codes if x[0] in good_genomics_codes]
    )

    clean_ai_codes = {
        code: clean_code_definition(code_desc)
        for code, code_desc in ai_codes.items()
        if code not in bad_ai_codes
    }
    clean_genomics_codes = {
        code: clean_code_definition(code_desc)
        for code, code_desc in genomics_codes.items()
        if code not in bad_genomics_codes
    }

    classification_codes["ai"] = clean_ai_codes
    classification_codes["genomics"] = clean_genomics_codes

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

    cpc_codes = get_classification_codes(
        cpc_codes_not_empty,
        bad_genomics_codes=BAD_GENOMICS_CPC_CODES,
        good_ai_codes=GOOD_AI_CPC_CODES,
        good_genomics_codes=GOOD_GENOMICS_CPC_CODES,
    )
    ipc_codes = get_classification_codes(
        ipc_codes_not_empty, bad_genomics_codes=BAD_GENOMICS_IPC_CODES
    )

    # save with definitions and ipc clean codes with definitions
    save_to_s3(
        bucket_name,
        cpc_codes,
        "outputs/patent_data/class_codes/cpc_with_definitions.json",
    )
    save_to_s3(
        bucket_name,
        ipc_codes,
        "outputs/patent_data/class_codes/ipc_with_definitions.json",
    )

    formatted_ipc_codes = dict()
    for topic in ("ai", "genomics"):
        formatted_ipc_codes[topic] = {
            k: clean_ipc_codes(v) for k, v in ipc_codes[topic].items()
        }

    save_to_s3(
        bucket_name,
        formatted_ipc_codes,
        "outputs/patent_data/class_codes/ipc_formatted_with_definitions.json",
    )
