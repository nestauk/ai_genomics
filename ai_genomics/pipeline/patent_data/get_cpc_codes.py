"""
Script to extract cpc codes related to genomics and AI via
exact match and validation.

To run: python ai_genomics/pipeline/patent_data/get_cpc_codes.py
"""
import string
from ai_genomics import bucket_name, config
from ai_genomics.getters.data_getters import (
    get_s3_dir_files,
    load_s3_data,
    save_to_s3,
)
from ai_genomics.utils.patents import (
    clean_code_definition,
    make_keywords_regex_pattern,
    AI_KEYWORDS,
    GOOD_AI_CPC_CODES,
)
from typing import Mapping, List
import re


def get_classification_codes(
    class_codes: List[str], good_ai_codes: List[str] = GOOD_AI_CPC_CODES,
) -> Mapping[str, Mapping[str, str]]:
    """Get keyword-related AI CPC classification codes.

    Args:
        class_codes: List of patent classification codes and
            descriptions of a given classification system.
        bad_ai_codes: List of 'bad' ai patent classification codes
            of a given classification system
        good_ai_codes: List of 'good' ai patent classification codes
            of a given classification system not found via keyword search

    Returns:
         classification_codes: A dictionary containing an ai key with values
            of a dictionary of classification codes and their respective definitions
    """
    classification_codes = {}
    ai_pattern = make_keywords_regex_pattern(AI_KEYWORDS)

    classification_codes = dict()
    ai_codes = dict(
        [x for x in class_codes if re.findall(ai_pattern, x[1])]
        + [x for x in class_codes if x[0] in good_ai_codes]
    )

    clean_ai_codes = {
        code: clean_code_definition(code_desc) for code, code_desc in ai_codes.items()
    }
    classification_codes["ai"] = clean_ai_codes

    return classification_codes


if __name__ == "__main__":

    class_code_files = get_s3_dir_files(bucket_name, config["patent_class_codes_path"])
    validated_genomics_cpc_codes_df = load_s3_data(
        bucket_name, "inputs/patent_data/validated_cpc_codes.csv"
    )
    validated_genomics_cpc_codes_df[
        "Code"
    ] = validated_genomics_cpc_codes_df.Code.str.replace("\n", "")

    cpc_codes = []
    for class_code in class_code_files:
        if "txt" in class_code:
            if "cpc" in class_code:
                cpc_codes.extend(load_s3_data(bucket_name, class_code))

    cpc_codes_not_empty = [[i[0], i[2]] for i in cpc_codes if len(i) == 3]

    cpc_codes = get_classification_codes(
        cpc_codes_not_empty, good_ai_codes=GOOD_AI_CPC_CODES,
    )

    # add validated
    cpc_codes["genomics"] = validated_genomics_cpc_codes_df.set_index("Code")[
        "Name"
    ].T.to_dict()

    save_to_s3(
        bucket_name,
        cpc_codes,
        "outputs/patent_data/class_codes/cpc_with_definitions.json",
    )
