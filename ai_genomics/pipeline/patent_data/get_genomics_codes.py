"""Simple script to extract cpc and ipc codes related to genomics via
exact match.

To run: python get_genomics_codes.py
"""
#############################################################
import string

from ai_genomics import bucket_name, config
from ai_genomics.getters.data_getters import (
    s3,
    get_s3_dir_files,
    load_s3_data,
    save_to_s3,
)

#############################################################

GENOMICS_KEYWORDS = ["genome", "dna", "gene", "genetic"]


def clean_class_code(cpc_code_text):
    """Cleans CPC definitions by:
            - lowercasing;
            - replacing values;
            - removing punctuation;
            - splitting
    Inputs:
        cpc_code_text: CPC definition to clean.
    Outputs:
        cpc_code_text: clean CPC definition
    """
    return (
        cpc_code_text.replace("\r", "")
        .lower()
        .translate(str.maketrans("", "", string.punctuation))
        .split(" ")
    )


def get_genomics_classification_codes(class_codes: list) -> dict:
    """Get genomics-related classification codes.
    
    Inputs:
        class_codes (list): List of patent classification codes.
    
    Outputs:
        genomics_codes (dict): Dict of relevant genomics-related
        classification codes.
    """
    genomics_codes = dict()
    for code in class_codes:
        code_clean = clean_class_code(code[1])
        if any([code in code_clean for code in GENOMICS_KEYWORDS]):
            genomics_codes[code[0]] = " ".join(code_clean)
    return genomics_codes


if __name__ == "__main__":

    class_code_files = get_s3_dir_files(s3, bucket_name, config["patent_class_codes_path"])

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

    genomics_codes_dict = dict()
    genomics_codes_dict["cpc"] = get_genomics_classification_codes(cpc_codes_not_empty)
    genomics_codes_dict["ipc"] = get_genomics_classification_codes(ipc_codes_not_empty)

    # save list of codes to s3
    save_to_s3(
        s3, bucket_name, genomics_codes_dict, config["genomics_class_codes_file"]
    )
