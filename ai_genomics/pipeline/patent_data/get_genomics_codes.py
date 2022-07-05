"""Simple script to extract cpc codes related to genomics via
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


def clean_cpc_code(cpc_code_text):
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


if __name__ == "__main__":

    # load data
    cpc_code_files = get_s3_dir_files(s3, bucket_name, config["cpc_codes_path"])

    cpc_codes = []
    for cpc_code_file in cpc_code_files[1:]:
        cpc_codes.extend(load_s3_data(bucket_name, cpc_code_file))

    # remove empty rows
    cpc_codes_not_empty = [cpc_code for cpc_code in cpc_codes if len(cpc_code) == 3]

    # exact match based on genomics key words
    cpc_genomics_codes = dict()
    for cpc_code in cpc_codes_not_empty:
        cpc_code_clean = clean_cpc_code(cpc_code[2])
        if any([code in cpc_code_clean for code in GENOMICS_KEYWORDS]):
            cpc_genomics_codes[cpc_code[0]] = cpc_code_clean
    # save list of codes to s3
    save_to_s3(s3, bucket_name, cpc_genomics_codes, config["genomics_cpc_codes_file"])
