# %% [markdown]
"""
Simple script to extract cpc and ipc codes related to genomics via
exact match.

To run: python get_genomics_codes.py
"""
# %% [markdown]
#
# %%
import string

from ai_genomics import bucket_name, config, PROJECT_DIR
from ai_genomics.getters.data_getters import (
    get_s3_dir_files,
    load_s3_data,
    save_txt_file,
)

from ai_genomics.utils.patent_data.get_ai_genomics_patents_utils import clean_ipc_codes

from typing import List

# %%
GENOMICS_KEYWORDS = ["genome", "dna", "gene", "genetic"]

BAD_CPC_CODES = [
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
    "G06N3/086",
    "G06N3/12",
    "G06N3/00",
    "G06N3/002",
]

BAD_IPC_CODES = ["G06N0003120000", "G10L0025390000", "G10L0025390000", "G06F0111060000"]


def clean_class_code(code_text: str) -> str:
    """Cleans definitions by:
            - lowercasing;
            - replacing values;
            - removing punctuation;
            - splitting
    Inputs:
        code_text: definition to clean
    Outputs:
        code_text: clean definition
    """
    return (
        code_text.replace("\r", "")
        .lower()
        .translate(str.maketrans("", "", string.punctuation))
        .split(" ")
    )


def get_genomics_classification_codes(
    class_codes: List[str], bad_codes: List[str]
) -> dict:
    """Get genomics-related classification codes.

    Inputs:
        class_codes (list): List of patent classification codes.

    Outputs:
        genomics_codes (dict): Dict of relevant genomics-related
        classification codes.
    """
    genomics_codes = []
    for code in class_codes:
        code_clean = clean_class_code(code[1])
        if any([code in code_clean for code in GENOMICS_KEYWORDS]):
            genomics_codes.append(code[0])
    # remove bad codes based on manual review
    return [code for code in genomics_codes if code not in bad_codes]


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

    genomics_cpc = get_genomics_classification_codes(cpc_codes_not_empty, BAD_CPC_CODES)
    genomics_ipc = get_genomics_classification_codes(ipc_codes_not_empty, BAD_IPC_CODES)

    save_txt_file("outputs/data/codes/genomics_cpc.txt", genomics_cpc)
    save_txt_file("outputs/data/codes/genomics_ipc.txt", genomics_ipc)
