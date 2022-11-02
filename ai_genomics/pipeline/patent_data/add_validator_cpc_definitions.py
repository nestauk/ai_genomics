"""
Script to add validator cpc definitions to generated AI genomics and
genomics sample patents data. A patent is considered 'in scope'
if it has at least 1 "in scope" CPC code associated to it.

python ai_genomics/pipeline/patent_data/add_validator_cpc_definitions.py
"""
from ai_genomics import bucket_name, logger
from ai_genomics.getters.patents import (
    get_ai_genomics_patents,
    get_genomics_sample_patents,
)
from ai_genomics.getters.data_getters import load_s3_data, save_to_s3
import ast
import pandas as pd
from typing import List


def scope_tag(cpc_codes: List[str]) -> List[str]:
    """tag each code with scope definition"""
    tags = []
    for code in cpc_codes:
        if code in not_in_scope_codes:
            tags.append("not in scope")
        elif code in in_scope_codes:
            tags.append("in scope")
        elif code in tangential_codes:
            tags.append("tangential")
        else:
            tags.append("not in validation set")

    return tags


def scope_flag(scope_tags: List[str], tag: str = "in scope") -> bool:
    """If at least 1 in scope tag matches the tag, flag document as in scope."""
    allowed = ["not in scope", "in scope", "tangential"]
    if tag in allowed:
        return True if tag in scope_tags else False
    else:
        raise ValueError(f"tag not in {allowed}")


if __name__ == "__main__":
    ai_genomics_patents = get_ai_genomics_patents()
    genomics_sample_patents = get_genomics_sample_patents()
    validated_cpc_codes = load_s3_data(
        bucket_name, "inputs/patent_data/validated_cpc_codes.csv"
    )
    logger.info("loaded ai genomics and genomics sample data")

    in_scope_codes, not_in_scope_codes, tangential_codes = (
        list(validated_cpc_codes[validated_cpc_codes["Scope"] == "In scope"].Code),
        list(validated_cpc_codes[validated_cpc_codes["Scope"] == "Not in scope"].Code),
        list(validated_cpc_codes[validated_cpc_codes["Scope"] == "Tangential"].Code),
    )

    # remove \n
    validated_cpc_codes["Code"] = validated_cpc_codes.Code.str.replace("\n", "")

    for patents_df, patents_name in zip(
        (ai_genomics_patents, genomics_sample_patents),
        ("ai_genomics", "genomics_sample"),
    ):
        patents_df["cpc_codes"] = patents_df.cpc_codes.apply(ast.literal_eval).apply(
            lambda x: list(x)
        )
        patents_df["scope_tags"] = patents_df.cpc_codes.apply(scope_tag)
        patents_df["in_scope"] = patents_df.scope_tags.apply(scope_flag)
        patents_df["tangential"] = patents_df.scope_tags.apply(
            lambda x: scope_flag(x, "tangential")
        )
        patents_df["not_in_scope"] = patents_df.scope_tags.apply(
            lambda x: scope_flag(x, "not in scope")
        )
        logger.info("added in scope flag to patents data")
        save_to_s3(
            bucket_name,
            patents_df,
            f"inputs/patent_data/processed_patent_data/{patents_name}_patents_cpc_codes.csv",
        )
