from ai_genomics import PROJECT_DIR, logger
import pandas as pd
import re

GTR_INPUTS_DIR = PROJECT_DIR / "inputs/data/gtr"


def get_ai_genomics_gtr_data(entity_type: str) -> pd.DataFrame:
    """Get AI and Genomics GtR data

    Args:
        entity_type: "projects" or "publications"

    Returns:
        Dataframe of AI and Genomics GtR projects or publications
    """
    if entity_type not in ["projects", "publications"]:
        raise ValueError(
            "ValueError: entity_type should be 'projects' or 'publications'"
        )

    try:
        return pd.read_json(
            GTR_INPUTS_DIR / f"gtr_ai_genomics_{entity_type}.json"
        ).rename(columns=lambda x: re.sub(r"(?<!^)(?=[A-Z])", "_", x).lower())
    except ValueError as e:
        logger.error(
            "ValueError: To create the missing file, run ai_genomics/analysis/gtr_definitions.py"
        )
        raise e
