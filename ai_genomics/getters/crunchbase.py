from ai_genomics import PROJECT_DIR, logger
import pandas as pd


def get_ai_genomics_crunchbase_org_ids() -> pd.DataFrame:
    """Returns dataframe of AI and Genomics crunchbase organisation ids"""
    try:
        return pd.read_csv(
            PROJECT_DIR / "inputs/data/crunchbase/ai_genomics_org_ids.csv",
            index_col=0,
            names=["cb_org_id"],
            skiprows=[0],
        )
    except FileNotFoundError as e:
        logger.error(
            "FileNotFoundError: To create the missing file, run ai_genomics/analysis/crunchbase_definitions.py"
        )
        raise e
