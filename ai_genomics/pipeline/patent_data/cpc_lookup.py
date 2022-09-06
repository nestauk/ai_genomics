from ai_genomics import PROJECT_DIR
from ai_genomics.utils.io import fetch
from ai_genomics.utils.reading import extractall
from ai_genomics.pipeline.patent_data.cpc_utils import make_cpc_lookup


CPC_SCHEME_URL = (
    "https://www.cooperativepatentclassification.org/"
    "sites/default/files/cpc/bulk/CPCSchemeXML202208.zip"
)

CPC_SCHEME_DIR = PROJECT_DIR / "inputs/patent_data/cpc_scheme/"

CPC_LOOKUP_PATH = PROJECT_DIR / "inputs/patent_data/cpc_code_to_description_lookup.json"

if __name__ == "__main__":

    cpc_scheme = extractall(
        fetch(CPC_SCHEME_URL),
        PROJECT_DIR / "inputs/patent_data/cpc_scheme/",
    )

    make_cpc_lookup(CPC_SCHEME_DIR, CPC_LOOKUP_PATH)
