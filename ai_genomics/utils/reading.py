import json
import pathlib
from typing import Union, Dict, List, Any


def read_json(data: Union[pathlib.Path, str]) -> List[Dict[str, Any]]:
    """
    Reads the json file and returns the data
    """
    with open(data) as json_file:
        return json.load(json_file)
