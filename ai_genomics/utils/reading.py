import json


def read_json(data: Union[pathlib.Path, str]) -> dict:
    """
    Reads the json file and returns the data
    """
    with open(data) as json_file:
        return json.load(json_file)
