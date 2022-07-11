import json


def read_json(data: Union[pathlib.Path, str]) -> dict:
    """
    Reads the json file and returns the data
    """
    with open(f"{data}") as json_file:
        data = json.load(json_file)
    return data
