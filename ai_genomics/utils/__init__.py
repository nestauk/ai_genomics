"""Utils for AI and Genomics."""


def id_to_source(uid):
    """Identifies the data source from a unique ID, such as an OpenAlex work ID, patent
    number, GtR project ID or Crunchbase company ID.
    """
    if uid.startswith("https:"):
        return "oa"
    elif len(uid.split("-")) == 3:
        return "pat"
    elif uid.split("-")[0].islower():
        return "cb"
    else:
        return "gtr"
