"""This script splits the OpenAlex entities into ai, ai_genomics and genomics entitiy lookups 
and saves a sample of the entity lookup.  
"""
from ai_genomics.getters.data_getters import save_to_s3
from ai_genomics.getters.openalex import (
    get_openalex_entities,
    get_openalex_ai_genomics_works,
)
from ai_genomics import logger, bucket_name
from typing import Union, Mapping, List
import random
from datetime import datetime

# set random seed
random.seed(42)
SAMPLE_SIZE = 5000

oa_ents = get_openalex_entities()
oa_works = get_openalex_ai_genomics_works()

ai_ids, ai_genomics_ids, genomics_ids = (
    list(oa_works.query("ai == True").work_id),
    list(oa_works.query("ai_genomics == True").work_id),
    list(oa_works.query("genomics == True").work_id),
)


def filter_entities(
    ids: List[str], ents_dict: Mapping[str, Mapping[str, Union[str, str]]] = oa_ents
):
    """filters entities based on list of ids"""
    filtered_oa_ents = dict()
    for id_ in ids:
        filtered_oa_ents[id_] = ents_dict.get(id_)
    return filtered_oa_ents


if __name__ == "__main__":
    start_time = datetime.now()

    logger.info("saving genomics ents...")
    genomics_oa_ents = filter_entities(ids=genomics_ids)
    save_to_s3(
        bucket_name,
        genomics_oa_ents,
        "outputs/entity_extraction/oa_genomics_lookup_clean.json",
    )

    logger.info("saving ai genomics ents...")
    ai_genomics_oa_ents = filter_entities(ids=ai_genomics_ids)
    save_to_s3(
        bucket_name,
        ai_genomics_oa_ents,
        "outputs/entity_extraction/oa_ai_genomics_lookup_clean.json",
    )

    logger.info("saving ai ents...")
    ai_ents = filter_entities(ids=ai_genomics_ids)
    save_to_s3(
        bucket_name, ai_ents, "outputs/entity_extraction/oa_ai_lookup_clean.json"
    )

    # just save a sample
    oa_ents_sample = (
        dict(random.sample(ai_ents.items(), SAMPLE_SIZE))
        | dict(random.sample(genomics_oa_ents.items(), SAMPLE_SIZE))
        | dict(random.sample(ai_genomics_oa_ents.items(), SAMPLE_SIZE))
    )
    save_to_s3(
        bucket_name,
        oa_ents_sample,
        "outputs/entity_extraction/oa_lookup_clean_sample.json",
    )

    logger.info(
        f"took {datetime.now() - start_time} to filter and generate samples of oa tags."
    )
