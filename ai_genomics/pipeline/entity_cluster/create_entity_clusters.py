import json
from sklearn import cluster
from toolz.dicttoolz import merge
from toolz.functoolz import pipe

from ai_genomics import PROJECT_DIR, get_yaml_config, logger
from ai_genomics.utils.reading import make_path_if_not_exist

from ai_genomics.getters.openalex import (
    get_openalex_entities,
    get_openalex_ai_genomics_works,
)
from ai_genomics.getters.patents import (
    get_ai_genomics_patents_entities,
)
from ai_genomics.getters.gtr import (
    get_gtr_entities,
    get_ai_genomics_project_table,
)
from ai_genomics.getters.crunchbase import (
    get_crunchbase_entities,
    get_ai_genomics_crunchbase_orgs,
)
from ai_genomics.pipeline.entity_cluster import (
    embed_entities,
    create_entity_embedding_clusters,
    create_doc_vectors,
)
from ai_genomics.utils.entities import (
    filter_entities,
    strip_scores,
)


CONFIG = get_yaml_config(PROJECT_DIR / "ai_genomics/config/entity_cluster.yaml")
OUT_DIR = PROJECT_DIR / "inputs/entities/"


if __name__ == "__main__":

    logger.info("Fetching and merging entities.")
    gtr_ids = list(get_ai_genomics_project_table().query("ai_genomics == True")["id"])
    oa_ids = list(
        get_openalex_ai_genomics_works().query("ai_genomics == True")["work_id"]
    )
    cb_ids = list(get_ai_genomics_crunchbase_orgs().query("ai_genom == True")["id"])

    oa_entities = get_openalex_entities()
    oa_entities = {k: oa_entities[k] for k in oa_ids}

    gtr_entities = get_gtr_entities()
    gtr_entities = {k: gtr_entities[k] for k in gtr_ids}

    cb_entities = get_crunchbase_entities()
    cb_entities = {k: cb_entities[k] for k in cb_ids}

    patent_entities = get_ai_genomics_patents_entities()

    entities = merge(
        oa_entities,
        gtr_entities,
        cb_entities,
        patent_entities,
    )

    logger.info("Filtering and embedding entities.")
    entities = strip_scores(entities)
    entities = filter_entities(
        entities,
        **CONFIG["filter_entities"],
    )

    embeddings = embed_entities(
        entities,
        **CONFIG["embed"],
    )

    make_path_if_not_exist(OUT_DIR)
    for k, params in CONFIG["cluster"].items():
        logger.info(f"Clustering {k} entities.")
        cluster_lookup = create_entity_embedding_clusters(
            embeddings,
            params,
        )

        with open(OUT_DIR / f"entity_groups_{k}.json", "w") as f:
            json.dump(cluster_lookup, f)

        entity_list = [oa_entities, patent_entities, gtr_entities, cb_entities]
        dataset_names = ["openalex", "patent", "gtr", "crunchbase"]

        for ents, name in zip(entity_list, dataset_names):
            ents = strip_scores(ents)
            doc_vecs = create_doc_vectors(ents, cluster_lookup)
            doc_vecs.to_csv(OUT_DIR / f"{name}_entity_group_vectors_{k}.csv")
