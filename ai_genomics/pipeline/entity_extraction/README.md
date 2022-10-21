### Entity Extraction

## Lookups for entity extraction and entity postprocessing

To generate look up tables for each data source accross AI and genomics, AI, and genomics, run `python ai_genomics/pipeline/entity_extraction/generate_lookups.py`.

This will output a series of `.json` files in s3 in `inputs/lookup_tables/`. Each file is in the format `{id: text}`.

To postprocess extracted entities in the anticipated format `{id: [[entities, confidence_score]]}`:

```
from ai_genomics.pipeline.entity_extraction.postprocess_entities import EntityCleaner

ec = EntityCleaner()
clean_entities = {text_id: ec.filter_entities(entity) for text_id, entity in entities.items()}
```

where `entities = {id: [[entities, confidence_score]]}`

You can also run the script (`python ai_genomics/pipeline/entity_extraction/postprocess_entities.py`) to print accuracy, f1, recall and precision from a labelled dataset with labelled 'good' or 'bad' entities.

## Filter OpenAlex extracted entities

This script splits the lookup based on the ids into ai, ai_genomics, genomics (and samples of each area type) entity lookups and saves the output to s3:

`python ai_genomics/pipeline/entity_extraction/openalex_entity_lookup_samples.py`
