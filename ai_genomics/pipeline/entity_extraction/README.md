### Entity Extraction

To generate look up tables for each data source accross AI and genomics, AI, and genomics, run `python ai_genomics/pipeline/entity_extraction/generate_lookups.py`.

This will output a series of `.json` files in s3 in `inputs/lookup_tables/`. Each file is in the format `{id: text}`.

To postprocess extracted entities in the anticipated format `{id: [entities]}`:

```
from ai_genomics.pipeline.entity_extraction.postprocess_entities import EntityCleaner

ec = EntityCleaner()
clean_entities = {text_id: ec.filter_entities(entity) for text_id, entity in entities.items()}
```

where `entities = {id: [entities]}`

You can also run the script (`python ai_genomics/pipeline/entity_extraction/postprocess_entities.py`) to print accuracy, f1, recall and precision from a labelled dataset with labelled 'good' or 'bad' entities.