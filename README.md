# :alien: AI in Genomics Innovation Mapping

**_Open-source code for innovation mapping of the AI in genomics landscape_**

## :wave: Welcome!

This repo contains the source code behind the AI in genomics innovation mapping exercise conducted for
[the Ada Lovelace Institute](https://www.adalovelaceinstitute.org/) and the [Nuffield Council for Bioethics](https://www.nuffieldbioethics.org/).

## :floppy_disk: Datasets (`ai_genomics/pipeline/`)

To uncover trends in the space, we are presently using the following data:

- **[Gateway to Research (GtR)](https://github.com/nestauk/ai_genomics/tree/project_readme/ai_genomics/pipeline/gtr)**: Research projects funded by UKRI
- **[Crunchbase](https://github.com/nestauk/ai_genomics/tree/project_readme/ai_genomics/pipeline/crunchbase_data)**: Global company directory
- **[OpenAlex](https://github.com/nestauk/ai_genomics/tree/project_readme/ai_genomics/pipeline/openalex)**: Global research paper catalogue
- **[Google Patents](https://github.com/nestauk/ai_genomics/tree/project_readme/ai_genomics/pipeline/patent_data)**: Inventions around the world

All these datasets except Crunchbase are freely available. Note, however, that this project accesses the datasets via our internal Nesta database and as such are intended for internal use.

## :floppy_disk: Data Enrichment (`ai_genomics/pipeline/entity_extraction`)

We enrich the above datasets by extracting [DBpedia entities](https://github.com/nestauk/service_dbpedia_annotation_python_sdk) across them.

We post-process the DBpedia entities in this repo.

## :rotating_light: AI in genomics topics (`ai_genomics/pipeline/`)

We identify main topics in the landscape via three **approaches**:

1. [Clustering semantic embeddings of texts across the datasets](https://github.com/nestauk/ai_genomics/tree/project_readme/ai_genomics/pipeline/doc_cluster);
2. [Clustering semantic embeddings of DBpedia entities](https://github.com/nestauk/ai_genomics/blob/project_readme/ai_genomics/pipeline/entity_cluster/create_entity_clusters.py) associated to the dataset texts.
3. Manually clustering Crunchbase data.

## :tv: Dimensions of Innovation (`ai_genomics/analysis/`)

We primarily map out the landscape using the identified topics via the following dimensions:

1. **Evolution**: This method describes the change in DBpedia macro-entities over time across all datasets;
2. [**Influence**](https://github.com/nestauk/ai_genomics/tree/project_readme/ai_genomics/analysis/influence): This method calculates AI vs. Genomics influence in papers across key datasets;
3. [**Emergence**](https://github.com/nestauk/ai_genomics/tree/project_readme/ai_genomics/analysis/integrated_emergence): This method uses [Innovation Sweet Spot's](https://github.com/nestauk/innovation_sweet_spots) typology of innovation to identify emergent, hot and stabilising topics across and between datasets
4. **Distribution**: This method explores the 'Revealed Comparative Advantage' (RCA) of institutions and regions in the AI and genomics topics defined by our clusters.

We also explore high level trends in patents and publications.

## Setup

- Meet the data science cookiecutter [requirements](http://nestauk.github.io/ds-cookiecutter/quickstart), in brief:
  - Install: `git-crypt`, `direnv`, and `conda`
  - Have a Nesta AWS account configured with `awscli`
- Run `make install` to configure the development environment:
  - Setup the conda environment
  - Configure pre-commit
  - Configure metaflow to use AWS

Download spaCy's en_core_web_sm 3.1.0 pipeline:

```bash
pip install https://github.com/explosion/spacy-models/releases/download/en_core_web_sm-3.1.0/en_core_web_sm-3.1.0.tar.gz
```

## Contributor guidelines

[Technical and working style guidelines](https://github.com/nestauk/ds-cookiecutter/blob/master/GUIDELINES.md)

---

<small><p>Project based on <a target="_blank" href="https://github.com/nestauk/ds-cookiecutter">Nesta's data science project template</a>
(<a href="http://nestauk.github.io/ds-cookiecutter">Read the docs here</a>).
</small>
