import pytest

from ai_genomics.pipeline.openalex.works_pipeline import api_generator

CONCEPT_IDS = ["C189206191"]
API_ROOT = "https://api.openalex.org/works?filter="


def test_api_generator():
    results = api_generator(API_ROOT, CONCEPT_IDS)
    assert len(results) > 0
