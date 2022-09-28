"""Class to post process extracted entities using
spacy's pretrained NER model where expected input of extracted
entities includes both the DBpedia URI and confidence score.

Filter out entities that are: people, organisations, locations

For example,

entities = {'EP-3810804-A1': [{'URI': 'http://dbpedia.org/resource/Alternative_splicing',
   'confidence': 80},
  {'URI': 'http://dbpedia.org/resource/Genomics', 'confidence': 70},
  {'URI': 'http://dbpedia.org/resource/Transcriptome', 'confidence': 100},
  {'URI': 'http://dbpedia.org/resource/Protein', 'confidence': 90},
  {'URI': 'http://dbpedia.org/resource/RNA', 'confidence': 90},
  {'URI': 'http://dbpedia.org/resource/Machine_learning', 'confidence': 90}],
 'EP-1967857-A3': [],
 'US-2007134705-A1': [{'URI': 'http://dbpedia.org/resource/Transcription_factor',
   'confidence': 90},
  {'URI': 'http://dbpedia.org/resource/Gene', 'confidence': 90},
  {'URI': 'http://dbpedia.org/resource/Interval_graph', 'confidence': 100}],
 'US-8921074-B2': [{'URI': 'http://dbpedia.org/resource/Colorectal_cancer',
   'confidence': 100},
  {'URI': 'http://dbpedia.org/resource/Gene', 'confidence': 90},
  {'URI': 'http://dbpedia.org/resource/IL2RB', 'confidence': 90},
  {'URI': 'http://dbpedia.org/resource/TSG-6', 'confidence': 90},
  {'URI': 'http://dbpedia.org/resource/RNA', 'confidence': 90}],
 'WO-2020092591-A1': [{'URI': 'http://dbpedia.org/resource/Germline',
   'confidence': 70},
  {'URI': 'http://dbpedia.org/resource/Genome', 'confidence': 90},
  {'URI': 'http://dbpedia.org/resource/Allele', 'confidence': 90}]}

clean_entities = {'EP-3810804-A1': [['Alternative splicing', 80],
  ['Genomics', 70],
  ['Protein', 90],
  ['RNA', 90],
  ['Machine learning', 90]],
 'EP-1967857-A3': [],
 'US-2007134705-A1': [['Transcription factor', 90],
  ['Gene', 90],
  ['Interval graph', 100]],
 'US-8921074-B2': [['Colorectal cancer', 100],
  ['Gene', 90],
  ['TSG-6', 90],
  ['RNA', 90]],
 'WO-2020092591-A1': [['Germline', 70], ['Genome', 90]]}

If script is run, evaluates labelled entities and applies clean_entities
to toy extracted entity dictionary.
"""
from ai_genomics.getters.data_getters import load_s3_data, save_to_s3
from ai_genomics import bucket_name, logger

import pandas as pd
import ast
import spacy
from datetime import datetime as date
import itertools
from sklearn.metrics import precision_score, recall_score, f1_score, accuracy_score
from typing import List, Dict

BAD_ENTS = ["ORG", "GPE", "MONEY", "LOC", "PERSON"]


class EntityCleaner:
    """
    Class that applies spaCy's pretrained NER model to predict entity types and
    filter entities for a given entity list that are labelled as
    people, organisations, locations or money.

    Attributes
    --------
    entity_lookup_path: s3 location of labelled entities file
    bad_ents: list of spaCy entity types to remove
    ner: Spacy's model
    save_eval: Boolean to save evaluation results or not

    Methods
    --------
    clean_entity_col(entity_col): cleans up entity column in labelled
                                    dataset to just extract entity
                                    and ignore entity score.
    format_labelled_data: Formats labelled dataset to be a dictionary
                            where key is id and value is list of
                            predicted entities where 1 == bad entity
                            and 0 == good entity.
    predict(entity_list): predicts 1== bad entity or 0 == good entity
                            based on spaCy's pretrained NER model.
                            entity is 'bad' if its entity type is in
                            bad_ents.
    filter_entities(entity_list): given the entity prediction, return
                                    entity list with only 'good' predictions.
    evaluate(y_pred, y_true): calculates f1, recall, accuracy and precision
                                of two lists of binaries. If no y_pred and
                                y_true lists are passed, use formatted
                                labelled dataset in s3.
    """

    def __init__(
        self,
        labelled_entity_path="inputs/ai_genomics_samples/db_tag_entities_to_filter.csv",
        ner=spacy.load("en_core_web_sm"),
        bad_entities=BAD_ENTS,
        save_eval=True,
    ):
        self.labelled_entity_path = labelled_entity_path
        self.ner = ner
        self.bad_entities = bad_entities
        self.save_eval = save_eval

    def clean_entity_col(self, entities):
        """FOR LABELLED DATA - cleans and extracts entity from entity col"""

        entities = ast.literal_eval(entities)
        if isinstance(entities, tuple):
            entities = list(map(list, entities))
        if not isinstance(entities[0], list):
            entities = [entities]

        return [ent[0].replace("_", " ") for ent in entities]

    def format_labelled_data(self):
        """FOR LABELLED DATA - formats labelled data to have binary labels
        where 0 means entity is 'good' and 1 means entity is 'bad'
        """

        self.labelled_entities = (
            load_s3_data(bucket_name, self.labelled_entity_path)
            .query("~bad_entities.isna()")
            .reset_index(drop=True)
        )

        for col in ("entities", "bad_entities"):
            self.labelled_entities[col] = self.labelled_entities[col].apply(
                self.clean_entity_col
            )

        labelled_ents = list()
        for ent, bad_ent in zip(
            self.labelled_entities.entities, self.labelled_entities.bad_entities
        ):
            bad_ent_indx = [ent.index(e) for e in bad_ent]
            labelled_ents.extend(
                [1 if indx in bad_ent_indx else 0 for indx, e in enumerate(ent)]
            )

        return labelled_ents

    def predict(self, entities_list: List[str]) -> List[int]:
        """predict whether entity is good (==0) or not (==1) for a given entities list"""
        labels = []
        for i, ent in enumerate(entities_list):
            bad_ents = []
            doc = self.ner(ent)
            if doc.ents:
                for tok in doc.ents:
                    if tok.label_ in self.bad_entities:
                        bad_ents.append(entities_list[i])
            bad_ents_deduped = list(set(bad_ents))
            if bad_ents_deduped != []:
                labels.append(1)
            else:
                labels.append(0)

        return labels

    def filter_entities(self, entities_list: List[Dict]) -> List[str]:
        """filters entities list based on predicted 'bad' entities"""
        ents = [
            [ent["URI"].split("/")[-1].replace("_", " "), ent["confidence"]]
            for ent in entities_list
        ]
        ent_preds = self.predict([ent[0] for ent in ents])

        return [ents[i] for i, pred in enumerate(ent_preds) if pred != 1]

    def evaluate(
        self, y_true: List[int] = None, y_pred: List[int] = None
    ) -> Dict[str, int]:
        """evaluate pretrained NER model based on labelled entities

        If no y_true and y_pred passed, use formatted labelled entities dataset.
        """
        if not y_true:
            y_true = self.format_labelled_data()

        if not y_pred:
            labelled_ents_lookup = self.labelled_entities["entities"].to_dict()

            y_pred = []
            for entity_list in labelled_ents_lookup.values():
                y_pred.extend(self.predict(entity_list))

        scores = {
            "accuracy": accuracy_score(y_true, y_pred),
            "precision": precision_score(y_true, y_pred),
            "recall": recall_score(y_true, y_pred),
            "f1": f1_score(y_true, y_pred),
        }

        if self.save_eval:
            date_stamp = str(date.today().date()).replace("-", "")
            save_to_s3(
                bucket_name,
                scores,
                f"outputs/entity_extraction/{date_stamp}_ner_postprocesser_results.json",
            )

        else:
            logger.info(
                f"results from applying spaCy's pretrained NER model to filter 'bad' entities: {scores}"
            )


if __name__ == "__main__":

    ec = EntityCleaner(save_eval=False)
    ec.evaluate()  # print evaluation metrics based on labelled dataset of 'bad' and 'good' entities

    test_entities = {
        "AU-2019293244-A1": [
            {"URI": "http://dbpedia.org/resource/Cancer_vaccine", "confidence": 90},
            {"URI": "http://dbpedia.org/resource/Cancer", "confidence": 70},
            {
                "URI": "http://dbpedia.org/resource/Biodiversity_hotspot",
                "confidence": 70,
            },
        ],
        "CA-2578634-A1": [
            {"URI": "http://dbpedia.org/resource/Breed", "confidence": 70}
        ],
        "CA-2577741-A1": [],
        "US-6920397-B2": [
            {"URI": "http://dbpedia.org/resource/Signal_processing", "confidence": 100},
            {"URI": "http://dbpedia.org/resource/Tessellation", "confidence": 90},
            {"URI": "http://dbpedia.org/resource/Interferometry", "confidence": 90},
            {"URI": "http://dbpedia.org/resource/Resonance", "confidence": 90},
            {
                "URI": "http://dbpedia.org/resource/Stochastic_resonance",
                "confidence": 70,
            },
            {"URI": "http://dbpedia.org/resource/Stochastic", "confidence": 80},
        ],
        "WO-2019046347-A9": [
            {"URI": "http://dbpedia.org/resource/Microorganism", "confidence": 90}
        ],
    }

    clean_entities = {
        text_id: ec.filter_entities(entity) for text_id, entity in test_entities.items()
    }  # apply clean entities to list and print results
    print(clean_entities)
