"""Script to post process extracted entities using
spacy's pretrained NER model where expected input of extracted
entities is {id: [entities]} and expected output is {id: [clean_entities]}

Filter out entities that are: people, organisations, locations

For example,

entities = {123324: ['University of Michigan', 'genomics', 'RNA'],
                14234: ['machine learning', 'MIT', 'personalised medicine'],
                1666: ['Yale', 'deep neural networks', 'genetics']}

clean_entitites = {123324: ['genomics', 'RNA'],
                    14234: ['machine learning', 'personalised medicine'],
                    1666: ['deep neural networks', 'genetics']}

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
from typing import List

BAD_ENTS = ["ORG", "GPE", "MONEY", "LOC", "PERSON"]


class EntityCleaner:
    """
    Class that applies spaCy's pretrained NER model to predict entity types and
    filter entities for a given entity list that are predicted
    people, organisations, locations or money.

    Attributes
    --------
    entity_lookup_path: s3 location of labelled entities file
    bad_ents: list of spaCy entity types to remove

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
    clean_entities(entity_list): given the entity prediction, return
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
        """FOR LABELLED DATA - formats labelled data to have binary labels"""

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

    def predict(self, entities_list):
        """predict whether entity is good (==0) or not (==1) for a given entities list"""
        labels = []
        for i, ent in enumerate(entities_list):
            bad_ents = []
            doc = self.ner(ent)
            if doc.ents:
                for tok in doc.ents:
                    if tok.label_ in BAD_ENTS:
                        bad_ents.append(entities_list[i])
            bad_ents_deduped = list(set(bad_ents))
            if bad_ents_deduped != []:
                labels.append(1)
            else:
                labels.append(0)

        return labels

    def clean_entities(self, entities_list):
        """filters entities list based on predicted 'bad' entities"""

        ent_preds = self.predict(entities_list)

        return [entities_list[i] for i, pred in enumerate(ent_preds) if pred != 1]

    def evaluate(self, y_true: List[int] = None, y_pred: List[int] = None):
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

    entities = {
        123324: ["University of Michigan", "genomics", "RNA", "Harvard"],
        14234: ["machine learning", "MIT", "personalised medicine"],
        1666: ["Yale", "deep neural networks", "genetics"],
    }
    ec.evaluate()  # print evaluation metrics based on labelled dataset of 'bad' and 'good' entities

    clean_entities = {
        text_id: ec.clean_entities(entity) for text_id, entity in entities.items()
    }  # apply clean entities to list
    print(clean_entities)
