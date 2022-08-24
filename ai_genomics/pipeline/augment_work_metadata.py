# Scripts to augment the work metadata with abstract information and predicted language
import logging
import os
from toolz import pipe
from functools import partial
from typing import List, Dict

import fasttext
import numpy as np
import pandas as pd

from ai_genomics import PROJECT_DIR
from ai_genomics.getters.openalex import work_abstracts

OALEX_PATH = f"{PROJECT_DIR}/inputs/data/openalex"

model = fasttext.load_model(f"{PROJECT_DIR}/inputs/models/lid.176.ftz")


def predict_language(text: str, model: fasttext.FastText._FastText) -> Dict:
    """Uses a fasttext model to predict the language"""

    if type(text) != str:
        return {"predicted_language": np.nan, "language_probability": np.nan}
    else:
        return pipe(
            text.lower(),
            partial(model.predict, k=1),
            lambda preds: {
                "predicted_language": preds[0][0].split("__")[-1],
                "language_probability": float(preds[1]),
            },
        )


if __name__ == "__main__":

    for year in range(2007, 2022):

        logging.info(year)

        for discipline in ["artificial_intelligence", "genetics"]:

            logging.info(f"processing works for {discipline} and {year}")

            if os.path.exists(f"{OALEX_PATH}/works_{discipline}_{year}_augmented.csv"):
                logging.info("Already exists")

            else:
                (
                    pipe(
                        f"{OALEX_PATH}/works_{discipline}_{year}.csv",
                        pd.read_csv,
                        lambda df: pd.concat(
                            [
                                df,
                                pd.DataFrame(
                                    df["display_name"]
                                    .str.replace("\n", "")
                                    .apply(lambda title: predict_language(title, model))
                                    .tolist()
                                ),
                            ],
                            axis=1,
                        ),
                    )
                    .assign(
                        has_abstract=lambda df: df["work_id"].map(
                            {
                                work_id: True if pd.isnull(abstract) == False else False
                                for work_id, abstract in work_abstracts(
                                    discipline, [year]
                                ).items()
                            }
                        )
                    )
                    .to_csv(
                        f"{OALEX_PATH}/works_{discipline}_{year}_augmented.csv",
                        index=False,
                    )
                )
