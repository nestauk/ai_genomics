from ai_genomics.getters.gtr import get_ai_genomics_gtr_data, get_gtr_from_s3
from ai_genomics import PROJECT_DIR
import pandas as pd

EXPERTS_PATH = PROJECT_DIR / "outputs/data/experts/"


def process_gtr_people(gtr_people: pd.DataFrame) -> pd.DataFrame:
    """Add new column combining first name and surnam, select relevant columns"""
    return gtr_people.assign(full_name=lambda x: x.firstName + " " + x.surname)[
        ["project_id", "full_name", "rel"]
    ].rename(columns={"rel": "person_rel"})


if __name__ == "__main__":

    gtr_people = get_gtr_from_s3("persons").pipe(process_gtr_people)

    ai_gen_gtr_projs_with_people = (
        get_ai_genomics_gtr_data("projects")
        .merge(gtr_people, how="left", on="project_id")
        .query("person_rel.notnull()")
        .reset_index(drop=True)[
            [
                "project_id",
                "title",
                "abstract_text",
                "potential_impact",
                "full_name",
                "lead_funder",
            ]
        ]
    )

    EXPERTS_PATH.mkdir(parents=True, exist_ok=True)

    ai_gen_gtr_projs_with_people.to_csv(
        EXPERTS_PATH / "ai_genomics_gtr_projects_with_people.csv", index=False
    )
