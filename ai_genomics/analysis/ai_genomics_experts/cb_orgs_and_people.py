from ai_genomics.getters.crunchbase import get_ai_genomics_crunchbase_org_ids
from ai_genomics.utils.crunchbase import fetch_crunchbase, parse_s3_table
from ai_genomics import PROJECT_DIR
import pandas as pd

EXPERTS_PATH = PROJECT_DIR / "outputs/data/experts/"


def add_and_process_orgs_data(
    ai_gen_cb_orgs: pd.DataFrame, cb_orgs: pd.DataFrame
) -> pd.DataFrame:
    """To AI Genomics Crunchase organisations dataframe this:
        - Adds additional organisation related columns
        - Creates `description` column which uses long_description
            or short_description depending what is available
        - Selects relevant columns
        - Filters out closed organisations

    Args:
        ai_gen_cb_orgs: AI genomics Crunchbase organisations
        cb_orgs: Crunchbase organisations (to get additional
            columns from)

    Returns:
        Processed AI genomics Crunchbase organisations
    """
    return (
        ai_gen_cb_orgs.merge(cb_orgs, how="left", left_on="cb_org_id", right_on="id")
        .assign(description=lambda x: x.long_description.fillna(x.short_description))[
            [
                "cb_org_id",
                "name",
                "description",
                "cb_url",
                "homepage_url",
                "total_funding_usd",
                "country",
                "roles",
                "closed_on",
                "employee_count",
            ]
        ]
        .query("closed_on.isnull()")
        .drop(columns="closed_on")
    )


def select_and_rename_cb_people_cols(cb_people: pd.DataFrame) -> pd.DataFrame:
    """Select and rename columns for Crunchbase people dataframe"""
    return cb_people[
        [
            "uuid",
            "name",
            "cb_url",
            "featured_job_organization_uuid",
            "featured_job_organization_name",
            "featured_job_title",
        ]
    ].rename(
        columns={
            "uuid": "person_id",
            "name": "person_name",
            "featured_job_organization_uuid": "org_id",
            "featured_job_organization_name": "org_name",
            "featured_job_title": "job_title",
        }
    )


def find_top_n_most_funded_ai_genomics_orgs(
    ai_gen_cb_orgs: pd.DataFrame, n: int
) -> pd.DataFrame:
    """Find the top n most funded AI genomics Crunchbase organisations

    Args:
        ai_gen_cb_orgs: AI genomics Crunchbase organisations
        n: Number of top companies to select

    Returns:
        Top n most funded AI genomics organisations
    """
    return (
        ai_gen_cb_orgs.dropna(subset="total_funding_usd")
        .astype({"total_funding_usd": float})
        .sort_values(by=["total_funding_usd"], ascending=False)
        .query("roles == 'company'")
        .head(n)
        .reset_index(drop=True)
    )


def find_people_related_to_orgs(
    cb_people: pd.DataFrame, cb_org_ids: list
) -> pd.DataFrame:
    """Find Crunchbase people related to specified cb_org_ids

    Args:
        cb_people: Crunchbase people dataframe
        cb_org_ids: List of Crunchbase org ids

    Returns:
        Crunchbase people related to specified cb_org_ids
    """
    return cb_people.query(f"org_id in {cb_org_ids}").reset_index(drop=True)


if __name__ == "__main__":
    cb_orgs = parse_s3_table(fetch_crunchbase("orgs"))

    ai_gen_cb_orgs = get_ai_genomics_crunchbase_org_ids().pipe(
        add_and_process_orgs_data, cb_orgs
    )

    cb_people = parse_s3_table(fetch_crunchbase("people")).pipe(
        select_and_rename_cb_people_cols
    )

    ai_gen_investor_orgs = ai_gen_cb_orgs.query(
        "roles in ['investor,company', 'investor']"
    )

    ai_gen_investor_people = find_people_related_to_orgs(
        cb_people, ai_gen_investor_orgs.cb_org_id.to_list()
    )

    ai_gen_top_20_funded_orgs = find_top_n_most_funded_ai_genomics_orgs(
        ai_gen_cb_orgs, 20
    )
    ai_gen_top_funded_related_people = find_people_related_to_orgs(
        cb_people, ai_gen_top_20_funded_orgs.cb_org_id.to_list()
    )

    to_save = {
        "ai_genomics_investor_orgs": ai_gen_investor_orgs,
        "ai_genomics_investor_people": ai_gen_investor_people,
        "ai_genomics_top_20_funded_orgs": ai_gen_top_20_funded_orgs,
        "ai_genomics_top_funded_related_people": ai_gen_top_funded_related_people,
    }

    EXPERTS_PATH.mkdir(parents=True, exist_ok=True)

    for file_name, table in to_save.items():
        table.to_csv(EXPERTS_PATH / f"{file_name}.csv", index=False)
