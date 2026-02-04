#!/usr/bin/env python3
"""
Federal Awards Star Schema Transformer
Author: Mboya Jeffers

Transforms raw USASpending awards into analytics-ready star schema.
Handles 1M+ rows with proper dimensional modeling.
"""

import pandas as pd
import numpy as np
from typing import Dict
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_dim_agency(awards_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create agency dimension from awards.

    Returns:
        DataFrame with agency_id, agency_code, agency_name, sub_agency
    """
    # Get unique agency combinations
    agencies = awards_df[["awarding_agency", "awarding_sub_agency"]].drop_duplicates()
    agencies = agencies.dropna(subset=["awarding_agency"])
    agencies = agencies.reset_index(drop=True)
    agencies["agency_id"] = agencies.index + 1

    # Rename columns
    agencies = agencies.rename(columns={
        "awarding_agency": "agency_name",
        "awarding_sub_agency": "sub_agency"
    })

    # Create agency code from name
    agencies["agency_code"] = agencies["agency_name"].apply(
        lambda x: "".join(word[0] for word in str(x).split()[:3]).upper() if x else "UNK"
    )

    logger.info(f"Created dim_agency: {len(agencies)} agencies")
    return agencies[["agency_id", "agency_code", "agency_name", "sub_agency"]]


def create_dim_recipient(awards_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create recipient dimension from awards.

    Returns:
        DataFrame with recipient_id, recipient_name, business_type
    """
    # Get unique recipients
    recipients = awards_df[["recipient_name"]].drop_duplicates()
    recipients = recipients.dropna(subset=["recipient_name"])
    recipients = recipients.reset_index(drop=True)
    recipients["recipient_id"] = recipients.index + 1

    # Classify recipient type
    def classify_recipient(name):
        name_lower = str(name).lower()
        if any(x in name_lower for x in ["llc", "inc", "corp", "ltd", "company"]):
            return "Corporation"
        elif any(x in name_lower for x in ["university", "college", "school"]):
            return "Educational"
        elif any(x in name_lower for x in ["hospital", "medical", "health"]):
            return "Healthcare"
        elif any(x in name_lower for x in ["state of", "county", "city of", "government"]):
            return "Government"
        elif any(x in name_lower for x in ["foundation", "nonprofit", "association"]):
            return "Nonprofit"
        else:
            return "Other"

    recipients["recipient_type"] = recipients["recipient_name"].apply(classify_recipient)

    logger.info(f"Created dim_recipient: {len(recipients)} recipients")
    logger.info(f"Recipient types: {recipients['recipient_type'].value_counts().to_dict()}")

    return recipients[["recipient_id", "recipient_name", "recipient_type"]]


def create_dim_location(awards_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create location dimension from awards.

    Returns:
        DataFrame with location_id, city, state_code, state_name, zip_code
    """
    # Get unique locations
    locations = awards_df[["place_city", "place_state", "place_zip"]].drop_duplicates()
    locations = locations.reset_index(drop=True)
    locations["location_id"] = locations.index + 1

    # State name mapping
    STATE_NAMES = {
        "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
        "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
        "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
        "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
        "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
        "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
        "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
        "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
        "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
        "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
        "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
        "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
        "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
        "PR": "Puerto Rico", "VI": "Virgin Islands", "GU": "Guam"
    }

    locations["state_name"] = locations["place_state"].map(STATE_NAMES)

    # Rename columns
    locations = locations.rename(columns={
        "place_city": "city",
        "place_state": "state_code",
        "place_zip": "zip_code"
    })

    logger.info(f"Created dim_location: {len(locations)} locations")

    return locations[["location_id", "city", "state_code", "state_name", "zip_code"]]


def create_dim_date(awards_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create date dimension from awards.

    Returns:
        DataFrame with date_id, fiscal_year, fiscal_quarter, calendar_year, calendar_month
    """
    # Get unique fiscal years
    years = awards_df["fiscal_year"].dropna().unique()

    date_records = []
    date_id = 1

    for fy in sorted(years):
        # Annual record
        date_records.append({
            "date_id": date_id,
            "fiscal_year": int(fy),
            "fiscal_quarter": "FY",
            "quarter_label": f"FY{int(fy)}"
        })
        date_id += 1

        # Quarterly records
        for q in range(1, 5):
            date_records.append({
                "date_id": date_id,
                "fiscal_year": int(fy),
                "fiscal_quarter": f"Q{q}",
                "quarter_label": f"FY{int(fy)}Q{q}"
            })
            date_id += 1

    dates = pd.DataFrame(date_records)

    logger.info(f"Created dim_date: {len(dates)} periods")

    return dates


def create_fact_awards(
    awards_df: pd.DataFrame,
    dim_agency: pd.DataFrame,
    dim_recipient: pd.DataFrame,
    dim_location: pd.DataFrame,
    dim_date: pd.DataFrame
) -> pd.DataFrame:
    """
    Create fact table by joining dimensions.

    Returns:
        Fact table with dimension keys and measures
    """
    logger.info(f"Creating fact_awards from {len(awards_df):,} raw awards...")

    # Merge agency dimension
    agency_lookup = dim_agency[["agency_id", "agency_name", "sub_agency"]]
    fact = awards_df.merge(
        agency_lookup,
        left_on=["awarding_agency", "awarding_sub_agency"],
        right_on=["agency_name", "sub_agency"],
        how="left"
    )

    # Merge recipient dimension
    recipient_lookup = dim_recipient[["recipient_id", "recipient_name"]]
    fact = fact.merge(
        recipient_lookup,
        on="recipient_name",
        how="left"
    )

    # Merge location dimension
    location_lookup = dim_location[["location_id", "city", "state_code", "zip_code"]]
    fact = fact.merge(
        location_lookup,
        left_on=["place_city", "place_state", "place_zip"],
        right_on=["city", "state_code", "zip_code"],
        how="left"
    )

    # Merge date dimension (annual level)
    date_lookup = dim_date[dim_date["fiscal_quarter"] == "FY"][["date_id", "fiscal_year"]]
    fact = fact.merge(
        date_lookup,
        on="fiscal_year",
        how="left"
    )

    # Select columns for fact table
    fact_table = fact[[
        "award_id", "agency_id", "recipient_id", "location_id", "date_id",
        "award_amount", "total_outlays", "award_type",
        "naics_code", "cfda_number", "description",
        "start_date", "end_date"
    ]].copy()

    # Add surrogate key
    fact_table = fact_table.reset_index(drop=True)
    fact_table["fact_id"] = fact_table.index + 1

    # Reorder columns
    fact_table = fact_table[[
        "fact_id", "award_id", "agency_id", "recipient_id", "location_id", "date_id",
        "award_amount", "total_outlays", "award_type",
        "naics_code", "cfda_number", "description",
        "start_date", "end_date"
    ]]

    logger.info(f"Created fact_awards: {len(fact_table):,} rows")

    return fact_table


def transform_to_star_schema(awards_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Transform raw awards into star schema.

    Args:
        awards_df: Raw extracted awards

    Returns:
        Dict of dimension and fact DataFrames
    """
    logger.info(f"\n{'='*60}")
    logger.info("TRANSFORMING TO STAR SCHEMA")
    logger.info(f"{'='*60}")
    logger.info(f"Input rows: {len(awards_df):,}")

    # Create dimensions
    dim_agency = create_dim_agency(awards_df)
    dim_recipient = create_dim_recipient(awards_df)
    dim_location = create_dim_location(awards_df)
    dim_date = create_dim_date(awards_df)

    # Create fact table
    fact_awards = create_fact_awards(
        awards_df, dim_agency, dim_recipient, dim_location, dim_date
    )

    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("STAR SCHEMA SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"dim_agency:    {len(dim_agency):>10,} rows")
    logger.info(f"dim_recipient: {len(dim_recipient):>10,} rows")
    logger.info(f"dim_location:  {len(dim_location):>10,} rows")
    logger.info(f"dim_date:      {len(dim_date):>10,} rows")
    logger.info(f"fact_awards:   {len(fact_awards):>10,} rows")
    logger.info(f"{'='*60}")

    return {
        "dim_agency": dim_agency,
        "dim_recipient": dim_recipient,
        "dim_location": dim_location,
        "dim_date": dim_date,
        "fact_awards": fact_awards
    }


def validate_star_schema(schema: Dict[str, pd.DataFrame]) -> Dict[str, any]:
    """
    Validate star schema integrity.
    """
    logger.info("\nValidating star schema...")

    results = {
        "passed": True,
        "checks": []
    }

    fact = schema["fact_awards"]

    # Check for orphan dimension keys
    for dim_col in ["agency_id", "recipient_id", "location_id", "date_id"]:
        orphans = fact[fact[dim_col].isna()]
        results["checks"].append({
            "check": f"orphan_{dim_col}",
            "passed": len(orphans) < len(fact) * 0.1,  # Allow up to 10% nulls
            "count": len(orphans),
            "percentage": round(len(orphans) / len(fact) * 100, 2)
        })

    # Check award amounts
    null_amounts = fact["award_amount"].isna().sum()
    results["checks"].append({
        "check": "null_award_amounts",
        "passed": null_amounts < len(fact) * 0.05,
        "count": null_amounts,
        "percentage": round(null_amounts / len(fact) * 100, 2)
    })

    # Overall
    results["passed"] = all(c["passed"] for c in results["checks"])

    for check in results["checks"]:
        status = "PASS" if check["passed"] else "WARN"
        logger.info(f"  [{status}] {check['check']}: {check.get('count', 'N/A')} ({check.get('percentage', 0)}%)")

    return results


if __name__ == "__main__":
    # Test transformation
    test_data = pd.read_parquet("./data/raw_awards.parquet")
    print(f"Loaded {len(test_data):,} raw awards")

    schema = transform_to_star_schema(test_data)
    validation = validate_star_schema(schema)

    print(f"\nValidation: {'PASSED' if validation['passed'] else 'WARNINGS'}")
