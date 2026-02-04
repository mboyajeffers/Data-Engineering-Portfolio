#!/usr/bin/env python3
"""
Medicare Part D Star Schema Transformer
Author: Mboya Jeffers

Transforms raw CMS Part D data into analytics-ready star schema.
Handles 5M+ rows with proper dimensional modeling.
"""

import pandas as pd
import numpy as np
from typing import Dict
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Drug classification patterns
OPIOID_GENERICS = [
    "hydrocodone", "oxycodone", "morphine", "fentanyl", "codeine",
    "tramadol", "methadone", "buprenorphine", "hydromorphone", "oxymorphone"
]

ANTIBIOTIC_GENERICS = [
    "amoxicillin", "azithromycin", "ciprofloxacin", "doxycycline",
    "cephalexin", "metronidazole", "clindamycin", "sulfamethoxazole",
    "levofloxacin", "penicillin"
]


def create_dim_prescriber(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create prescriber dimension from prescription data.

    Returns:
        DataFrame with prescriber_id, npi, name, specialty, credential
    """
    # Get unique prescribers
    prescribers = df[["npi", "provider_last_name", "provider_first_name", "specialty"]].drop_duplicates(subset=["npi"])
    prescribers = prescribers.dropna(subset=["npi"])
    prescribers = prescribers.reset_index(drop=True)
    prescribers["prescriber_id"] = prescribers.index + 1

    # Create full name
    prescribers["provider_name"] = (
        prescribers["provider_first_name"].fillna("") + " " +
        prescribers["provider_last_name"].fillna("")
    ).str.strip()

    # Classify entity type
    def classify_entity(name):
        name_lower = str(name).lower()
        if any(x in name_lower for x in ["clinic", "center", "hospital", "health", "medical"]):
            return "Organization"
        return "Individual"

    prescribers["entity_type"] = prescribers["provider_name"].apply(classify_entity)

    logger.info(f"Created dim_prescriber: {len(prescribers)} prescribers")
    logger.info(f"Top specialties: {prescribers['specialty'].value_counts().head(5).to_dict()}")

    return prescribers[["prescriber_id", "npi", "provider_name", "specialty", "entity_type"]]


def create_dim_drug(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create drug dimension from prescription data.

    Returns:
        DataFrame with drug_id, drug_name, generic_name, is_opioid, is_antibiotic
    """
    # Get unique drugs
    drug_cols = ["drug_name", "generic_name", "brand_name"]
    available_cols = [c for c in drug_cols if c in df.columns]

    drugs = df[available_cols].drop_duplicates()
    drugs = drugs.dropna(subset=["drug_name"] if "drug_name" in available_cols else available_cols[:1])
    drugs = drugs.reset_index(drop=True)
    drugs["drug_id"] = drugs.index + 1

    # Classify drugs
    def is_opioid(generic):
        if pd.isna(generic):
            return False
        return any(op in str(generic).lower() for op in OPIOID_GENERICS)

    def is_antibiotic(generic):
        if pd.isna(generic):
            return False
        return any(ab in str(generic).lower() for ab in ANTIBIOTIC_GENERICS)

    if "generic_name" in drugs.columns:
        drugs["is_opioid"] = drugs["generic_name"].apply(is_opioid)
        drugs["is_antibiotic"] = drugs["generic_name"].apply(is_antibiotic)
    else:
        drugs["is_opioid"] = False
        drugs["is_antibiotic"] = False

    # Ensure required columns exist
    if "generic_name" not in drugs.columns:
        drugs["generic_name"] = drugs["drug_name"]
    if "brand_name" not in drugs.columns:
        drugs["brand_name"] = None

    logger.info(f"Created dim_drug: {len(drugs)} drugs")
    logger.info(f"Opioids: {drugs['is_opioid'].sum()}, Antibiotics: {drugs['is_antibiotic'].sum()}")

    return drugs[["drug_id", "drug_name", "generic_name", "brand_name", "is_opioid", "is_antibiotic"]]


def create_dim_location(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create location dimension from prescription data.

    Returns:
        DataFrame with location_id, city, state, zip_code
    """
    # Get unique locations
    loc_cols = ["city", "state"]
    available_cols = [c for c in loc_cols if c in df.columns]

    if not available_cols:
        # Create minimal location dimension
        locations = pd.DataFrame({"location_id": [1], "city": ["Unknown"], "state": ["UNK"]})
        return locations

    locations = df[available_cols].drop_duplicates()
    locations = locations.reset_index(drop=True)
    locations["location_id"] = locations.index + 1

    # Add state name
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
        "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia"
    }

    if "state" in locations.columns:
        locations["state_name"] = locations["state"].map(STATE_NAMES)

        # Classify urban/rural (simplified)
        urban_states = ["CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"]
        locations["urban_rural"] = locations["state"].apply(
            lambda x: "Urban" if x in urban_states else "Suburban/Rural"
        )

    logger.info(f"Created dim_location: {len(locations)} locations")

    return locations


def create_dim_year(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create year dimension from prescription data.

    Returns:
        DataFrame with year_id, calendar_year
    """
    if "year" not in df.columns:
        # Default to recent years
        years = [2022, 2021, 2020]
    else:
        years = sorted(df["year"].dropna().unique())

    year_records = []
    for i, year in enumerate(years):
        year_records.append({
            "year_id": i + 1,
            "calendar_year": int(year)
        })

    year_df = pd.DataFrame(year_records)
    logger.info(f"Created dim_year: {len(year_df)} years")

    return year_df


def create_fact_prescriptions(
    df: pd.DataFrame,
    dim_prescriber: pd.DataFrame,
    dim_drug: pd.DataFrame,
    dim_location: pd.DataFrame,
    dim_year: pd.DataFrame
) -> pd.DataFrame:
    """
    Create fact table by joining dimensions.

    Returns:
        Fact table with dimension keys and measures
    """
    logger.info(f"Creating fact_prescriptions from {len(df):,} raw records...")

    # Merge prescriber dimension
    prescriber_lookup = dim_prescriber[["prescriber_id", "npi"]]
    fact = df.merge(prescriber_lookup, on="npi", how="left")

    # Merge drug dimension
    drug_lookup = dim_drug[["drug_id", "drug_name"]]
    fact = fact.merge(drug_lookup, on="drug_name", how="left")

    # Merge location dimension
    if "city" in df.columns and "state" in df.columns:
        location_lookup = dim_location[["location_id", "city", "state"]]
        fact = fact.merge(location_lookup, on=["city", "state"], how="left")
    else:
        fact["location_id"] = 1

    # Merge year dimension
    if "year" in df.columns:
        year_lookup = dim_year[["year_id", "calendar_year"]]
        fact = fact.merge(year_lookup, left_on="year", right_on="calendar_year", how="left")
    else:
        fact["year_id"] = 1

    # Select columns for fact table
    measure_cols = ["total_claims", "total_day_supply", "total_drug_cost",
                    "total_beneficiaries", "total_30day_fills"]
    available_measures = [c for c in measure_cols if c in fact.columns]

    fact_cols = ["prescriber_id", "drug_id", "location_id", "year_id"] + available_measures

    fact_table = fact[fact_cols].copy()

    # Calculate derived measures
    if "total_drug_cost" in fact_table.columns and "total_claims" in fact_table.columns:
        fact_table["avg_cost_per_claim"] = (
            fact_table["total_drug_cost"] / fact_table["total_claims"]
        ).replace([np.inf, -np.inf], np.nan)

    # Add surrogate key
    fact_table = fact_table.reset_index(drop=True)
    fact_table["fact_id"] = fact_table.index + 1

    # Reorder columns
    final_cols = ["fact_id", "prescriber_id", "drug_id", "location_id", "year_id"]
    final_cols += [c for c in fact_table.columns if c not in final_cols]
    fact_table = fact_table[final_cols]

    logger.info(f"Created fact_prescriptions: {len(fact_table):,} rows")

    return fact_table


def transform_to_star_schema(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Transform raw prescription data into star schema.

    Args:
        df: Raw extracted prescription data

    Returns:
        Dict of dimension and fact DataFrames
    """
    logger.info(f"\n{'='*60}")
    logger.info("TRANSFORMING TO STAR SCHEMA")
    logger.info(f"{'='*60}")
    logger.info(f"Input rows: {len(df):,}")

    # Create dimensions
    dim_prescriber = create_dim_prescriber(df)
    dim_drug = create_dim_drug(df)
    dim_location = create_dim_location(df)
    dim_year = create_dim_year(df)

    # Create fact table
    fact_prescriptions = create_fact_prescriptions(
        df, dim_prescriber, dim_drug, dim_location, dim_year
    )

    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("STAR SCHEMA SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"dim_prescriber:     {len(dim_prescriber):>10,} rows")
    logger.info(f"dim_drug:           {len(dim_drug):>10,} rows")
    logger.info(f"dim_location:       {len(dim_location):>10,} rows")
    logger.info(f"dim_year:           {len(dim_year):>10,} rows")
    logger.info(f"fact_prescriptions: {len(fact_prescriptions):>10,} rows")
    logger.info(f"{'='*60}")

    return {
        "dim_prescriber": dim_prescriber,
        "dim_drug": dim_drug,
        "dim_location": dim_location,
        "dim_year": dim_year,
        "fact_prescriptions": fact_prescriptions
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

    fact = schema["fact_prescriptions"]

    # Check for orphan dimension keys
    for dim_col in ["prescriber_id", "drug_id", "location_id", "year_id"]:
        if dim_col in fact.columns:
            orphans = fact[fact[dim_col].isna()]
            results["checks"].append({
                "check": f"orphan_{dim_col}",
                "passed": len(orphans) < len(fact) * 0.1,
                "count": len(orphans),
                "percentage": round(len(orphans) / len(fact) * 100, 2)
            })

    # Check claims column
    if "total_claims" in fact.columns:
        null_claims = fact["total_claims"].isna().sum()
        results["checks"].append({
            "check": "null_claims",
            "passed": null_claims < len(fact) * 0.05,
            "count": null_claims,
            "percentage": round(null_claims / len(fact) * 100, 2)
        })

    # Overall
    results["passed"] = all(c["passed"] for c in results["checks"])

    for check in results["checks"]:
        status = "PASS" if check["passed"] else "WARN"
        logger.info(f"  [{status}] {check['check']}: {check.get('count', 'N/A')} ({check.get('percentage', 0)}%)")

    return results


if __name__ == "__main__":
    test_data = pd.read_parquet("./data/raw_prescriptions.parquet")
    print(f"Loaded {len(test_data):,} raw prescriptions")

    schema = transform_to_star_schema(test_data)
    validation = validate_star_schema(schema)

    print(f"\nValidation: {'PASSED' if validation['passed'] else 'WARNINGS'}")
