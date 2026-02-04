#!/usr/bin/env python3
"""
Federal Awards Bulk Extractor
Author: Mboya Jeffers

Extracts federal contract and grant awards from USASpending.gov at enterprise scale.
Target: 1M+ award transactions.
"""

import os
import json
import time
import zipfile
import requests
import pandas as pd
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from tqdm import tqdm
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# USASpending.gov API Configuration
USASPENDING_BASE = "https://api.usaspending.gov/api/v2"
BULK_DOWNLOAD_ENDPOINT = f"{USASPENDING_BASE}/bulk_download/awards/"
DOWNLOAD_STATUS_ENDPOINT = f"{USASPENDING_BASE}/bulk_download/status/"
SPENDING_BY_AWARD = f"{USASPENDING_BASE}/search/spending_by_award/"

# Rate limiting
REQUEST_DELAY = 0.2
MAX_RETRIES = 3


class USASpendingExtractor:
    """
    Enterprise-scale extractor for USASpending.gov federal awards.

    Processes 1M+ award transactions using bulk download API.
    """

    def __init__(self, cache_dir: str = "./data/cache"):
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json"
        })
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        self._last_request = 0
        self.stats = {
            "requests_made": 0,
            "awards_extracted": 0,
            "errors": 0
        }

    def _rate_limit(self):
        """Ensure we don't exceed API rate limits."""
        elapsed = time.time() - self._last_request
        if elapsed < REQUEST_DELAY:
            time.sleep(REQUEST_DELAY - elapsed)
        self._last_request = time.time()

    def _post(self, url: str, payload: Dict) -> Optional[Dict]:
        """Make rate-limited POST request."""
        self._rate_limit()
        self.stats["requests_made"] += 1

        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.post(url, json=payload, timeout=60)
                response.raise_for_status()
                return response.json()
            except Exception as e:
                logger.warning(f"Request failed (attempt {attempt+1}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)
                else:
                    self.stats["errors"] += 1
                    return None
        return None

    def search_awards(
        self,
        fiscal_years: List[int],
        award_types: List[str] = None,
        page: int = 1,
        limit: int = 100
    ) -> Optional[Dict]:
        """
        Search awards using spending_by_award endpoint.

        Args:
            fiscal_years: List of fiscal years to query
            award_types: Contract types (A, B, C, D) or grant types
            page: Page number for pagination
            limit: Results per page (max 100)

        Returns:
            Dict containing award results
        """
        if award_types is None:
            # All prime award types
            award_types = [
                "A", "B", "C", "D",  # Contracts
                "02", "03", "04", "05",  # Grants
                "06", "07", "08", "09", "10", "11"  # Other assistance
            ]

        payload = {
            "filters": {
                "time_period": [
                    {"start_date": f"{fy-1}-10-01", "end_date": f"{fy}-09-30"}
                    for fy in fiscal_years
                ],
                "award_type_codes": award_types
            },
            "fields": [
                "Award ID",
                "Recipient Name",
                "Award Amount",
                "Total Outlays",
                "Description",
                "Start Date",
                "End Date",
                "Awarding Agency",
                "Awarding Sub Agency",
                "Award Type",
                "Funding Agency",
                "recipient_id",
                "Place of Performance City",
                "Place of Performance State",
                "Place of Performance Zip",
                "NAICS Code",
                "CFDA Number",
                "generated_internal_id"
            ],
            "page": page,
            "limit": limit,
            "sort": "Award Amount",
            "order": "desc"
        }

        return self._post(SPENDING_BY_AWARD, payload)

    def extract_awards_paginated(
        self,
        fiscal_years: List[int],
        min_awards: int = 1_000_000,
        max_pages: int = 10000
    ) -> pd.DataFrame:
        """
        Extract awards using paginated search.

        Args:
            fiscal_years: Fiscal years to extract
            min_awards: Minimum awards to extract
            max_pages: Maximum pages to process

        Returns:
            DataFrame with all awards
        """
        logger.info(f"Starting award extraction for FY{fiscal_years}")
        logger.info(f"Target: {min_awards:,} awards minimum")

        all_awards = []
        page = 1
        limit = 100

        with tqdm(total=min_awards, desc="Extracting awards", unit=" awards") as pbar:
            while page <= max_pages:
                result = self.search_awards(fiscal_years, page=page, limit=limit)

                if not result or "results" not in result:
                    logger.warning(f"No results on page {page}")
                    break

                awards = result["results"]
                if not awards:
                    logger.info(f"No more awards after page {page}")
                    break

                # Process awards
                for award in awards:
                    all_awards.append({
                        "award_id": award.get("generated_internal_id") or award.get("Award ID"),
                        "recipient_name": award.get("Recipient Name"),
                        "award_amount": award.get("Award Amount"),
                        "total_outlays": award.get("Total Outlays"),
                        "description": award.get("Description"),
                        "start_date": award.get("Start Date"),
                        "end_date": award.get("End Date"),
                        "awarding_agency": award.get("Awarding Agency"),
                        "awarding_sub_agency": award.get("Awarding Sub Agency"),
                        "funding_agency": award.get("Funding Agency"),
                        "award_type": award.get("Award Type"),
                        "place_city": award.get("Place of Performance City"),
                        "place_state": award.get("Place of Performance State"),
                        "place_zip": award.get("Place of Performance Zip"),
                        "naics_code": award.get("NAICS Code"),
                        "cfda_number": award.get("CFDA Number")
                    })

                pbar.n = len(all_awards)
                pbar.refresh()

                # Check if we've hit minimum
                if len(all_awards) >= min_awards:
                    logger.info(f"Reached {len(all_awards):,} awards")
                    break

                # Progress logging
                if page % 100 == 0:
                    logger.info(f"Page {page}: {len(all_awards):,} awards extracted")

                page += 1

        df = pd.DataFrame(all_awards)
        self.stats["awards_extracted"] = len(df)

        logger.info(f"\n{'='*60}")
        logger.info("EXTRACTION COMPLETE")
        logger.info(f"{'='*60}")
        logger.info(f"Total awards extracted: {len(df):,}")
        logger.info(f"API requests made: {self.stats['requests_made']:,}")
        logger.info(f"Errors: {self.stats['errors']}")
        logger.info(f"{'='*60}")

        return df

    def extract_bulk_download(
        self,
        fiscal_year: int,
        award_levels: List[str] = None
    ) -> Optional[str]:
        """
        Request bulk download file from USASpending.

        This triggers async generation of bulk CSV files.

        Args:
            fiscal_year: Fiscal year for download
            award_levels: ['prime_awards', 'sub_awards']

        Returns:
            Download file URL when ready
        """
        if award_levels is None:
            award_levels = ["prime_awards"]

        payload = {
            "filters": {
                "prime_award_types": ["A", "B", "C", "D", "02", "03", "04", "05"],
                "date_type": "action_date",
                "date_range": {
                    "start_date": f"{fiscal_year-1}-10-01",
                    "end_date": f"{fiscal_year}-09-30"
                }
            },
            "columns": [],
            "file_format": "csv"
        }

        result = self._post(BULK_DOWNLOAD_ENDPOINT, payload)
        if result and "file_url" in result:
            return result["file_url"]
        elif result and "status_url" in result:
            # Async job - would need to poll status
            return result["status_url"]

        return None


def extract_federal_awards(
    output_path: str = "./data/raw_awards.parquet",
    min_awards: int = 1_000_000,
    fiscal_years: List[int] = None
) -> pd.DataFrame:
    """
    Main extraction function.

    Args:
        output_path: Where to save extracted awards
        min_awards: Minimum awards to extract
        fiscal_years: Fiscal years to extract (default: recent 3 years)

    Returns:
        DataFrame with extracted awards
    """
    if fiscal_years is None:
        current_year = datetime.now().year
        # Federal fiscal year runs Oct-Sep
        current_fy = current_year if datetime.now().month >= 10 else current_year
        fiscal_years = list(range(current_fy - 2, current_fy + 1))

    extractor = USASpendingExtractor()

    # Extract awards
    awards_df = extractor.extract_awards_paginated(
        fiscal_years=fiscal_years,
        min_awards=min_awards
    )

    # Parse dates
    for col in ["start_date", "end_date"]:
        if col in awards_df.columns:
            awards_df[col] = pd.to_datetime(awards_df[col], errors="coerce")

    # Convert amounts to numeric
    for col in ["award_amount", "total_outlays"]:
        if col in awards_df.columns:
            awards_df[col] = pd.to_numeric(awards_df[col], errors="coerce")

    # Extract fiscal year from start_date
    awards_df["fiscal_year"] = awards_df["start_date"].apply(
        lambda x: x.year + 1 if pd.notna(x) and x.month >= 10 else (x.year if pd.notna(x) else None)
    )

    # Save to parquet
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    awards_df.to_parquet(output_path, index=False)
    logger.info(f"Saved {len(awards_df):,} awards to {output_path}")

    return awards_df


if __name__ == "__main__":
    # Test extraction
    df = extract_federal_awards(
        output_path="./data/raw_awards.parquet",
        min_awards=100_000,
        fiscal_years=[2023, 2024]
    )

    print(f"\nSample data:")
    print(df.head(10))
    print(f"\nShape: {df.shape}")
    print(f"\nAgencies: {df['awarding_agency'].nunique()}")
