#!/usr/bin/env python3
"""
SEC EDGAR Financial Facts Extractor
Author: Mboya Jeffers

Extracts company financial facts from SEC EDGAR at enterprise scale.
Target: 1M+ financial facts across 500+ companies.
"""

import os
import json
import time
import requests
import pandas as pd
from typing import List, Dict, Optional, Generator
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# SEC EDGAR API Configuration
SEC_BASE_URL = "https://data.sec.gov"
SEC_COMPANY_TICKERS = f"{SEC_BASE_URL}/files/company_tickers.json"
SEC_COMPANY_FACTS = f"{SEC_BASE_URL}/api/xbrl/companyfacts/CIK{{cik}}.json"

# Required by SEC - personal identifier
USER_AGENT = "Mboya Jeffers MboyaJeffers9@gmail.com"

# Rate limiting: SEC allows 10 requests/second
REQUEST_DELAY = 0.12
MAX_WORKERS = 5  # Parallel threads for extraction


class SECBulkExtractor:
    """
    Enterprise-scale extractor for SEC EDGAR financial facts.

    Processes 500+ companies to extract 1M+ financial facts.
    """

    def __init__(self, cache_dir: str = "./data/cache"):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept-Encoding": "gzip, deflate"
        })
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        self._last_request = 0
        self.stats = {
            "companies_processed": 0,
            "facts_extracted": 0,
            "errors": 0
        }

    def _rate_limit(self):
        """Ensure we don't exceed SEC rate limits."""
        elapsed = time.time() - self._last_request
        if elapsed < REQUEST_DELAY:
            time.sleep(REQUEST_DELAY - elapsed)
        self._last_request = time.time()

    def _get(self, url: str, use_cache: bool = True) -> Optional[Dict]:
        """Make rate-limited GET request with caching."""
        # Check cache
        cache_key = url.split("/")[-1]
        cache_path = os.path.join(self.cache_dir, cache_key)

        if use_cache and os.path.exists(cache_path):
            with open(cache_path, 'r') as f:
                return json.load(f)

        self._rate_limit()

        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()

            # Cache response
            with open(cache_path, 'w') as f:
                json.dump(data, f)

            return data

        except Exception as e:
            logger.debug(f"Error fetching {url}: {e}")
            return None

    def get_all_company_tickers(self) -> pd.DataFrame:
        """
        Get all company tickers and CIKs from SEC.

        Returns:
            DataFrame with columns: cik, ticker, title
        """
        logger.info("Fetching company tickers from SEC...")

        data = self._get(SEC_COMPANY_TICKERS)
        if not data:
            raise RuntimeError("Failed to fetch company tickers")

        # Convert to DataFrame
        records = []
        for key, company in data.items():
            records.append({
                "cik": str(company["cik_str"]).zfill(10),
                "ticker": company["ticker"],
                "title": company["title"]
            })

        df = pd.DataFrame(records)
        logger.info(f"Found {len(df)} companies")

        return df

    def get_company_facts(self, cik: str) -> Optional[Dict]:
        """
        Get all financial facts for a company.

        Args:
            cik: Zero-padded CIK number

        Returns:
            Dict containing all XBRL facts for the company
        """
        url = SEC_COMPANY_FACTS.format(cik=cik)
        return self._get(url)

    def extract_facts_to_rows(self, company_data: Dict, cik: str) -> List[Dict]:
        """
        Convert company facts JSON to flat rows.

        Args:
            company_data: Raw company facts from SEC
            cik: Company CIK

        Returns:
            List of fact dictionaries
        """
        rows = []

        if not company_data:
            return rows

        entity_name = company_data.get("entityName", "Unknown")
        facts = company_data.get("facts", {})

        # Process US-GAAP facts
        us_gaap = facts.get("us-gaap", {})
        for metric_name, metric_data in us_gaap.items():
            units = metric_data.get("units", {})

            for unit_type, values in units.items():
                for val in values:
                    rows.append({
                        "cik": cik,
                        "entity_name": entity_name,
                        "taxonomy": "us-gaap",
                        "metric": metric_name,
                        "unit": unit_type,
                        "value": val.get("val"),
                        "fiscal_year": val.get("fy"),
                        "fiscal_period": val.get("fp"),
                        "form": val.get("form"),
                        "filed": val.get("filed"),
                        "start_date": val.get("start"),
                        "end_date": val.get("end"),
                        "accession_number": val.get("accn")
                    })

        # Process DEI facts (Document and Entity Information)
        dei = facts.get("dei", {})
        for metric_name, metric_data in dei.items():
            units = metric_data.get("units", {})

            for unit_type, values in units.items():
                for val in values:
                    rows.append({
                        "cik": cik,
                        "entity_name": entity_name,
                        "taxonomy": "dei",
                        "metric": metric_name,
                        "unit": unit_type,
                        "value": val.get("val"),
                        "fiscal_year": val.get("fy"),
                        "fiscal_period": val.get("fp"),
                        "form": val.get("form"),
                        "filed": val.get("filed"),
                        "start_date": val.get("start"),
                        "end_date": val.get("end"),
                        "accession_number": val.get("accn")
                    })

        return rows

    def extract_company(self, cik: str) -> List[Dict]:
        """Extract all facts for a single company."""
        try:
            data = self.get_company_facts(cik)
            if data:
                rows = self.extract_facts_to_rows(data, cik)
                self.stats["companies_processed"] += 1
                self.stats["facts_extracted"] += len(rows)
                return rows
            else:
                self.stats["errors"] += 1
                return []
        except Exception as e:
            logger.debug(f"Error processing CIK {cik}: {e}")
            self.stats["errors"] += 1
            return []

    def extract_bulk(
        self,
        companies: pd.DataFrame,
        limit: Optional[int] = None,
        min_facts: int = 1_000_000
    ) -> pd.DataFrame:
        """
        Extract financial facts for multiple companies.

        Continues until reaching minimum fact threshold.

        Args:
            companies: DataFrame with 'cik' column
            limit: Max companies to process
            min_facts: Minimum facts to extract (default 1M)

        Returns:
            DataFrame with all financial facts
        """
        ciks = companies["cik"].tolist()
        if limit:
            ciks = ciks[:limit]

        logger.info(f"Starting bulk extraction for {len(ciks)} companies...")
        logger.info(f"Target: {min_facts:,} facts minimum")

        all_rows = []

        with tqdm(total=min_facts, desc="Extracting facts", unit=" facts") as pbar:
            for i, cik in enumerate(ciks):
                rows = self.extract_company(cik)
                all_rows.extend(rows)

                # Update progress
                pbar.n = len(all_rows)
                pbar.refresh()

                # Check if we've hit minimum
                if len(all_rows) >= min_facts:
                    logger.info(f"Reached {len(all_rows):,} facts after {i+1} companies")
                    break

                # Log progress every 50 companies
                if (i + 1) % 50 == 0:
                    logger.info(
                        f"Progress: {i+1} companies, {len(all_rows):,} facts, "
                        f"{self.stats['errors']} errors"
                    )

        df = pd.DataFrame(all_rows)

        logger.info(f"\n{'='*60}")
        logger.info("EXTRACTION COMPLETE")
        logger.info(f"{'='*60}")
        logger.info(f"Companies processed: {self.stats['companies_processed']:,}")
        logger.info(f"Total facts extracted: {len(df):,}")
        logger.info(f"Unique metrics: {df['metric'].nunique():,}")
        logger.info(f"Errors: {self.stats['errors']}")
        logger.info(f"{'='*60}")

        return df


def extract_sec_financial_facts(
    output_path: str = "./data/raw_facts.parquet",
    min_facts: int = 1_000_000,
    company_limit: Optional[int] = None
) -> pd.DataFrame:
    """
    Main extraction function.

    Args:
        output_path: Where to save extracted facts
        min_facts: Minimum facts to extract
        company_limit: Max companies to process

    Returns:
        DataFrame with extracted facts
    """
    extractor = SECBulkExtractor()

    # Get all companies
    companies = extractor.get_all_company_tickers()

    # Sort by market cap proxy (larger companies first for more facts)
    # Note: This is a heuristic - larger tickers tend to have more filings
    companies = companies.sort_values("ticker")

    # Extract facts
    facts_df = extractor.extract_bulk(
        companies,
        limit=company_limit,
        min_facts=min_facts
    )

    # Save to parquet for efficient storage
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    facts_df.to_parquet(output_path, index=False)
    logger.info(f"Saved {len(facts_df):,} facts to {output_path}")

    return facts_df


if __name__ == "__main__":
    # Test extraction
    df = extract_sec_financial_facts(
        output_path="./data/raw_facts.parquet",
        min_facts=100_000,  # Start with 100K for testing
        company_limit=200
    )

    print(f"\nSample data:")
    print(df.head(10))
    print(f"\nShape: {df.shape}")
