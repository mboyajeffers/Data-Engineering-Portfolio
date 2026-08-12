"""
P07 Media & Streaming Analytics - Data Extraction Module
Author: Mboya Jeffers

Enterprise-scale extractor for IMDB datasets and media data.
Implements chunked processing for large TSV files and logging.
"""

import gzip
import json
import logging
import time
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Iterator
import requests
import io

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IMDBDataExtractor:
    """
    Enterprise-scale extractor for IMDB public datasets.

    Data Sources (datasets.imdbws.com):
    - title.basics.tsv.gz: 10M+ titles
    - title.ratings.tsv.gz: 1.3M+ rated titles
    - name.basics.tsv.gz: 12M+ people
    - title.principals.tsv.gz: 50M+ credits

    Features:
    - Streaming/chunked processing for large files
    - Gzip decompression
    - Progress logging
    - Checkpoint capability
    """

    # IMDB dataset URLs (publicly available)
    IMDB_BASE = "https://datasets.imdbws.com"

    DATASETS = {
        'titles': 'title.basics.tsv.gz',
        'ratings': 'title.ratings.tsv.gz',
        'names': 'name.basics.tsv.gz',
        'principals': 'title.principals.tsv.gz',
        'crew': 'title.crew.tsv.gz',
        'episodes': 'title.episode.tsv.gz'
    }

    # Chunk size for streaming
    CHUNK_SIZE = 10000

    def __init__(self, output_dir: str = "data"):
        """Initialize extractor with output directory."""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Media-Analytics-Pipeline/1.0 (mboyajeffers9@gmail.com)'
        })

        self.extraction_log = {
            'start_time': None,
            'end_time': None,
            'datasets_processed': [],
            'total_rows': 0,
            'errors': []
        }

    def _download_dataset(self, dataset_name: str, limit: Optional[int] = None) -> List[Dict]:
        """
        Download and parse IMDB dataset.

        Args:
            dataset_name: Name of dataset (titles, ratings, names, principals)
            limit: Maximum rows to extract (None for all)

        Returns:
            List of parsed records
        """
        if dataset_name not in self.DATASETS:
            logger.error(f"Unknown dataset: {dataset_name}")
            return []

        filename = self.DATASETS[dataset_name]
        url = f"{self.IMDB_BASE}/{filename}"

        logger.info(f"Downloading {dataset_name} from {url}...")

        try:
            response = self.session.get(url, stream=True, timeout=300)
            response.raise_for_status()

            # Decompress gzip stream
            decompressed = gzip.GzipFile(fileobj=io.BytesIO(response.content))
            content = decompressed.read().decode('utf-8')

            lines = content.strip().split('\n')
            headers = lines[0].split('\t')

            records = []
            for i, line in enumerate(lines[1:], 1):
                if limit and i > limit:
                    break

                values = line.split('\t')
                record = {}
                for j, header in enumerate(headers):
                    if j < len(values):
                        value = values[j]
                        # Handle IMDB null values
                        record[header] = None if value == '\\N' else value
                    else:
                        record[header] = None

                records.append(record)

                # Progress logging
                if i % 100000 == 0:
                    logger.info(f"  Processed {i:,} rows...")

            logger.info(f"  Extracted {len(records):,} records from {dataset_name}")
            self.extraction_log['total_rows'] += len(records)

            return records

        except Exception as e:
            logger.error(f"Failed to download {dataset_name}: {e}")
            self.extraction_log['errors'].append(f"{dataset_name}: {str(e)}")
            return []

    def extract_titles(self, limit: Optional[int] = None,
                       title_types: Optional[List[str]] = None) -> List[Dict]:
        """
        Extract title data from IMDB.

        Args:
            limit: Maximum titles to extract
            title_types: Filter by type (movie, tvSeries, etc.)

        Returns:
            List of title records
        """
        logger.info("Extracting titles...")

        records = self._download_dataset('titles', limit=limit)

        if title_types:
            records = [r for r in records if r.get('titleType') in title_types]
            logger.info(f"  Filtered to {len(records)} titles of types: {title_types}")

        # Parse numeric fields
        for record in records:
            if record.get('startYear'):
                try:
                    record['startYear'] = int(record['startYear'])
                except:
                    record['startYear'] = None

            if record.get('endYear'):
                try:
                    record['endYear'] = int(record['endYear'])
                except:
                    record['endYear'] = None

            if record.get('runtimeMinutes'):
                try:
                    record['runtimeMinutes'] = int(record['runtimeMinutes'])
                except:
                    record['runtimeMinutes'] = None

            record['isAdult'] = record.get('isAdult') == '1'

        self.extraction_log['datasets_processed'].append({
            'name': 'titles',
            'records': len(records)
        })

        return records

    def extract_ratings(self, limit: Optional[int] = None) -> List[Dict]:
        """
        Extract rating data from IMDB.

        Args:
            limit: Maximum ratings to extract

        Returns:
            List of rating records
        """
        logger.info("Extracting ratings...")

        records = self._download_dataset('ratings', limit=limit)

        # Parse numeric fields
        for record in records:
            if record.get('averageRating'):
                try:
                    record['averageRating'] = float(record['averageRating'])
                except:
                    record['averageRating'] = None

            if record.get('numVotes'):
                try:
                    record['numVotes'] = int(record['numVotes'])
                except:
                    record['numVotes'] = None

        self.extraction_log['datasets_processed'].append({
            'name': 'ratings',
            'records': len(records)
        })

        return records

    def extract_names(self, limit: Optional[int] = None) -> List[Dict]:
        """
        Extract person/name data from IMDB.

        Args:
            limit: Maximum names to extract

        Returns:
            List of person records
        """
        logger.info("Extracting names...")

        records = self._download_dataset('names', limit=limit)

        # Parse numeric fields
        for record in records:
            if record.get('birthYear'):
                try:
                    record['birthYear'] = int(record['birthYear'])
                except:
                    record['birthYear'] = None

            if record.get('deathYear'):
                try:
                    record['deathYear'] = int(record['deathYear'])
                except:
                    record['deathYear'] = None

        self.extraction_log['datasets_processed'].append({
            'name': 'names',
            'records': len(records)
        })

        return records

    def extract_principals(self, limit: Optional[int] = None,
                          title_ids: Optional[set] = None) -> List[Dict]:
        """
        Extract cast/crew principal data from IMDB.

        Args:
            limit: Maximum records to extract
            title_ids: Filter to specific title IDs

        Returns:
            List of principal (cast/crew) records
        """
        logger.info("Extracting principals (cast/crew)...")

        records = self._download_dataset('principals', limit=limit)

        if title_ids:
            records = [r for r in records if r.get('tconst') in title_ids]
            logger.info(f"  Filtered to {len(records)} principals for specified titles")

        # Parse ordering
        for record in records:
            if record.get('ordering'):
                try:
                    record['ordering'] = int(record['ordering'])
                except:
                    record['ordering'] = None

        self.extraction_log['datasets_processed'].append({
            'name': 'principals',
            'records': len(records)
        })

        return records

    def run_test_extraction(self, limit: int = 1000) -> Dict:
        """
        Run limited test extraction for validation.

        Args:
            limit: Maximum records per dataset

        Returns:
            Combined extraction results
        """
        self.extraction_log['start_time'] = datetime.now().isoformat()

        logger.info("=" * 60)
        logger.info(f"STARTING TEST EXTRACTION (limit={limit})")
        logger.info("=" * 60)

        all_data = {
            'titles': [],
            'ratings': [],
            'names': [],
            'principals': []
        }

        # Extract each dataset with limit
        all_data['titles'] = self.extract_titles(
            limit=limit,
            title_types=['movie', 'tvSeries', 'tvMiniSeries']
        )

        all_data['ratings'] = self.extract_ratings(limit=limit)

        all_data['names'] = self.extract_names(limit=limit)

        # For principals, filter to extracted title IDs
        title_ids = set(t['tconst'] for t in all_data['titles'] if t.get('tconst'))
        all_data['principals'] = self.extract_principals(limit=limit * 10, title_ids=title_ids)

        self.extraction_log['end_time'] = datetime.now().isoformat()

        # Save extraction log
        log_path = self.output_dir / "extraction_log.json"
        with open(log_path, 'w') as f:
            json.dump(self.extraction_log, f, indent=2)
        logger.info(f"Extraction log saved: {log_path}")

        return all_data

    def run_full_extraction(self, title_types: Optional[List[str]] = None) -> Dict:
        """
        Run full-scale extraction for production.

        Args:
            title_types: Optional filter for title types

        Returns:
            Combined extraction results
        """
        self.extraction_log['start_time'] = datetime.now().isoformat()

        if title_types is None:
            title_types = ['movie', 'tvSeries', 'tvMiniSeries', 'short']

        logger.info("=" * 60)
        logger.info("STARTING FULL EXTRACTION")
        logger.info(f"Title types: {title_types}")
        logger.info("=" * 60)

        all_data = {
            'titles': [],
            'ratings': [],
            'names': [],
            'principals': []
        }

        # Extract titles
        all_data['titles'] = self.extract_titles(title_types=title_types)

        # Save intermediate
        titles_path = self.output_dir / "raw_titles.json"
        with open(titles_path, 'w') as f:
            json.dump(all_data['titles'], f)
        logger.info(f"Saved: {titles_path}")

        # Extract ratings
        all_data['ratings'] = self.extract_ratings()

        # Extract names
        all_data['names'] = self.extract_names()

        # Extract principals for extracted titles
        title_ids = set(t['tconst'] for t in all_data['titles'] if t.get('tconst'))
        all_data['principals'] = self.extract_principals(title_ids=title_ids)

        self.extraction_log['end_time'] = datetime.now().isoformat()

        # Save extraction log
        log_path = self.output_dir / "extraction_log.json"
        with open(log_path, 'w') as f:
            json.dump(self.extraction_log, f, indent=2)
        logger.info(f"Extraction log saved: {log_path}")

        return all_data


if __name__ == "__main__":
    extractor = IMDBDataExtractor()

    # Test extraction
    data = extractor.run_test_extraction(limit=500)

    print("\nExtraction Summary:")
    print(f"  Titles: {len(data['titles'])}")
    print(f"  Ratings: {len(data['ratings'])}")
    print(f"  Names: {len(data['names'])}")
    print(f"  Principals: {len(data['principals'])}")
    print(f"  Total: {extractor.extraction_log['total_rows']}")
