"""
P05 Gaming Analytics - Data Transformation Module
Transforms raw gaming data into analytics-ready star schema.

Author: Mboya Jeffers
Schema: Kimball dimensional model (fact + dimension tables)
"""

import pandas as pd
import numpy as np
import hashlib
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Tuple, List

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class GamingStarSchemaTransformer:
    """
    Transforms raw gaming data into Kimball star schema.

    Dimensions:
    - dim_game: Game master data
    - dim_developer: Developer/publisher info
    - dim_genre: Game genres
    - dim_platform: Gaming platforms
    - dim_date: Date dimension

    Facts:
    - fact_game_metrics: Player counts, playtime, revenue estimates
    - fact_reviews: Review aggregations
    """

    def __init__(self, input_dir: str = "data", output_dir: str = "data/star_schema"):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.transform_log = {
            'start_time': None,
            'end_time': None,
            'input_rows': 0,
            'output_rows': {},
            'quality_checks': {}
        }

    def _generate_surrogate_key(self, *values) -> str:
        """Generate surrogate key from business values."""
        combined = '|'.join(str(v) for v in values if v is not None)
        return hashlib.md5(combined.encode()).hexdigest()[:16]

    def _parse_date(self, date_str: str) -> Tuple[int, int, int, int]:
        """Parse date string and return (year, month, quarter, day_of_week)."""
        if pd.isna(date_str) or not date_str:
            return (None, None, None, None)

        try:
            # Handle various date formats
            for fmt in ['%b %d, %Y', '%d %b, %Y', '%Y-%m-%d', '%B %d, %Y']:
                try:
                    dt = datetime.strptime(date_str.strip(), fmt)
                    quarter = (dt.month - 1) // 3 + 1
                    return (dt.year, dt.month, quarter, dt.weekday())
                except ValueError:
                    continue
        except Exception:
            pass

        return (None, None, None, None)

    def _parse_owners(self, owners_str: str) -> Tuple[int, int]:
        """Parse owners range string like '1,000,000 .. 2,000,000'."""
        if pd.isna(owners_str) or not owners_str:
            return (0, 0)

        try:
            owners_str = str(owners_str).replace(',', '')
            if '..' in owners_str:
                parts = owners_str.split('..')
                low = int(parts[0].strip())
                high = int(parts[1].strip())
                return (low, high)
            else:
                val = int(owners_str)
                return (val, val)
        except Exception:
            return (0, 0)

    def build_dim_game(self, games_df: pd.DataFrame) -> pd.DataFrame:
        """Build game dimension table."""
        logger.info("Building dim_game...")

        if games_df.empty:
            return pd.DataFrame()

        dim_game = games_df[['appid', 'name', 'type', 'is_free', 'short_description',
                             'coming_soon', 'achievements_total']].copy()

        dim_game = dim_game.rename(columns={
            'appid': 'game_id',
            'name': 'game_name',
            'type': 'game_type',
            'short_description': 'description',
            'achievements_total': 'total_achievements'
        })

        # Generate surrogate key
        dim_game['game_key'] = dim_game['game_id'].apply(
            lambda x: self._generate_surrogate_key('game', x)
        )

        # Add metadata
        dim_game['effective_date'] = datetime.now().date()
        dim_game['is_current'] = True

        dim_game = dim_game.drop_duplicates(subset=['game_id'])
        logger.info(f"dim_game: {len(dim_game)} records")

        return dim_game

    def build_dim_developer(self, games_df: pd.DataFrame) -> pd.DataFrame:
        """Build developer dimension table."""
        logger.info("Building dim_developer...")

        if games_df.empty:
            return pd.DataFrame()

        # Extract unique developers
        developers = []
        for _, row in games_df.iterrows():
            dev_str = row.get('developers', '')
            pub_str = row.get('publishers', '')

            if dev_str:
                for dev in str(dev_str).split(','):
                    dev = dev.strip()
                    if dev:
                        developers.append({
                            'developer_name': dev,
                            'is_publisher': False
                        })

            if pub_str:
                for pub in str(pub_str).split(','):
                    pub = pub.strip()
                    if pub:
                        developers.append({
                            'developer_name': pub,
                            'is_publisher': True
                        })

        dim_dev = pd.DataFrame(developers).drop_duplicates(subset=['developer_name'])

        if dim_dev.empty:
            return pd.DataFrame()

        dim_dev['developer_key'] = dim_dev['developer_name'].apply(
            lambda x: self._generate_surrogate_key('dev', x)
        )
        dim_dev['developer_id'] = range(1, len(dim_dev) + 1)

        logger.info(f"dim_developer: {len(dim_dev)} records")
        return dim_dev

    def build_dim_genre(self, games_df: pd.DataFrame) -> pd.DataFrame:
        """Build genre dimension table."""
        logger.info("Building dim_genre...")

        if games_df.empty:
            return pd.DataFrame()

        genres = set()
        for _, row in games_df.iterrows():
            genre_str = row.get('genres', '')
            if genre_str:
                for genre in str(genre_str).split(','):
                    genre = genre.strip()
                    if genre:
                        genres.add(genre)

        dim_genre = pd.DataFrame({'genre_name': list(genres)})

        if dim_genre.empty:
            return pd.DataFrame()

        dim_genre['genre_key'] = dim_genre['genre_name'].apply(
            lambda x: self._generate_surrogate_key('genre', x)
        )
        dim_genre['genre_id'] = range(1, len(dim_genre) + 1)

        logger.info(f"dim_genre: {len(dim_genre)} records")
        return dim_genre

    def build_dim_platform(self) -> pd.DataFrame:
        """Build platform dimension table."""
        logger.info("Building dim_platform...")

        platforms = [
            {'platform_id': 1, 'platform_name': 'Windows', 'platform_code': 'WIN'},
            {'platform_id': 2, 'platform_name': 'macOS', 'platform_code': 'MAC'},
            {'platform_id': 3, 'platform_name': 'Linux', 'platform_code': 'LNX'},
            {'platform_id': 4, 'platform_name': 'Steam Deck', 'platform_code': 'DECK'},
        ]

        dim_platform = pd.DataFrame(platforms)
        dim_platform['platform_key'] = dim_platform['platform_code'].apply(
            lambda x: self._generate_surrogate_key('platform', x)
        )

        logger.info(f"dim_platform: {len(dim_platform)} records")
        return dim_platform

    def build_dim_date(self, games_df: pd.DataFrame) -> pd.DataFrame:
        """Build date dimension table."""
        logger.info("Building dim_date...")

        # Extract all dates from release dates
        dates = set()
        for _, row in games_df.iterrows():
            date_str = row.get('release_date', '')
            year, month, _, _ = self._parse_date(date_str)
            if year and month:
                dates.add((year, month))

        # Add current period
        now = datetime.now()
        dates.add((now.year, now.month))

        dim_date = []
        for year, month in dates:
            quarter = (month - 1) // 3 + 1
            dim_date.append({
                'date_key': self._generate_surrogate_key('date', year, month),
                'year': year,
                'month': month,
                'quarter': quarter,
                'year_month': f"{year}-{month:02d}",
                'is_current': (year == now.year and month == now.month)
            })

        dim_date_df = pd.DataFrame(dim_date).drop_duplicates(subset=['year', 'month'])
        logger.info(f"dim_date: {len(dim_date_df)} records")
        return dim_date_df

    def build_fact_game_metrics(self, games_df: pd.DataFrame, dim_game: pd.DataFrame) -> pd.DataFrame:
        """Build game metrics fact table."""
        logger.info("Building fact_game_metrics...")

        if games_df.empty:
            return pd.DataFrame()

        # Merge with dimension to get surrogate keys
        game_key_map = dict(zip(dim_game['game_id'], dim_game['game_key']))

        facts = []
        for _, row in games_df.iterrows():
            game_id = row.get('appid')
            game_key = game_key_map.get(game_id)

            if not game_key:
                continue

            # Parse owners
            owners_low, owners_high = self._parse_owners(row.get('owners_estimate', '0'))
            owners_mid = (owners_low + owners_high) // 2

            # Calculate metrics
            positive = int(row.get('positive_reviews', 0) or 0)
            negative = int(row.get('negative_reviews', 0) or 0)
            total_reviews = positive + negative
            review_score = (positive / total_reviews * 100) if total_reviews > 0 else None

            # Revenue estimate (owners * price * 0.7 Steam cut)
            price = float(row.get('price_usd', 0) or 0)
            revenue_estimate = owners_mid * price * 0.7 if price > 0 else 0

            facts.append({
                'fact_id': self._generate_surrogate_key('fact', game_id, datetime.now().isoformat()),
                'game_key': game_key,
                'game_id': game_id,
                'snapshot_date': datetime.now().date(),

                # Player metrics
                'owners_low': owners_low,
                'owners_high': owners_high,
                'owners_estimate': owners_mid,
                'players_forever': int(row.get('players_forever', 0) or 0),
                'players_2weeks': int(row.get('players_2weeks', 0) or 0),
                'concurrent_users': int(row.get('ccu', 0) or 0),

                # Playtime metrics (minutes)
                'avg_playtime_forever': int(row.get('average_forever', 0) or 0),
                'avg_playtime_2weeks': int(row.get('average_2weeks', 0) or 0),
                'median_playtime_forever': int(row.get('median_forever', 0) or 0),
                'median_playtime_2weeks': int(row.get('median_2weeks', 0) or 0),

                # Review metrics
                'positive_reviews': positive,
                'negative_reviews': negative,
                'total_reviews': total_reviews,
                'review_score_pct': review_score,
                'recommendations': int(row.get('recommendations', 0) or 0),

                # Financial metrics
                'price_usd': price,
                'revenue_estimate_usd': revenue_estimate,

                # Quality metrics
                'metacritic_score': row.get('metacritic_score'),
            })

        fact_df = pd.DataFrame(facts)
        logger.info(f"fact_game_metrics: {len(fact_df)} records")
        return fact_df

    def build_game_genre_bridge(self, games_df: pd.DataFrame,
                                 dim_game: pd.DataFrame,
                                 dim_genre: pd.DataFrame) -> pd.DataFrame:
        """Build bridge table for game-genre many-to-many relationship."""
        logger.info("Building game_genre_bridge...")

        if games_df.empty or dim_genre.empty:
            return pd.DataFrame()

        game_key_map = dict(zip(dim_game['game_id'], dim_game['game_key']))
        genre_key_map = dict(zip(dim_genre['genre_name'], dim_genre['genre_key']))

        bridges = []
        for _, row in games_df.iterrows():
            game_id = row.get('appid')
            game_key = game_key_map.get(game_id)

            if not game_key:
                continue

            genre_str = row.get('genres', '')
            if genre_str:
                for genre in str(genre_str).split(','):
                    genre = genre.strip()
                    genre_key = genre_key_map.get(genre)
                    if genre_key:
                        bridges.append({
                            'game_key': game_key,
                            'genre_key': genre_key
                        })

        bridge_df = pd.DataFrame(bridges).drop_duplicates()
        logger.info(f"game_genre_bridge: {len(bridge_df)} records")
        return bridge_df

    def run_transformation(self, games_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Run complete transformation pipeline.

        Returns dict of all dimension and fact tables.
        """
        self.transform_log['start_time'] = datetime.now().isoformat()
        self.transform_log['input_rows'] = len(games_df)

        logger.info("=" * 60)
        logger.info("STARTING STAR SCHEMA TRANSFORMATION")
        logger.info(f"Input rows: {len(games_df)}")
        logger.info("=" * 60)

        # Build dimensions
        dim_game = self.build_dim_game(games_df)
        dim_developer = self.build_dim_developer(games_df)
        dim_genre = self.build_dim_genre(games_df)
        dim_platform = self.build_dim_platform()
        dim_date = self.build_dim_date(games_df)

        # Build facts
        fact_game_metrics = self.build_fact_game_metrics(games_df, dim_game)

        # Build bridge tables
        game_genre_bridge = self.build_game_genre_bridge(games_df, dim_game, dim_genre)

        # Collect all tables
        tables = {
            'dim_game': dim_game,
            'dim_developer': dim_developer,
            'dim_genre': dim_genre,
            'dim_platform': dim_platform,
            'dim_date': dim_date,
            'fact_game_metrics': fact_game_metrics,
            'game_genre_bridge': game_genre_bridge
        }

        # Save all tables
        for name, df in tables.items():
            if not df.empty:
                output_path = self.output_dir / f"{name}.parquet"
                df.to_parquet(output_path, index=False)
                self.transform_log['output_rows'][name] = len(df)
                logger.info(f"Saved {name}: {len(df)} rows")

        self.transform_log['end_time'] = datetime.now().isoformat()

        # Save transform log
        import json
        log_path = self.output_dir / 'transform_log.json'
        with open(log_path, 'w') as f:
            json.dump(self.transform_log, f, indent=2, default=str)

        logger.info("=" * 60)
        logger.info("TRANSFORMATION COMPLETE")
        logger.info("=" * 60)

        return tables


if __name__ == "__main__":
    # Test transformation with sample data
    transformer = GamingStarSchemaTransformer()

    # Load raw data if exists
    raw_path = Path("data/raw_game_details.parquet")
    if raw_path.exists():
        games_df = pd.read_parquet(raw_path)
        tables = transformer.run_transformation(games_df)
        print(f"\nCreated {len(tables)} tables")
    else:
        print("No raw data found. Run extract.py first.")
