"""
P05 Gaming Analytics - Analytics & KPI Module
Calculates gaming industry KPIs from star schema data.

Author: Mboya Jeffers
KPIs: DAU, MAU, ARPU, retention, engagement, revenue metrics
"""

import pandas as pd
import numpy as np
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class GamingAnalytics:
    """
    Gaming industry analytics engine.

    Calculates KPIs aligned with industry standards:
    - Player engagement metrics
    - Monetization metrics
    - Content performance
    - Market analysis
    """

    def __init__(self, data_dir: str = "data/star_schema"):
        self.data_dir = Path(data_dir)
        self.tables = {}
        self.kpis = {}

    def load_data(self):
        """Load star schema tables."""
        logger.info("Loading star schema data...")

        table_names = ['dim_game', 'dim_genre', 'dim_developer',
                       'fact_game_metrics', 'game_genre_bridge']

        for name in table_names:
            path = self.data_dir / f"{name}.parquet"
            if path.exists():
                self.tables[name] = pd.read_parquet(path)
                logger.info(f"Loaded {name}: {len(self.tables[name])} rows")
            else:
                logger.warning(f"Table not found: {name}")
                self.tables[name] = pd.DataFrame()

    def calculate_player_metrics(self) -> Dict:
        """Calculate player engagement metrics."""
        logger.info("Calculating player metrics...")

        fact = self.tables.get('fact_game_metrics', pd.DataFrame())
        if fact.empty:
            return {}

        metrics = {
            'total_games_analyzed': len(fact),
            'total_estimated_owners': int(fact['owners_estimate'].sum()),
            'total_active_players_2w': int(fact['players_2weeks'].sum()),
            'avg_concurrent_users': float(fact['concurrent_users'].mean()),
            'max_concurrent_users': int(fact['concurrent_users'].max()),

            # Engagement
            'avg_playtime_hours': float(fact['avg_playtime_forever'].mean() / 60),
            'median_playtime_hours': float(fact['median_playtime_forever'].median() / 60),
            'total_playtime_hours': int(fact['avg_playtime_forever'].sum() * fact['owners_estimate'].sum() / 60),

            # Activity rate (players_2weeks / owners)
            'avg_activity_rate': float(
                (fact['players_2weeks'] / fact['owners_estimate'].replace(0, np.nan)).mean() * 100
            ),
        }

        return metrics

    def calculate_review_metrics(self) -> Dict:
        """Calculate review and sentiment metrics."""
        logger.info("Calculating review metrics...")

        fact = self.tables.get('fact_game_metrics', pd.DataFrame())
        if fact.empty:
            return {}

        total_positive = int(fact['positive_reviews'].sum())
        total_negative = int(fact['negative_reviews'].sum())
        total_reviews = total_positive + total_negative

        # Review score distribution
        scores = fact['review_score_pct'].dropna()

        metrics = {
            'total_reviews': total_reviews,
            'total_positive_reviews': total_positive,
            'total_negative_reviews': total_negative,
            'overall_positive_rate': float(total_positive / total_reviews * 100) if total_reviews > 0 else 0,

            'avg_review_score': float(scores.mean()) if len(scores) > 0 else 0,
            'median_review_score': float(scores.median()) if len(scores) > 0 else 0,

            # Distribution
            'games_overwhelmingly_positive': int((scores >= 95).sum()),  # 95%+
            'games_very_positive': int(((scores >= 80) & (scores < 95)).sum()),  # 80-94%
            'games_mostly_positive': int(((scores >= 70) & (scores < 80)).sum()),  # 70-79%
            'games_mixed': int(((scores >= 40) & (scores < 70)).sum()),  # 40-69%
            'games_negative': int((scores < 40).sum()),  # <40%
        }

        return metrics

    def calculate_financial_metrics(self) -> Dict:
        """Calculate revenue and pricing metrics."""
        logger.info("Calculating financial metrics...")

        fact = self.tables.get('fact_game_metrics', pd.DataFrame())
        if fact.empty:
            return {}

        # Filter to paid games
        paid_games = fact[fact['price_usd'] > 0]

        metrics = {
            'total_games': len(fact),
            'free_to_play_games': int((fact['price_usd'] == 0).sum()),
            'paid_games': len(paid_games),
            'free_to_play_pct': float((fact['price_usd'] == 0).sum() / len(fact) * 100),

            # Pricing
            'avg_price_usd': float(paid_games['price_usd'].mean()) if len(paid_games) > 0 else 0,
            'median_price_usd': float(paid_games['price_usd'].median()) if len(paid_games) > 0 else 0,
            'max_price_usd': float(paid_games['price_usd'].max()) if len(paid_games) > 0 else 0,

            # Revenue estimates
            'total_revenue_estimate_usd': float(fact['revenue_estimate_usd'].sum()),
            'avg_revenue_per_game_usd': float(fact['revenue_estimate_usd'].mean()),
            'top_10_revenue_usd': float(fact.nlargest(10, 'revenue_estimate_usd')['revenue_estimate_usd'].sum()),

            # ARPU (Average Revenue Per User)
            'avg_arpu_usd': float(
                fact['revenue_estimate_usd'].sum() / fact['owners_estimate'].sum()
            ) if fact['owners_estimate'].sum() > 0 else 0,
        }

        return metrics

    def calculate_genre_metrics(self) -> Dict:
        """Calculate metrics by genre."""
        logger.info("Calculating genre metrics...")

        fact = self.tables.get('fact_game_metrics', pd.DataFrame())
        dim_genre = self.tables.get('dim_genre', pd.DataFrame())
        bridge = self.tables.get('game_genre_bridge', pd.DataFrame())

        if fact.empty or dim_genre.empty or bridge.empty:
            return {}

        # Join fact with genre through bridge
        fact_with_genre = fact.merge(bridge, on='game_key', how='left')
        fact_with_genre = fact_with_genre.merge(dim_genre, on='genre_key', how='left')

        genre_stats = fact_with_genre.groupby('genre_name').agg({
            'game_key': 'count',
            'owners_estimate': 'sum',
            'revenue_estimate_usd': 'sum',
            'review_score_pct': 'mean',
            'avg_playtime_forever': 'mean'
        }).reset_index()

        genre_stats.columns = ['genre', 'game_count', 'total_owners', 'total_revenue',
                               'avg_review_score', 'avg_playtime_min']

        # Top genres by different metrics
        top_by_games = genre_stats.nlargest(5, 'game_count')[['genre', 'game_count']].to_dict('records')
        top_by_owners = genre_stats.nlargest(5, 'total_owners')[['genre', 'total_owners']].to_dict('records')
        top_by_revenue = genre_stats.nlargest(5, 'total_revenue')[['genre', 'total_revenue']].to_dict('records')

        metrics = {
            'total_genres': len(dim_genre),
            'top_genres_by_games': top_by_games,
            'top_genres_by_owners': top_by_owners,
            'top_genres_by_revenue': top_by_revenue,
        }

        return metrics

    def calculate_quality_metrics(self) -> Dict:
        """Calculate Metacritic and quality metrics."""
        logger.info("Calculating quality metrics...")

        fact = self.tables.get('fact_game_metrics', pd.DataFrame())
        if fact.empty:
            return {}

        metacritic = fact['metacritic_score'].dropna()

        metrics = {
            'games_with_metacritic': len(metacritic),
            'avg_metacritic_score': float(metacritic.mean()) if len(metacritic) > 0 else 0,
            'metacritic_90plus': int((metacritic >= 90).sum()),
            'metacritic_80_89': int(((metacritic >= 80) & (metacritic < 90)).sum()),
            'metacritic_70_79': int(((metacritic >= 70) & (metacritic < 80)).sum()),
            'metacritic_below_70': int((metacritic < 70).sum()),
        }

        return metrics

    def generate_top_games_report(self, limit: int = 20) -> pd.DataFrame:
        """Generate report of top games by various metrics."""
        logger.info(f"Generating top {limit} games report...")

        fact = self.tables.get('fact_game_metrics', pd.DataFrame())
        dim_game = self.tables.get('dim_game', pd.DataFrame())

        if fact.empty or dim_game.empty:
            return pd.DataFrame()

        # Join with game names
        report = fact.merge(dim_game[['game_key', 'game_name']], on='game_key', how='left')

        # Select top games by revenue
        top_games = report.nlargest(limit, 'revenue_estimate_usd')[[
            'game_name', 'owners_estimate', 'players_2weeks', 'concurrent_users',
            'review_score_pct', 'price_usd', 'revenue_estimate_usd', 'metacritic_score'
        ]]

        return top_games

    def run_all_analytics(self) -> Dict:
        """Run complete analytics pipeline."""
        logger.info("=" * 60)
        logger.info("STARTING GAMING ANALYTICS")
        logger.info("=" * 60)

        self.load_data()

        self.kpis = {
            'generated_at': datetime.now().isoformat(),
            'player_metrics': self.calculate_player_metrics(),
            'review_metrics': self.calculate_review_metrics(),
            'financial_metrics': self.calculate_financial_metrics(),
            'genre_metrics': self.calculate_genre_metrics(),
            'quality_metrics': self.calculate_quality_metrics(),
        }

        # Generate top games report
        top_games = self.generate_top_games_report()
        if not top_games.empty:
            self.kpis['top_games'] = top_games.to_dict('records')

        # Save KPIs
        output_path = self.data_dir.parent / 'evidence' / 'P05_kpis.json'
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(self.kpis, f, indent=2, default=str)

        logger.info(f"KPIs saved to: {output_path}")

        logger.info("=" * 60)
        logger.info("ANALYTICS COMPLETE")
        logger.info("=" * 60)

        return self.kpis

    def print_summary(self):
        """Print human-readable summary of KPIs."""
        if not self.kpis:
            print("No KPIs calculated. Run run_all_analytics() first.")
            return

        print("\n" + "=" * 60)
        print("GAMING ANALYTICS SUMMARY")
        print("=" * 60)

        pm = self.kpis.get('player_metrics', {})
        print(f"\nPLAYER METRICS:")
        print(f"  Total Games Analyzed: {pm.get('total_games_analyzed', 0):,}")
        print(f"  Total Estimated Owners: {pm.get('total_estimated_owners', 0):,}")
        print(f"  Active Players (2 weeks): {pm.get('total_active_players_2w', 0):,}")
        print(f"  Avg Playtime: {pm.get('avg_playtime_hours', 0):.1f} hours")

        rm = self.kpis.get('review_metrics', {})
        print(f"\nREVIEW METRICS:")
        print(f"  Total Reviews: {rm.get('total_reviews', 0):,}")
        print(f"  Positive Rate: {rm.get('overall_positive_rate', 0):.1f}%")
        print(f"  Avg Review Score: {rm.get('avg_review_score', 0):.1f}%")

        fm = self.kpis.get('financial_metrics', {})
        print(f"\nFINANCIAL METRICS:")
        print(f"  Total Revenue Estimate: ${fm.get('total_revenue_estimate_usd', 0):,.0f}")
        print(f"  Avg Price: ${fm.get('avg_price_usd', 0):.2f}")
        print(f"  ARPU: ${fm.get('avg_arpu_usd', 0):.2f}")
        print(f"  F2P Games: {fm.get('free_to_play_pct', 0):.1f}%")

        print("\n" + "=" * 60)


if __name__ == "__main__":
    analytics = GamingAnalytics()
    kpis = analytics.run_all_analytics()
    analytics.print_summary()
