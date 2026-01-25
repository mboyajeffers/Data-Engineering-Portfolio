"""
Financial Data Ingestion
========================

Loads and parses stock market OHLCV data from CSV sources.
Handles type coercion, date parsing, and derived column computation.
"""

import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def ingest_stock_data(
    filepath: Path,
    ticker_filter: Optional[list[str]] = None
) -> pd.DataFrame:
    """
    Load stock data from CSV with type coercion and date parsing.

    Args:
        filepath: Path to CSV with OHLCV data
        ticker_filter: Optional list of tickers to include

    Returns:
        DataFrame with parsed dates and proper dtypes
    """
    logger.info(f"Ingesting stock data from {filepath.name}")

    df = pd.read_csv(
        filepath,
        parse_dates=['date'],
        dtype={
            'ticker': 'string',
            'open': 'float64',
            'high': 'float64',
            'low': 'float64',
            'close': 'float64',
            'volume': 'int64',
        }
    )

    if ticker_filter:
        df = df[df['ticker'].isin(ticker_filter)]
        logger.info(f"  Filtered to tickers: {ticker_filter}")

    df = df.sort_values(['ticker', 'date']).reset_index(drop=True)
    logger.info(f"  Loaded {len(df):,} rows, {df['ticker'].nunique()} tickers")
    logger.info(f"  Date range: {df['date'].min().date()} to {df['date'].max().date()}")

    return df


def compute_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add technical indicators to raw OHLCV data.

    Computes per-ticker:
        - daily_return: close-to-close pct change
        - volatility_20d: 20-day rolling std of returns
        - sma_20: 20-day simple moving average of close
        - sma_50: 50-day simple moving average of close
        - volume_sma_20: 20-day average volume
        - price_range_pct: (high - low) / close
    """
    result = df.copy()

    for ticker in result['ticker'].unique():
        mask = result['ticker'] == ticker
        close = result.loc[mask, 'close']

        result.loc[mask, 'daily_return'] = close.pct_change()
        result.loc[mask, 'volatility_20d'] = (
            result.loc[mask, 'daily_return'].rolling(20).std()
        )
        result.loc[mask, 'sma_20'] = close.rolling(20).mean()
        result.loc[mask, 'sma_50'] = close.rolling(50).mean()
        result.loc[mask, 'volume_sma_20'] = (
            result.loc[mask, 'volume'].rolling(20).mean()
        )

    result['price_range_pct'] = (result['high'] - result['low']) / result['close']
    logger.info(f"  Computed 6 derived columns")
    return result


def get_ingestion_metadata(df: pd.DataFrame) -> dict:
    """Generate metadata summary for ingested data."""
    return {
        'row_count': len(df),
        'ticker_count': df['ticker'].nunique(),
        'tickers': sorted(df['ticker'].unique().tolist()),
        'date_range': {
            'start': str(df['date'].min().date()),
            'end': str(df['date'].max().date()),
            'trading_days': int(df.groupby('ticker')['date'].count().mean()),
        },
        'memory_mb': round(df.memory_usage(deep=True).sum() / 1024**2, 2),
    }
