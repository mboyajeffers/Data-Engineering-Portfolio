"""
Data Ingestion Module
=====================

Handles loading raw data from various file formats into pandas DataFrames
with appropriate type casting and initial normalization.

Synthetic demo output for portfolio purposes.
"""

import pandas as pd
from pathlib import Path
from typing import Optional
import logging

logger = logging.getLogger(__name__)


def detect_file_type(file_path: Path) -> str:
    """Detect file type from extension."""
    suffix = file_path.suffix.lower()
    type_map = {
        '.csv': 'csv',
        '.json': 'json',
        '.parquet': 'parquet',
        '.xlsx': 'excel',
        '.xls': 'excel',
    }
    return type_map.get(suffix, 'unknown')


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names to lowercase with underscores.

    Examples:
        'Order ID' -> 'order_id'
        'orderTotal' -> 'ordertotal'
        'CUSTOMER_ID' -> 'customer_id'
    """
    df.columns = (
        df.columns
        .str.lower()
        .str.strip()
        .str.replace(r'[^\w\s]', '', regex=True)
        .str.replace(r'\s+', '_', regex=True)
    )
    return df


def cast_types(df: pd.DataFrame, type_hints: Optional[dict] = None) -> pd.DataFrame:
    """
    Apply type casting based on column name patterns or explicit hints.

    Auto-detection patterns:
        - *_date, *_time -> datetime
        - *_id -> string
        - *_total, *_amount, *_price, *_fee -> float
        - *_count, quantity -> int
    """
    if type_hints is None:
        type_hints = {}

    for col in df.columns:
        if col in type_hints:
            target_type = type_hints[col]
            if target_type == 'datetime':
                df[col] = pd.to_datetime(df[col], errors='coerce')
            elif target_type == 'float':
                df[col] = pd.to_numeric(df[col], errors='coerce')
            elif target_type == 'int':
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            elif target_type == 'string':
                df[col] = df[col].astype(str)
        else:
            # Auto-detection based on column name patterns
            col_lower = col.lower()
            if col_lower.endswith('_date') or col_lower == 'order_date':
                df[col] = pd.to_datetime(df[col], errors='coerce')
            elif any(col_lower.endswith(suffix) for suffix in ['_total', '_amount', '_price', '_fee', '_rate']):
                df[col] = pd.to_numeric(df[col], errors='coerce')
            elif col_lower == 'quantity':
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    return df


def ingest_file(
    file_path: str | Path,
    type_hints: Optional[dict] = None,
    encoding: str = 'utf-8'
) -> pd.DataFrame:
    """
    Load a data file into a pandas DataFrame with normalization.

    Args:
        file_path: Path to the input file
        type_hints: Optional dict mapping column names to types
        encoding: File encoding (default: utf-8)

    Returns:
        Normalized DataFrame ready for validation

    Raises:
        ValueError: If file type is unsupported
        FileNotFoundError: If file doesn't exist
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")

    file_type = detect_file_type(file_path)
    logger.info(f"Ingesting {file_type} file: {file_path.name}")

    if file_type == 'csv':
        df = pd.read_csv(file_path, encoding=encoding)
    elif file_type == 'json':
        df = pd.read_json(file_path, lines=True)
    elif file_type == 'parquet':
        df = pd.read_parquet(file_path)
    elif file_type == 'excel':
        df = pd.read_excel(file_path)
    else:
        raise ValueError(f"Unsupported file type: {file_path.suffix}")

    # Normalize
    df = normalize_column_names(df)
    df = cast_types(df, type_hints)

    logger.info(f"Ingested {len(df):,} rows, {len(df.columns)} columns")

    return df


def ingest_metadata(df: pd.DataFrame, file_path: str | Path) -> dict:
    """
    Generate metadata about the ingested data.

    Returns:
        Dict with file info, row count, column info, etc.
    """
    file_path = Path(file_path)

    return {
        'file_name': file_path.name,
        'file_size_bytes': file_path.stat().st_size,
        'row_count': len(df),
        'column_count': len(df.columns),
        'columns': list(df.columns),
        'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()},
        'memory_usage_mb': round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2),
    }
