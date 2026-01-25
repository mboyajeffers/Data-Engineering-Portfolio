"""
Data Validation Module
======================

Applies schema contracts and validation rules to detect data quality issues.
Separates valid records from those requiring quarantine.

Synthetic demo output for portfolio purposes.
"""

import pandas as pd
from typing import Optional
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Container for validation results."""
    passed: bool
    check_name: str
    severity: str  # BLOCKER, WARN, INFO
    message: str
    affected_rows: int = 0
    affected_columns: list = field(default_factory=list)


@dataclass
class ValidationReport:
    """Aggregated validation report."""
    results: list[ValidationResult] = field(default_factory=list)
    total_rows: int = 0
    valid_rows: int = 0
    quarantined_rows: int = 0

    @property
    def passed(self) -> bool:
        """Check if all BLOCKER validations passed."""
        return not any(
            r.severity == 'BLOCKER' and not r.passed
            for r in self.results
        )

    @property
    def blocker_count(self) -> int:
        return sum(1 for r in self.results if r.severity == 'BLOCKER' and not r.passed)

    @property
    def warning_count(self) -> int:
        return sum(1 for r in self.results if r.severity == 'WARN' and not r.passed)

    def to_dict(self) -> dict:
        return {
            'passed': self.passed,
            'total_rows': self.total_rows,
            'valid_rows': self.valid_rows,
            'quarantined_rows': self.quarantined_rows,
            'blocker_count': self.blocker_count,
            'warning_count': self.warning_count,
            'checks': [
                {
                    'name': r.check_name,
                    'passed': r.passed,
                    'severity': r.severity,
                    'message': r.message,
                    'affected_rows': r.affected_rows,
                }
                for r in self.results
            ]
        }


def check_required_columns(
    df: pd.DataFrame,
    required: list[str]
) -> ValidationResult:
    """Verify all required columns are present."""
    missing = [col for col in required if col not in df.columns]

    if missing:
        return ValidationResult(
            passed=False,
            check_name='required_columns',
            severity='BLOCKER',
            message=f"Missing required columns: {missing}",
            affected_columns=missing,
        )

    return ValidationResult(
        passed=True,
        check_name='required_columns',
        severity='BLOCKER',
        message=f"All {len(required)} required columns present",
    )


def check_null_values(
    df: pd.DataFrame,
    column: str,
    max_null_pct: float = 0.0,
    severity: str = 'BLOCKER'
) -> ValidationResult:
    """Check null percentage for a column."""
    if column not in df.columns:
        return ValidationResult(
            passed=False,
            check_name=f'null_check_{column}',
            severity=severity,
            message=f"Column {column} not found",
        )

    null_count = df[column].isna().sum()
    null_pct = (null_count / len(df)) * 100 if len(df) > 0 else 0

    passed = null_pct <= max_null_pct

    return ValidationResult(
        passed=passed,
        check_name=f'null_check_{column}',
        severity=severity,
        message=f"{column}: {null_pct:.2f}% null (threshold: {max_null_pct}%)",
        affected_rows=null_count,
        affected_columns=[column],
    )


def check_duplicates(
    df: pd.DataFrame,
    key_columns: list[str]
) -> ValidationResult:
    """Check for duplicate records based on key columns."""
    if not all(col in df.columns for col in key_columns):
        missing = [col for col in key_columns if col not in df.columns]
        return ValidationResult(
            passed=False,
            check_name='duplicate_check',
            severity='BLOCKER',
            message=f"Key columns missing: {missing}",
            affected_columns=missing,
        )

    dup_count = df.duplicated(subset=key_columns, keep='first').sum()

    return ValidationResult(
        passed=dup_count == 0,
        check_name='duplicate_check',
        severity='BLOCKER',
        message=f"{dup_count:,} duplicate records found on {key_columns}",
        affected_rows=dup_count,
        affected_columns=key_columns,
    )


def check_value_range(
    df: pd.DataFrame,
    column: str,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
    severity: str = 'WARN'
) -> ValidationResult:
    """Check if numeric values fall within expected range."""
    if column not in df.columns:
        return ValidationResult(
            passed=False,
            check_name=f'range_check_{column}',
            severity=severity,
            message=f"Column {column} not found",
        )

    violations = 0

    if min_value is not None:
        violations += (df[column] < min_value).sum()
    if max_value is not None:
        violations += (df[column] > max_value).sum()

    range_str = f"[{min_value}, {max_value}]"

    return ValidationResult(
        passed=violations == 0,
        check_name=f'range_check_{column}',
        severity=severity,
        message=f"{column}: {violations:,} values outside {range_str}",
        affected_rows=violations,
        affected_columns=[column],
    )


def check_allowed_values(
    df: pd.DataFrame,
    column: str,
    allowed: list[str],
    severity: str = 'WARN'
) -> ValidationResult:
    """Check if categorical values are in allowed set."""
    if column not in df.columns:
        return ValidationResult(
            passed=False,
            check_name=f'allowed_values_{column}',
            severity=severity,
            message=f"Column {column} not found",
        )

    # Get unique values excluding nulls
    actual = set(df[column].dropna().unique())
    invalid = actual - set(allowed)
    invalid_count = df[column].isin(invalid).sum() if invalid else 0

    return ValidationResult(
        passed=len(invalid) == 0,
        check_name=f'allowed_values_{column}',
        severity=severity,
        message=f"{column}: {invalid_count:,} rows with invalid values: {invalid}" if invalid else f"{column}: all values valid",
        affected_rows=invalid_count,
        affected_columns=[column],
    )


def validate_dataframe(
    df: pd.DataFrame,
    schema: dict,
    rules: dict
) -> tuple[pd.DataFrame, pd.DataFrame, ValidationReport]:
    """
    Apply all validations and separate valid/quarantine records.

    Args:
        df: Input DataFrame
        schema: Schema contract (required columns, types)
        rules: Validation rules (null thresholds, ranges, etc.)

    Returns:
        Tuple of (valid_df, quarantine_df, report)
    """
    report = ValidationReport(total_rows=len(df))
    results = []
    quarantine_mask = pd.Series(False, index=df.index)

    # 1. Required columns check
    required_cols = schema.get('required_columns', [])
    result = check_required_columns(df, required_cols)
    results.append(result)

    if not result.passed:
        # Cannot continue without required columns
        report.results = results
        report.quarantined_rows = len(df)
        return pd.DataFrame(columns=df.columns), df, report

    # 2. Null checks
    null_rules = rules.get('null_checks', {})
    for col, config in null_rules.items():
        if col in df.columns:
            result = check_null_values(
                df, col,
                max_null_pct=config.get('max_null_pct', 0),
                severity=config.get('severity', 'WARN')
            )
            results.append(result)

            if not result.passed and config.get('severity') == 'BLOCKER':
                quarantine_mask |= df[col].isna()

    # 3. Duplicate check
    key_cols = schema.get('primary_key', [])
    if key_cols:
        result = check_duplicates(df, key_cols)
        results.append(result)

        if not result.passed:
            quarantine_mask |= df.duplicated(subset=key_cols, keep='first')

    # 4. Range checks
    range_rules = rules.get('range_checks', {})
    for col, config in range_rules.items():
        if col in df.columns:
            result = check_value_range(
                df, col,
                min_value=config.get('min'),
                max_value=config.get('max'),
                severity=config.get('severity', 'WARN')
            )
            results.append(result)

            if not result.passed and config.get('severity') == 'BLOCKER':
                if config.get('min') is not None:
                    quarantine_mask |= (df[col] < config['min'])
                if config.get('max') is not None:
                    quarantine_mask |= (df[col] > config['max'])

    # 5. Allowed values checks
    allowed_rules = rules.get('allowed_values', {})
    for col, config in allowed_rules.items():
        if col in df.columns:
            result = check_allowed_values(
                df, col,
                allowed=config.get('values', []),
                severity=config.get('severity', 'WARN')
            )
            results.append(result)

    # Split DataFrames
    valid_df = df[~quarantine_mask].copy()
    quarantine_df = df[quarantine_mask].copy()

    report.results = results
    report.valid_rows = len(valid_df)
    report.quarantined_rows = len(quarantine_df)

    logger.info(
        f"Validation complete: {report.valid_rows:,} valid, "
        f"{report.quarantined_rows:,} quarantined, "
        f"{report.blocker_count} blockers, {report.warning_count} warnings"
    )

    return valid_df, quarantine_df, report
