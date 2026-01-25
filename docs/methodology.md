# Data Processing Methodology

> **Synthetic demo output for portfolio purposes.**
> This document demonstrates industry-standard data engineering practices.

---

## 1. Overview

This methodology defines the end-to-end data processing approach for transactional analytics pipelines. It covers intake, validation, transformation, metrics computation, and delivery—designed for enterprise-grade reliability and auditability.

---

## 2. Data Intake & Contract Expectations

### 2.1 Intake Requirements

| Requirement | Specification |
|-------------|---------------|
| File Format | CSV, JSON, Parquet |
| Encoding | UTF-8 (BOM optional) |
| Delimiter | Comma (CSV), newline-delimited (JSON) |
| Header Row | Required for CSV |
| Max File Size | 500 MB per file (chunked processing for larger) |
| Naming Convention | `{source}_{entity}_{YYYYMMDD}.{ext}` |

### 2.2 Data Contracts

Every dataset must conform to a **schema contract** defined in YAML:

```yaml
# Example contract
entity: transactions
version: "1.0"
columns:
  - name: order_id
    type: string
    nullable: false
    unique: true
  - name: order_date
    type: date
    format: "%Y-%m-%d"
    nullable: false
  - name: order_total
    type: decimal
    precision: 2
    nullable: false
    min_value: 0
```

Contracts are versioned and stored alongside pipeline code for traceability.

---

## 3. Validation Gates

All data passes through sequential validation gates before transformation:

### 3.1 Gate Sequence

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Schema    │ →  │    Null     │ →  │  Duplicate  │ →  │    Range    │
│  Validation │    │   Checks    │    │   Detection │    │   Checks    │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                                                                │
                                                                ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────────────────────────┐
│  Referential│ ←  │   Format    │ ←  │  Business Rule Validation       │
│  Integrity  │    │  Validation │    │  (cross-field, temporal logic)  │
└─────────────┘    └─────────────┘    └─────────────────────────────────┘
```

### 3.2 Gate Definitions

| Gate | Severity | Action on Failure |
|------|----------|-------------------|
| Schema Validation | BLOCKER | Reject file, quarantine |
| Null Checks | BLOCKER/WARN | Depends on field criticality |
| Duplicate Detection | BLOCKER | Reject duplicates, log |
| Range Checks | WARN | Flag records, continue |
| Format Validation | BLOCKER | Reject malformed records |
| Referential Integrity | WARN | Flag orphans, continue |

### 3.3 Quarantine Protocol

Records failing BLOCKER validations are:
1. Moved to a `quarantine/` directory
2. Tagged with failure reason and timestamp
3. Excluded from downstream processing
4. Reported in the validation summary

---

## 4. Schema Drift Handling

### 4.1 Detection

Schema drift is detected by comparing incoming data against the active contract:

- **New columns**: Logged as INFO, ignored unless contract updated
- **Missing columns**: BLOCKER if required, WARN if optional
- **Type changes**: BLOCKER, requires contract revision
- **Renamed columns**: Handled via column alias mapping

### 4.2 Column Alias Mapping

A centralized alias mapper normalizes column names across sources:

```python
COLUMN_ALIASES = {
    "order_id": ["orderid", "order_number", "transaction_id", "txn_id"],
    "order_date": ["transaction_date", "purchase_date", "date"],
    "customer_id": ["cust_id", "client_id", "buyer_id"],
}
```

This ensures resilience to minor naming variations without code changes.

---

## 5. Standardization Rules

### 5.1 Time Zones

| Rule | Implementation |
|------|----------------|
| Storage | All timestamps stored in UTC |
| Input Parsing | Detect source timezone from metadata or assume UTC |
| Output | Convert to business timezone (configurable) for reports |
| Ambiguous Times | DST transitions logged as WARN |

### 5.2 Currency Handling

| Rule | Implementation |
|------|----------------|
| Base Currency | USD (configurable per deployment) |
| Multi-currency Input | Convert at transaction-date FX rate |
| FX Source | External API with daily rate cache |
| Precision | 4 decimal places for rates, 2 for amounts |

### 5.3 Identifier Normalization

- **Customer IDs**: Uppercase, trimmed, alphanumeric only
- **SKUs**: Uppercase, hyphens preserved
- **Dates**: ISO 8601 format (`YYYY-MM-DD`)
- **Nulls**: Standardized to `NULL` (not empty string, "N/A", etc.)

---

## 6. Re-runs, Backfills & Idempotency

### 6.1 Idempotency Guarantee

All pipeline operations are idempotent:

- Re-running with the same input produces identical output
- Achieved via deterministic processing and upsert logic
- State tracked via job IDs and checksums

### 6.2 Backfill Protocol

```
1. Define backfill window (start_date, end_date)
2. Clear existing output for window (soft delete)
3. Re-process source data for window
4. Validate row counts match expectations
5. Promote backfilled data to production
```

### 6.3 Conflict Resolution

| Scenario | Resolution |
|----------|------------|
| Duplicate order_id in same batch | Keep first occurrence, log duplicate |
| Duplicate order_id across batches | Update existing record (upsert) |
| Partial batch failure | Rollback entire batch, retry |

---

## 7. Logging & Observability

### 7.1 Log Levels

| Level | Usage |
|-------|-------|
| DEBUG | Row-level transformations (disabled in prod) |
| INFO | Stage completion, record counts |
| WARN | Non-blocking issues, data quality flags |
| ERROR | Recoverable failures, retries |
| CRITICAL | Pipeline halt, manual intervention required |

### 7.2 Structured Log Format

```json
{
  "timestamp": "2026-01-21T10:30:00Z",
  "level": "INFO",
  "job_id": "job_20260121_001",
  "stage": "validation",
  "message": "Schema validation passed",
  "metrics": {
    "rows_processed": 10000,
    "rows_valid": 9987,
    "rows_quarantined": 13
  }
}
```

### 7.3 Artifacts Produced

| Artifact | Format | Retention |
|----------|--------|-----------|
| Processing Log | JSON | 90 days |
| Validation Report | JSON/MD | 90 days |
| Metrics Output | JSON | Permanent |
| Executive Report | PDF/MD | Permanent |
| Cleaned Dataset | CSV/Parquet | Per policy |

---

## 8. Delivery Packaging

### 8.1 Output Bundle Structure

```
deliverables/{client_id}/{job_id}/
├── Report_Executive_Summary.pdf
├── Report_Detailed_Analysis.pdf
├── Dataset_Cleaned.csv
├── Dataset_Cleaned.parquet
├── Metrics_KPIs.json
├── Validation_Summary.json
└── MANIFEST.json
```

### 8.2 Manifest File

```json
{
  "job_id": "job_20260121_001",
  "generated_at": "2026-01-21T10:45:00Z",
  "input_file": "transactions_20260120.csv",
  "input_rows": 10000,
  "output_rows": 9987,
  "validation_status": "PASSED",
  "files": [
    {"name": "Report_Executive_Summary.pdf", "checksum": "sha256:abc123..."},
    {"name": "Dataset_Cleaned.csv", "checksum": "sha256:def456..."}
  ]
}
```

---

## 9. Quality Metrics

Every pipeline run produces these quality metrics:

| Metric | Definition |
|--------|------------|
| Completeness | % of non-null values in required fields |
| Uniqueness | % of records with unique primary key |
| Validity | % of records passing all validation rules |
| Timeliness | Processing latency (intake to delivery) |
| Consistency | Cross-field logical consistency rate |

---

## References

- [Schema Contract](../configs/schema_contract.yaml)
- [Validation Rules](../configs/validation_rules.yaml)
- [KPI Definitions](../configs/kpi_definitions.yaml)
- [Quality Scorecard](./quality_scorecard.md)
