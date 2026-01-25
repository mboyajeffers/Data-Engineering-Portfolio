# Data Quality Scorecard

> **Synthetic demo output for portfolio purposes.**
> This scorecard demonstrates enterprise-grade data quality controls suitable for regulated environments.

---

## Overview

This scorecard defines the data quality checks applied to all incoming datasets. Each check has a severity level that determines pipeline behavior on failure.

---

## Severity Levels

| Level | Code | Pipeline Behavior |
|-------|------|-------------------|
| **BLOCKER** | `B` | Halt processing, quarantine data, alert |
| **WARNING** | `W` | Flag records, continue processing, include in report |
| **INFO** | `I` | Log for monitoring, no action required |

---

## Schema Validation Checks

| Check ID | Check Name | Severity | Method | Threshold | Failure Action |
|----------|------------|----------|--------|-----------|----------------|
| `SCH-001` | Required columns present | B | Schema comparison | 100% | Reject file |
| `SCH-002` | Column data types match | B | Type inference | 100% | Reject file |
| `SCH-003` | Date format compliance | B | Regex + parse | 100% | Quarantine records |
| `SCH-004` | Numeric precision valid | W | Range check | 99.9% | Flag records |
| `SCH-005` | Unexpected columns detected | I | Schema diff | N/A | Log only |

---

## Null & Completeness Checks

| Check ID | Check Name | Severity | Method | Threshold | Failure Action |
|----------|------------|----------|--------|-----------|----------------|
| `NUL-001` | Primary key not null | B | Null scan | 100% | Quarantine records |
| `NUL-002` | Required fields not null | B | Null scan | 100% | Quarantine records |
| `NUL-003` | Critical fields completeness | W | Null % | ≤ 5% null | Flag + report |
| `NUL-004` | Optional fields completeness | I | Null % | ≤ 20% null | Log only |

### Field Criticality Matrix

| Field | Criticality | Max Null % |
|-------|-------------|------------|
| `order_id` | Primary Key | 0% |
| `order_date` | Required | 0% |
| `customer_id` | Required | 0% |
| `order_total` | Required | 0% |
| `product_category` | Critical | 5% |
| `customer_segment` | Critical | 5% |
| `marketing_channel` | Optional | 20% |
| `nps_score` | Optional | 50% |

---

## Duplicate Detection Checks

| Check ID | Check Name | Severity | Method | Threshold | Failure Action |
|----------|------------|----------|--------|-----------|----------------|
| `DUP-001` | Primary key uniqueness | B | Hash set | 100% unique | Reject duplicates |
| `DUP-002` | Composite key uniqueness | B | Multi-column hash | 100% unique | Reject duplicates |
| `DUP-003` | Near-duplicate detection | W | Fuzzy match | ≤ 0.1% | Flag for review |
| `DUP-004` | Cross-batch duplicate check | W | Historical lookup | 100% unique | Upsert logic |

---

## Range & Domain Checks

| Check ID | Check Name | Severity | Method | Threshold | Failure Action |
|----------|------------|----------|--------|-----------|----------------|
| `RNG-001` | Monetary values ≥ 0 | B | Range check | 100% | Quarantine records |
| `RNG-002` | Quantity values ≥ 1 | B | Range check | 100% | Quarantine records |
| `RNG-003` | Discount % in [0, 100] | W | Range check | 100% | Flag records |
| `RNG-004` | Tax rate in [0, 0.25] | W | Range check | 100% | Flag records |
| `RNG-005` | NPS score in [0, 10] | W | Range check | 100% | Flag records |
| `RNG-006` | Date within valid range | B | Date bounds | ±2 years | Quarantine records |

### Allowed Value Lists

| Field | Allowed Values |
|-------|---------------|
| `customer_segment` | `VIP`, `Premium`, `Standard`, `New` |
| `order_status` | `Completed`, `Pending`, `Cancelled`, `Refunded` |
| `payment_method` | `Credit Card`, `Debit Card`, `PayPal`, `Wire Transfer`, `Financing` |
| `sales_channel` | `Online`, `In-Store`, `Phone`, `Private Client` |

---

## Referential Integrity Checks

| Check ID | Check Name | Severity | Method | Threshold | Failure Action |
|----------|------------|----------|--------|-----------|----------------|
| `REF-001` | Customer exists in master | W | Lookup | ≥ 99% | Flag orphans |
| `REF-002` | Product SKU exists in catalog | W | Lookup | ≥ 99% | Flag orphans |
| `REF-003` | Region/State valid combination | W | Lookup table | 100% | Flag invalid |

---

## Business Logic Checks

| Check ID | Check Name | Severity | Method | Threshold | Failure Action |
|----------|------------|----------|--------|-----------|----------------|
| `BIZ-001` | Subtotal = unit_price × quantity | B | Calculation | 100% | Quarantine |
| `BIZ-002` | Discount ≤ Subtotal | B | Comparison | 100% | Quarantine |
| `BIZ-003` | Order total = subtotal - discount + tax + fees | W | Calculation | 99.9% | Flag |
| `BIZ-004` | Gift orders have gift_wrap_fee or is_gift=true | W | Logic check | 100% | Flag |
| `BIZ-005` | Completed orders have valid total | B | Status + amount | 100% | Quarantine |

---

## Reconciliation Checks

> Critical for regulated environments (SOX, financial audits)

| Check ID | Check Name | Severity | Method | Threshold | Failure Action |
|----------|------------|----------|--------|-----------|----------------|
| `REC-001` | Row count matches source manifest | B | Count comparison | Exact match | Halt + alert |
| `REC-002` | Sum of order_total matches control total | B | Sum comparison | ±$0.01 | Halt + alert |
| `REC-003` | Distinct customers matches source | W | Count comparison | ±1% | Flag + report |
| `REC-004` | Date range matches expected window | B | Min/max check | Exact match | Halt + alert |

### Reconciliation Report Template

```
╔══════════════════════════════════════════════════════════════╗
║                   RECONCILIATION SUMMARY                      ║
╠══════════════════════════════════════════════════════════════╣
║ Metric              │ Source      │ Processed   │ Variance   ║
╠═════════════════════╪═════════════╪═════════════╪════════════╣
║ Row Count           │ 10,000      │ 10,000      │ 0 (0.00%)  ║
║ Total Revenue       │ $2,847,293  │ $2,847,293  │ $0.00      ║
║ Unique Customers    │ 4,521       │ 4,521       │ 0 (0.00%)  ║
║ Date Range Start    │ 2025-01-01  │ 2025-01-01  │ Match      ║
║ Date Range End      │ 2025-12-31  │ 2025-12-31  │ Match      ║
╠══════════════════════════════════════════════════════════════╣
║ STATUS: ✓ PASSED                                              ║
╚══════════════════════════════════════════════════════════════╝
```

---

## Statistical Anomaly Checks

| Check ID | Check Name | Severity | Method | Threshold | Failure Action |
|----------|------------|----------|--------|-----------|----------------|
| `ANO-001` | Order total distribution shift | W | Z-score | \|z\| > 3 | Flag + report |
| `ANO-002` | Daily volume spike | W | % change | > 200% | Flag + alert |
| `ANO-003` | Unusual category mix | I | Chi-square | p < 0.01 | Log + report |
| `ANO-004` | Customer concentration spike | W | Herfindahl | > 0.15 | Flag + report |

---

## Check Execution Summary

After each pipeline run, a scorecard summary is generated:

```
┌────────────────────────────────────────────────────────────────┐
│                    QUALITY SCORECARD SUMMARY                    │
├────────────────────────────────────────────────────────────────┤
│ Run ID: job_20260121_001                                       │
│ Input File: transactions_20260120.csv                          │
│ Processed: 2026-01-21 10:30:00 UTC                            │
├────────────────────────────────────────────────────────────────┤
│ Category          │ Checks │ Passed │ Warned │ Failed │ Score  │
├───────────────────┼────────┼────────┼────────┼────────┼────────┤
│ Schema            │   5    │   5    │   0    │   0    │ 100%   │
│ Null/Completeness │   4    │   4    │   0    │   0    │ 100%   │
│ Duplicates        │   4    │   4    │   0    │   0    │ 100%   │
│ Range/Domain      │   6    │   5    │   1    │   0    │  98%   │
│ Referential       │   3    │   3    │   0    │   0    │ 100%   │
│ Business Logic    │   5    │   5    │   0    │   0    │ 100%   │
│ Reconciliation    │   4    │   4    │   0    │   0    │ 100%   │
├───────────────────┼────────┼────────┼────────┼────────┼────────┤
│ OVERALL           │  31    │  30    │   1    │   0    │  99%   │
└────────────────────────────────────────────────────────────────┘
│ Status: ✓ PASSED (0 blockers, 1 warning)                       │
└────────────────────────────────────────────────────────────────┘
```

---

## Alert Escalation Matrix

| Severity | Notification | Response SLA |
|----------|--------------|--------------|
| BLOCKER | PagerDuty + Email + Slack | Immediate |
| WARNING (≥5) | Email + Slack | 4 hours |
| WARNING (1-4) | Slack | 24 hours |
| INFO | Dashboard only | N/A |

---

## References

- [Methodology](./methodology.md)
- [Validation Rules Config](../configs/validation_rules.yaml)
- [Schema Contract](../configs/schema_contract.yaml)
