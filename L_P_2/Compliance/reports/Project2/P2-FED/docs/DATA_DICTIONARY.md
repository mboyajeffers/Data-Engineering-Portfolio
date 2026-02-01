# P2-FED Data Dictionary
## Federal Procurement Spend Intelligence Pipeline
**Version:** 1.0.0 | **Last Updated:** 2026-02-01

---

## Overview

This document describes the data model for the P2-FED Federal Procurement Spend Intelligence Pipeline, which ingests federal contract award data from USAspending.gov API.

---

## Source System

| Attribute | Value |
|-----------|-------|
| **Source** | USAspending.gov API v2 |
| **API Documentation** | https://api.usaspending.gov/docs/endpoints |
| **Primary Endpoint** | `/api/v2/search/spending_by_award/` |
| **Data Coverage** | Multi-year contracts (FY2004-2027) |
| **Target Agencies** | DoD, HHS, DHS |
| **Award Types** | Contracts (A, B, C, D) |

> **Note:** Federal contracts frequently span multiple fiscal years. The dataset includes contract obligations across their full performance periods, resulting in records dated from FY2004 through FY2027. Total spend ($67.14B) reflects cumulative obligations across all fiscal years, not a single-year snapshot.

---

## Data Model (Star Schema)

### Fact Table: `award_fact`

Primary transaction table containing federal contract awards.

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `award_id` | VARCHAR(50) | NO | Unique award identifier |
| `internal_id` | INTEGER | NO | USAspending internal ID |
| `generated_internal_id` | VARCHAR(100) | YES | Generated unique key |
| `award_amount` | DECIMAL(15,2) | YES | Total obligated amount (USD) |
| `total_outlays` | DECIMAL(15,2) | YES | Actual payments made (USD) |
| `start_date` | DATE | YES | Award period of performance start |
| `end_date` | DATE | YES | Award period of performance end |
| `naics_code` | VARCHAR(10) | YES | North American Industry Classification |
| `naics_description` | VARCHAR(255) | YES | NAICS code description |
| `psc_code` | VARCHAR(10) | YES | Product/Service Code |
| `psc_description` | VARCHAR(255) | YES | PSC code description |
| `description` | TEXT | YES | Award description/purpose |
| `fiscal_year` | INTEGER | YES | Derived fiscal year (Oct-Sep) |
| `fiscal_quarter` | INTEGER | YES | Derived fiscal quarter (1-4) |
| `agency_id` | INTEGER | YES | FK → agency_dim |
| `recipient_id` | INTEGER | YES | FK → recipient_dim |
| `record_hash` | VARCHAR(16) | NO | MD5 hash for deduplication |

**Row Count:** 24,800

---

### Dimension Table: `agency_dim`

Federal agency hierarchy (toptier/subtier).

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `agency_id` | INTEGER | NO | Primary key |
| `toptier_name` | VARCHAR(100) | NO | Top-level agency name |
| `subtier_name` | VARCHAR(100) | YES | Sub-agency name |

**Row Count:** 6

**Sample Values:**
- Department of Defense → Department of the Army
- Department of Defense → U.S. Special Operations Command
- Department of Health and Human Services → Centers for Medicare and Medicaid Services
- Department of Homeland Security → U.S. Customs and Border Protection

---

### Dimension Table: `recipient_dim`

Contract award recipients (vendors).

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `recipient_id` | INTEGER | NO | Primary key |
| `recipient_name` | VARCHAR(255) | NO | Legal entity name |
| `uei` | VARCHAR(12) | YES | Unique Entity Identifier (replaced DUNS) |

**Row Count:** 7,452

**Note:** UEI (Unique Entity Identifier) replaced DUNS as the standard federal identifier in April 2022.

---

### Dimension Table: `time_dim`

Calendar and fiscal year dimensions.

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `date_id` | INTEGER | NO | Primary key |
| `date` | DATE | NO | Calendar date |
| `year` | INTEGER | NO | Calendar year |
| `month` | INTEGER | NO | Calendar month (1-12) |
| `quarter` | INTEGER | NO | Calendar quarter (1-4) |
| `fiscal_year` | INTEGER | NO | Federal fiscal year (Oct-Sep) |

**Row Count:** 2,160

**Note:** Federal fiscal year starts October 1. FY2024 = Oct 2023 through Sep 2024.

---

### Dimension Table: `geo_dim`

Place of performance geography.

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `geo_id` | INTEGER | NO | Primary key |
| `state_code` | VARCHAR(2) | YES | 2-letter state code |
| `city_name` | VARCHAR(100) | YES | City name |

**Row Count:** 58

---

## Key Business Rules

### Fiscal Year Calculation
```
fiscal_year = CASE
    WHEN month >= 10 THEN year + 1
    ELSE year
END
```

### Fiscal Quarter Calculation
```
fiscal_quarter = ((month - 10) % 12) // 3 + 1
```
- Q1: Oct-Dec
- Q2: Jan-Mar
- Q3: Apr-Jun
- Q4: Jul-Sep

### Deduplication
Records are deduplicated using MD5 hash of: `award_id + recipient_name + award_amount`

---

## Quality Gates

| Gate | Threshold | Description |
|------|-----------|-------------|
| Schema Drift | ≥95% | Expected columns present |
| Freshness | ≥80% | Latest data within 365 days |
| Completeness | ≥90% | Null rate for key fields <5% |
| Duplicates | ≥95% | Unique records by hash |
| Value Sanity | ≥85% | No anomalous values |
| Referential Integrity | ≥95% | FK references exist |

---

## KPI Definitions

### Spend Metrics
| Metric | Formula | Description |
|--------|---------|-------------|
| Total Spend | `SUM(award_amount)` | Aggregate obligations |
| Average Award | `AVG(award_amount)` | Mean award size |
| Median Award | `PERCENTILE(0.5)` | Median award size |

### Concentration Metrics
| Metric | Formula | Description |
|--------|---------|-------------|
| Top-10 Share | `SUM(top_10_vendor_spend) / total_spend` | Market share of top 10 |
| HHI | `Σ(share²) × 10,000` | Herfindahl-Hirschman Index |

**HHI Interpretation:**
- < 1,500: Unconcentrated (healthy competition)
- 1,500-2,500: Moderately Concentrated
- > 2,500: Highly Concentrated

---

## Data Lineage

```
USAspending.gov API
       │
       ▼
┌──────────────────┐
│  Raw Ingestion   │ → raw_awards_fy2024.csv
└──────────────────┘
       │
       ▼
┌──────────────────┐
│  Data Cleaning   │ → cleaned_awards_fy2024.csv
│  - Type casting  │
│  - Null handling │
│  - Deduplication │
└──────────────────┘
       │
       ▼
┌──────────────────┐
│  Star Schema     │ → award_fact.csv
│  Transformation  │ → agency_dim.csv
│                  │ → recipient_dim.csv
│                  │ → time_dim.csv
│                  │ → geo_dim.csv
└──────────────────┘
       │
       ▼
┌──────────────────┐
│  Quality Gates   │ → pipeline_metrics.json
└──────────────────┘
       │
       ▼
┌──────────────────┐
│  KPI Calculation │ → kpis.json
└──────────────────┘
       │
       ▼
┌──────────────────┐
│  PDF Generation  │ → Executive Summary
│                  │ → Methodology/QA
└──────────────────┘
```

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0.0 | 2026-02-01 | Initial data dictionary |

---

*Document maintained as part of P2-FED Pipeline*
