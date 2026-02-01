# P2-SEC Data Dictionary
## SEC EDGAR XBRL Financial Facts Pipeline
**Version:** 1.0.0 | **Last Updated:** 2026-02-01

---

## Overview

This document describes the data model for the P2-SEC pipeline, which extracts standardized financial metrics from SEC EDGAR XBRL filings for public company analysis.

---

## Source System

| Attribute | Value |
|-----------|-------|
| **Source** | SEC EDGAR (data.sec.gov) |
| **Documentation** | sec.gov/search-filings/edgar-application-programming-interfaces |
| **Primary Endpoints** | `/submissions/CIK{cik}.json`, `/api/xbrl/companyfacts/CIK{cik}.json` |
| **Cohort Size** | 39 public companies |
| **Sectors** | Technology, Healthcare, Financial, Consumer, Industrial, Energy |

---

## Data Model

### Fact Table: `xbrl_facts`

Core financial facts extracted from XBRL filings.

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `fact_id` | VARCHAR(16) | NO | MD5 hash identifier |
| `cik` | VARCHAR(10) | NO | SEC Central Index Key |
| `ticker` | VARCHAR(10) | NO | Stock ticker symbol |
| `taxonomy` | VARCHAR(20) | NO | XBRL taxonomy (us-gaap, dei) |
| `concept` | VARCHAR(100) | NO | XBRL concept/tag name |
| `unit` | VARCHAR(20) | YES | Measurement unit (USD, shares) |
| `value` | DECIMAL | YES | Reported value |
| `period_start` | DATE | YES | Period start (null for instant) |
| `period_end` | DATE | NO | Period end / instant date |
| `accession` | VARCHAR(25) | YES | Filing accession number |
| `fiscal_year` | INTEGER | YES | Fiscal year |
| `fiscal_period` | VARCHAR(5) | YES | FY, Q1, Q2, Q3, Q4 |
| `form` | VARCHAR(10) | YES | Filing form type (10-K, 10-Q) |
| `filed` | DATE | YES | Filing date |
| `period_type` | VARCHAR(20) | YES | instant/quarterly/annual |
| `canonical_metric` | VARCHAR(50) | YES | Mapped canonical name |
| `company_id` | INTEGER | YES | FK → company_dim |

**Row Count:** 1,082,675

---

### Dimension Table: `company_dim`

Company master data.

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `company_id` | INTEGER | NO | Primary key |
| `cik` | VARCHAR(10) | NO | SEC Central Index Key |
| `name` | VARCHAR(255) | NO | Legal company name |
| `ticker` | VARCHAR(10) | YES | Stock ticker |
| `sector` | VARCHAR(50) | YES | Industry sector |
| `sic` | VARCHAR(10) | YES | SIC code |
| `sic_description` | VARCHAR(100) | YES | SIC description |
| `fiscal_year_end` | VARCHAR(10) | YES | Fiscal year end (MMDD) |
| `state` | VARCHAR(5) | YES | State of incorporation |

**Row Count:** 39

---

### Dimension Table: `filings_dim`

SEC filing history.

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `filing_id` | INTEGER | NO | Primary key |
| `cik` | VARCHAR(10) | NO | Company CIK |
| `accession` | VARCHAR(25) | NO | Accession number |
| `form` | VARCHAR(10) | NO | Form type (10-K, 10-Q, 8-K) |
| `filing_date` | DATE | YES | Date filed with SEC |
| `report_date` | DATE | YES | Report period end date |
| `primary_document` | VARCHAR(100) | YES | Primary document filename |

**Row Count:** 270

---

### Reference Table: `concept_map`

Mapping from raw XBRL concepts to canonical metrics.

| Column | Data Type | Description |
|--------|-----------|-------------|
| `raw_concept` | VARCHAR(100) | Original XBRL tag name |
| `canonical_metric` | VARCHAR(50) | Standardized metric name |
| `category` | VARCHAR(50) | Financial statement category |

**Row Count:** 27

---

## Canonical Metric Mapping

| Canonical | XBRL Concepts | Category |
|-----------|---------------|----------|
| `revenue` | Revenues, RevenueFromContractWithCustomerExcludingAssessedTax, SalesRevenueNet | Income Statement |
| `cost_of_revenue` | CostOfGoodsAndServicesSold, CostOfRevenue | Income Statement |
| `gross_profit` | GrossProfit | Income Statement |
| `operating_income` | OperatingIncomeLoss | Income Statement |
| `net_income` | NetIncomeLoss, ProfitLoss | Income Statement |
| `eps` | EarningsPerShareDiluted, EarningsPerShareBasic | Income Statement |
| `assets` | Assets | Balance Sheet |
| `liabilities` | Liabilities | Balance Sheet |
| `equity` | StockholdersEquity | Balance Sheet |
| `cash` | CashAndCashEquivalentsAtCarryingValue | Balance Sheet |
| `operating_cash_flow` | NetCashProvidedByUsedInOperatingActivities | Cash Flow |
| `capex` | PaymentsToAcquirePropertyPlantAndEquipment | Cash Flow |

---

## Quality Gates

| Gate | Threshold | Description |
|------|-----------|-------------|
| Unit Consistency | ≥90% | Each concept uses single unit |
| Period Logic | ≥95% | Valid period types |
| Coverage | ≥80% | Required metrics present |
| Restatement | Info | Same concept+period, different values |
| Outliers | ≥85% | Z-score < 5 |

---

## KPI Formulas

| Metric | Formula |
|--------|---------|
| Net Margin | net_income / revenue |
| Gross Margin | gross_profit / revenue |
| ROA | net_income / assets |
| ROE | net_income / equity |

---

## Data Lineage

```
SEC EDGAR API
     │
     ▼
┌──────────────┐
│  Submissions │ → Company info, filing history
│  Endpoint    │
└──────────────┘
     │
     ▼
┌──────────────┐
│ Company Facts│ → XBRL financial facts
│  Endpoint    │
└──────────────┘
     │
     ▼
┌──────────────┐
│ Raw Data     │ → raw_xbrl_facts.csv (1.08M rows)
│              │ → raw_filings.csv
│              │ → raw_companies.csv
└──────────────┘
     │
     ▼
┌──────────────┐
│ Cleaning     │ → Type conversion, period logic
│              │ → Canonical mapping
└──────────────┘
     │
     ▼
┌──────────────┐
│ Star Schema  │ → xbrl_facts, company_dim
│              │ → filings_dim, concept_map
└──────────────┘
     │
     ▼
┌──────────────┐
│ Quality      │ → 5 gates, all passing
│ Gates        │
└──────────────┘
     │
     ▼
┌──────────────┐
│ KPIs         │ → Company metrics, benchmarks
└──────────────┘
     │
     ▼
┌──────────────┐
│ PDFs         │ → Founder, Executive, Methodology
└──────────────┘
```

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0.0 | 2026-02-01 | Initial data dictionary |

---

*Document maintained as part of P2-SEC Pipeline*
