# Federal Awards Pipeline Architecture
## P2-FED: Enterprise Data Engineering on USAspending.gov

**Author:** Mboya Jeffers
**Version:** 1.0.0
**Pipeline:** `/pipelines/federal_awards/pipeline.py` (931 lines)

---

## High-Level Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     FEDERAL AWARDS PIPELINE (P2-FED)                        │
│                    USAspending.gov API → Star Schema → KPIs                 │
└─────────────────────────────────────────────────────────────────────────────┘

     ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
     │ USAspending  │      │   DoD, HHS,  │      │   ~500K+     │
     │    .gov API  │─────▶│ DHS Contracts│─────▶│   Awards     │
     │   (Public)   │      │    FY2024    │      │   Fetched    │
     └──────────────┘      └──────────────┘      └──────────────┘
            │                                           │
            ▼                                           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  EXTRACT                                                                    │
│  ┌────────────────────────────────────────────────────────────────────────┐│
│  │ USASpendingClient                                                      ││
│  │  • Cursor-based pagination (100 records/page)                         ││
│  │  • Rate limiting (0.25s delay)                                        ││
│  │  • Error handling with backoff                                        ││
│  │  • 4 concurrent workers                                               ││
│  └────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  TRANSFORM                                                                  │
│  ┌──────────────────────┐   ┌──────────────────────────────────────────────┐│
│  │  DataCleaner         │   │  DataModeler                                 ││
│  │  • Type conversions  │   │  • Star schema creation                      ││
│  │  • Date parsing      │──▶│  • Dimension tables                          ││
│  │  • String normalization│ │  • Fact table with FKs                       ││
│  │  • Hash generation   │   │  • Surrogate key assignment                  ││
│  └──────────────────────┘   └──────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  VALIDATE                                                                   │
│  ┌────────────────────────────────────────────────────────────────────────┐│
│  │ QualityGateRunner (6 Gates)                                            ││
│  │  ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐││
│  │  │ Schema    │ │ Freshness │ │Completeness│ │ Duplicates│ │  Value   │ ││
│  │  │  Drift    │ │           │ │            │ │           │ │ Sanity   │ ││
│  │  │  (15%)    │ │   (15%)   │ │   (20%)    │ │   (15%)   │ │  (20%)   │ ││
│  │  └───────────┘ └───────────┘ └───────────────┘───────────┘ └───────────┘││
│  │                                    │                                    ││
│  │                            ┌───────────────┐                            ││
│  │                            │ Referential   │                            ││
│  │                            │  Integrity    │                            ││
│  │                            │    (15%)      │                            ││
│  │                            └───────────────┘                            ││
│  └────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  LOAD                                                                       │
│  ┌────────────────────────────────────────────────────────────────────────┐│
│  │ KPICalculator                                                          ││
│  │  • Spend trends (by agency, FY, quarter)                               ││
│  │  • Vendor concentration (HHI index)                                    ││
│  │  • Change detection (QoQ, rank changes)                                ││
│  │  • Summary statistics                                                  ││
│  └────────────────────────────────────────────────────────────────────────┘│
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │ Outputs:                                                                ││
│  │  • raw_awards_fy{year}.csv      → Raw API data                         ││
│  │  • cleaned_awards_fy{year}.csv  → Standardized data                    ││
│  │  • agency_dim.csv               → Agency dimension                     ││
│  │  • recipient_dim.csv            → Vendor dimension                     ││
│  │  • award_fact.csv               → Central fact table                   ││
│  │  • kpis.json                    → Business metrics                     ││
│  │  • pipeline_metrics.json        → Execution telemetry                  ││
│  └─────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Star Schema Data Model

```
                          ┌───────────────────────┐
                          │      agency_dim       │
                          ├───────────────────────┤
                          │ agency_id (PK)        │
                          │ toptier_name          │
                          │ subtier_name          │
                          └───────────────────────┘
                                     │
                                     │ 1:N
                                     ▼
┌───────────────────────┐   ┌───────────────────────────────────────┐   ┌───────────────────────┐
│    recipient_dim      │   │              award_fact               │   │       time_dim        │
├───────────────────────┤   ├───────────────────────────────────────┤   ├───────────────────────┤
│ recipient_id (PK)     │   │ award_id (PK)                         │   │ date_id (PK)          │
│ recipient_name        │◀──│ agency_id (FK)                        │──▶│ date                  │
│ uei                   │   │ recipient_id (FK)                     │   │ year                  │
└───────────────────────┘   │ award_amount                          │   │ month                 │
                            │ total_outlays                         │   │ quarter               │
                            │ start_date                            │   │ fiscal_year           │
                            │ end_date                              │   └───────────────────────┘
                            │ naics_code                            │
                            │ naics_description                     │
                            │ psc_code                              │
                            │ psc_description                       │   ┌───────────────────────┐
                            │ description                           │   │       geo_dim         │
                            │ fiscal_year                           │   ├───────────────────────┤
                            │ fiscal_quarter                        │──▶│ geo_id (PK)           │
                            │ record_hash                           │   │ state_code            │
                            └───────────────────────────────────────┘   │ city_name             │
                                                                        └───────────────────────┘
```

---

## Quality Gate Framework

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         QUALITY GATE FRAMEWORK                              │
│                      (Weighted Score: 0.0 - 1.0)                            │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────┬────────────────────────────────────────────────┬──────────┐
│ Gate            │ Validation Logic                               │ Weight   │
├─────────────────┼────────────────────────────────────────────────┼──────────┤
│ Schema Drift    │ • Expected columns present                     │   15%    │
│                 │ • Key column null rates < 1%                   │          │
│                 │ • Pass threshold: 95%                          │          │
├─────────────────┼────────────────────────────────────────────────┼──────────┤
│ Freshness       │ • Data age assessment                          │   15%    │
│                 │ • Penalty: -50% for data > 2 years old         │          │
│                 │ • Pass threshold: 80%                          │          │
├─────────────────┼────────────────────────────────────────────────┼──────────┤
│ Completeness    │ • Null rate checks on key dimensions           │   20%    │
│                 │ • agency, recipient, amount                    │          │
│                 │ • Pass threshold: 90%                          │          │
├─────────────────┼────────────────────────────────────────────────┼──────────┤
│ Duplicates      │ • MD5 hash-based deduplication                 │   15%    │
│                 │ • award_id + recipient + amount                │          │
│                 │ • Pass threshold: 95%                          │          │
├─────────────────┼────────────────────────────────────────────────┼──────────┤
│ Value Sanity    │ • Negative amount detection                    │   20%    │
│                 │ • Z-score outlier analysis (z > 5)             │          │
│                 │ • Pass threshold: 85%                          │          │
├─────────────────┼────────────────────────────────────────────────┼──────────┤
│ Referential     │ • FK → PK mapping validation                   │   15%    │
│ Integrity       │ • Orphan record detection                      │          │
│                 │ • Pass threshold: 95%                          │          │
└─────────────────┴────────────────────────────────────────────────┴──────────┘

                    ┌────────────────────────────────┐
                    │  Overall Score Calculation     │
                    │  ────────────────────────────  │
                    │  Σ (gate_score × gate_weight)  │
                    │  ─────────────────────────────  │
                    │       Σ (gate_weight)          │
                    └────────────────────────────────┘
```

---

## API Integration Pattern

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    USAspending.gov API INTEGRATION                          │
└─────────────────────────────────────────────────────────────────────────────┘

Endpoint: POST https://api.usaspending.gov/api/v2/search/spending_by_award/

┌─────────────────────────────────────────────────────────────────────────────┐
│  Request Payload                                                            │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │  {                                                                      ││
│  │    "filters": {                                                         ││
│  │      "time_period": [{"start_date": "2023-10-01",                       ││
│  │                       "end_date": "2024-09-30"}],                       ││
│  │      "agencies": [                                                      ││
│  │        {"type": "awarding", "tier": "toptier",                          ││
│  │         "name": "Department of Defense"},                               ││
│  │        {"type": "awarding", "tier": "toptier",                          ││
│  │         "name": "Department of Health and Human Services"},             ││
│  │        {"type": "awarding", "tier": "toptier",                          ││
│  │         "name": "Department of Homeland Security"}                      ││
│  │      ],                                                                 ││
│  │      "award_type_codes": ["A", "B", "C", "D"]                           ││
│  │    },                                                                   ││
│  │    "fields": ["Award ID", "Recipient Name", "Award Amount", ...],       ││
│  │    "limit": 100,                                                        ││
│  │    "page": 1,                                                           ││
│  │    "last_record_unique_id": <cursor>,                                   ││
│  │    "last_record_sort_value": <cursor>                                   ││
│  │  }                                                                      ││
│  └─────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  Pagination Strategy                                                        │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                             │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐                   │
│  │ Page 1  │───▶│ Page 2  │───▶│ Page 3  │───▶│   ...   │───▶ target_rows  │
│  │ 100 rec │    │ 100 rec │    │ 100 rec │    │         │                   │
│  └─────────┘    └─────────┘    └─────────┘    └─────────┘                   │
│       │              │              │                                       │
│       ▼              ▼              ▼                                       │
│  ┌─────────────────────────────────────────────┐                            │
│  │ Cursor-based: last_record_unique_id         │                            │
│  │              + last_record_sort_value       │                            │
│  └─────────────────────────────────────────────┘                            │
│                                                                             │
│  Rate Limiting: 0.25s delay between requests                                │
│  Error Handling: Exponential backoff (2s on error)                          │
│  Max Errors: 10 before abort                                                │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## KPI Calculations

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           KPI FRAMEWORK                                     │
└─────────────────────────────────────────────────────────────────────────────┘

┌───────────────────────────────────────┐
│ 1. SPEND TRENDS                       │
├───────────────────────────────────────┤
│ • by_agency: {total_spend, count,     │
│               avg_award}              │
│ • by_fiscal_year: {total, count}      │
│ • by_quarter: FY_Q1, FY_Q2, etc.      │
└───────────────────────────────────────┘

┌───────────────────────────────────────┐
│ 2. VENDOR CONCENTRATION               │
├───────────────────────────────────────┤
│ • top_10_share: Top 10 vendor %       │
│ • top_20_share: Top 20 vendor %       │
│ • hhi: Herfindahl-Hirschman Index     │
│   ┌─────────────────────────────────┐ │
│   │ HHI = Σ(market_share²) × 10000  │ │
│   │ ─────────────────────────────── │ │
│   │ < 1500: Unconcentrated          │ │
│   │ 1500-2500: Moderately Conc.     │ │
│   │ > 2500: Highly Concentrated     │ │
│   └─────────────────────────────────┘ │
│ • total_vendors                       │
│ • vendors_with_spend                  │
└───────────────────────────────────────┘

┌───────────────────────────────────────┐
│ 3. CHANGE DETECTION                   │
├───────────────────────────────────────┤
│ • qoq_changes: Quarter-over-quarter   │
│ • max_qoq_increase: Biggest jump      │
│ • max_qoq_decrease: Biggest drop      │
│ • top_rank_improvers: Vendors moving  │
│   up in spending rank                 │
└───────────────────────────────────────┘

┌───────────────────────────────────────┐
│ 4. SUMMARY STATISTICS                 │
├───────────────────────────────────────┤
│ • total_spend                         │
│ • avg_award, median_award             │
│ • std_award, min_award, max_award     │
│ • p25, p75, p95 percentiles           │
│ • total_awards                        │
│ • unique_vendors                      │
│ • unique_agencies                     │
└───────────────────────────────────────┘
```

---

## Key Technical Decisions

| Decision | Rationale |
|----------|-----------|
| **Cursor pagination** | More reliable than offset for large datasets; handles concurrent inserts |
| **MD5 record hashing** | Fast deduplication without full-row comparison |
| **Star schema** | Optimized for analytical queries; clear dimension/fact separation |
| **6 quality gates** | Comprehensive coverage: schema, freshness, completeness, duplicates, values, referential integrity |
| **HHI calculation** | Industry-standard market concentration metric; meaningful for procurement analysis |
| **Fiscal year derivation** | Federal FY starts Oct 1; derived from start_date |

---

## Files Produced

| File | Description | Typical Size |
|------|-------------|--------------|
| `raw_awards_fy{year}.csv` | Raw API response data | 50-200 MB |
| `cleaned_awards_fy{year}.csv` | Type-converted, standardized | 40-150 MB |
| `agency_dim.csv` | Agency dimension | < 1 MB |
| `recipient_dim.csv` | Vendor dimension | 1-10 MB |
| `time_dim.csv` | Date dimension | < 1 MB |
| `geo_dim.csv` | Geography dimension | < 1 MB |
| `award_fact.csv` | Central fact table | 30-100 MB |
| `kpis.json` | Calculated business metrics | < 1 MB |
| `pipeline_metrics.json` | Execution telemetry | < 1 MB |

---

## Execution

```bash
# Run pipeline for FY2024 with 500K target rows (default)
python pipeline.py

# Custom fiscal year and target
python pipeline.py --fiscal-year 2023 --target-rows 100000
```

---

*Architecture Document v1.0.0 | Mboya Jeffers | Feb 2026*
