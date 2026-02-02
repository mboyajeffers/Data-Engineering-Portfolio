# Pipeline Architecture Documentation

**Author:** Mboya Jeffers
**Portfolio:** Data Engineering Portfolio

---

## Overview

This folder contains visual architecture documentation for the top production pipelines in the portfolio. These documents are designed to demonstrate enterprise-grade data engineering capabilities to technical interviewers and hiring managers.

---

## Documents

| Document | Pipeline | Key Highlights |
|----------|----------|----------------|
| [FEDERAL_AWARDS_ARCHITECTURE.md](FEDERAL_AWARDS_ARCHITECTURE.md) | P2-FED | 931-line pipeline, USAspending.gov API, star schema, 6 quality gates, HHI market concentration |
| [SEC_FINANCIAL_ARCHITECTURE.md](SEC_FINANCIAL_ARCHITECTURE.md) | P2-SEC | 889-line pipeline, SEC EDGAR XBRL, 40+ companies, cohort benchmarking, financial ratios |

---

## Why These Pipelines?

### 1. Federal Awards (P2-FED)
- **Complexity:** Largest codebase (931 lines)
- **Scale:** 500K+ federal contract records
- **Quality:** 6-layer validation framework with weighted scoring
- **Analysis:** HHI (Herfindahl-Hirschman Index) for vendor concentration
- **Pattern:** Cursor-based pagination for large datasets

### 2. SEC Financial (P2-SEC)
- **Complexity:** Multi-source ingestion (submissions + XBRL facts)
- **Scale:** 40+ public companies, 500K+ XBRL facts
- **Mapping:** Raw XBRL tags to canonical financial metrics
- **Analysis:** Cross-company benchmarking with percentile rankings
- **Domain:** Financial statement expertise (income, balance sheet, cash flow)

---

## Common Architecture Patterns

Both pipelines share enterprise-grade patterns:

```
┌────────────────────────────────────────────────────────────┐
│                    SHARED PATTERNS                          │
├────────────────────────────────────────────────────────────┤
│ 1. Modular Architecture                                    │
│    DataClient → DataIngestion → DataCleaner →              │
│    DataModeler → QualityGateRunner → KPICalculator         │
├────────────────────────────────────────────────────────────┤
│ 2. Star Schema Modeling                                    │
│    Dimension tables + Central fact table                   │
├────────────────────────────────────────────────────────────┤
│ 3. Quality Gate Framework                                  │
│    Weighted scoring, pass thresholds, issue logging        │
├────────────────────────────────────────────────────────────┤
│ 4. Pipeline Telemetry                                      │
│    Duration, API calls, errors, quality scores             │
├────────────────────────────────────────────────────────────┤
│ 5. Idempotent Outputs                                      │
│    Deterministic file outputs, hash-based deduplication    │
└────────────────────────────────────────────────────────────┘
```

---

## Skills Demonstrated

| Category | Skills |
|----------|--------|
| **Data Engineering** | ETL pipeline design, API integration, star schema modeling |
| **Python** | Dataclasses, type hints, pandas, numpy, concurrent.futures |
| **Quality** | Data validation, null handling, duplicate detection, referential integrity |
| **APIs** | REST API consumption, rate limiting, pagination, error handling |
| **Domain Knowledge** | Federal procurement (NAICS, PSC), SEC XBRL (us-gaap taxonomy) |
| **Best Practices** | Logging, metrics collection, modular design, documentation |

---

## How to Use These Documents

**For Interviewers:** These diagrams demonstrate system design thinking and production-grade architecture. The ASCII diagrams work in any environment (terminals, GitHub, plain text editors).

**For Code Review:** Cross-reference with actual pipeline code in `/pipelines/federal_awards/pipeline.py` and `/pipelines/sec_financial/pipeline.py`.

**For Portfolio Presentation:** Use as talking points during technical discussions about data engineering approaches.

---

*Architecture Documentation v1.0.0 | Mboya Jeffers*
