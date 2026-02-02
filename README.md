# Data Engineering Portfolio

Production-grade data pipelines demonstrating ETL patterns, public API integration,
star schema modeling, and data quality practices.

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue)](https://linkedin.com/in/mboya-jeffers-6377ba325)
[![Email](https://img.shields.io/badge/Email-Contact-green)](mailto:MboyaJeffers9@gmail.com)

---

## Projects

Each pipeline is self-contained, uses real public APIs, and produces verifiable outputs.

### Federal Awards Analysis
**Source:** USASpending.gov API
**Scale:** 100K+ federal awards processed
**Pattern:** Star schema dimensional model (agency, recipient, geography dimensions)
**Verification:** [api.usaspending.gov](https://api.usaspending.gov)

```bash
cd pipelines/federal_awards
python pipeline.py
```

### SEC Financial Intelligence
**Source:** SEC EDGAR XBRL API
**Scale:** 36 Fortune 500 companies, 1M+ financial facts
**Pattern:** XBRL extraction, financial metrics (revenue, net income, EPS)
**Verification:** [data.sec.gov](https://www.sec.gov/cgi-bin/browse-edgar)

```bash
cd pipelines/sec_financial
python pipeline.py
```

### Healthcare Quality Metrics
**Source:** CMS Hospital Compare API (Data.Medicare.gov)
**Scale:** 942 hospitals analyzed across 10 states
**Pattern:** Ownership-type benchmarking, quality score aggregation
**Verification:** [data.cms.gov](https://data.cms.gov/provider-data/)

```bash
cd pipelines/healthcare_quality
python pipeline.py
```

### Energy Grid Monitoring
**Source:** EIA-930 API (U.S. Energy Information Administration)
**Scale:** 5K records from 7 balancing authorities
**Pattern:** Time-series aggregation, renewables percentage tracking
**Verification:** [api.eia.gov](https://www.eia.gov/opendata/)

```bash
cd pipelines/energy_grid
python pipeline.py
```

### Vulnerability Prioritization
**Source:** NIST NVD + CISA KEV + FIRST EPSS APIs
**Scale:** 2K CVEs with priority scoring
**Pattern:** Multi-source enrichment, risk-based prioritization
**Verification:** [nvd.nist.gov](https://nvd.nist.gov/developers)

```bash
cd pipelines/vulnerability_scoring
python pipeline.py
```

---

## Tech Stack

| Category | Technologies |
|----------|--------------|
| Language | Python 3.9+ |
| Data | pandas, numpy |
| APIs | requests (rate limiting, retry logic) |
| Output | JSON, CSV, Parquet |
| Quality | Data validation gates, completeness checks |

---

## Engineering Practices Demonstrated

- **Schema Validation** — Contract-driven column validation with 150+ field alias mappings
- **Quality Gates** — Completeness, uniqueness, range validation on every pipeline run
- **Rate Limiting** — Exponential backoff for API reliability under throttling
- **Star Schema** — Fact and dimension tables with surrogate keys
- **Idempotent Pipelines** — Safe to re-run without side effects
- **Observability** — Structured logging, pipeline metrics, audit trails

---

## Repository Structure

```
Proof_Package/
├── README.md
├── pipelines/
│   ├── federal_awards/       # USASpending API
│   ├── sec_financial/        # SEC EDGAR XBRL
│   ├── healthcare_quality/   # CMS Hospital Compare
│   ├── energy_grid/          # EIA-930 grid data
│   └── vulnerability_scoring/# NIST NVD + CISA KEV
├── reports/
│   ├── founder_summaries/    # One-page project summaries
│   └── executive_reports/    # Detailed analysis PDFs
└── _archive/                 # Previous versions
```

---

## Sample Output

Each pipeline produces:
- **Raw data** — Unmodified API responses (CSV)
- **Cleaned data** — Normalized, validated datasets
- **Star schema** — Fact and dimension tables
- **KPIs** — Computed metrics (JSON)
- **Pipeline metrics** — Runtime, API calls, quality scores

---

## About

Built by **Mboya Jeffers** — Data Engineer with full-stack pipeline ownership.

These are simplified versions of patterns I use in production data systems.
All data sources are public APIs; outputs are independently verifiable.

**Contact:** MboyaJeffers9@gmail.com
**LinkedIn:** [linkedin.com/in/mboya-jeffers-6377ba325](https://linkedin.com/in/mboya-jeffers-6377ba325)
**Location:** Remote (US-based)

---

## What I'm Looking For

**Target Roles:** Data Engineer, Analytics Engineer, Senior Data Engineer

**Ideal Environment:**
- Companies solving complex data problems at scale
- Teams that value clean architecture and production discipline
- Roles with end-to-end pipeline ownership
- Remote-first organizations

---

*All portfolio data is verifiable via public APIs. No simulated or synthetic data.*
