# Mboya Jeffers | Data Engineering Portfolio

**Data Engineer & Analytics Engineer** — Building production-grade analytics pipelines that transform raw data into actionable intelligence.

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue)](https://linkedin.com/in/mboyajeffers)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-black)](https://github.com/mboyajeffers)
[![Email](https://img.shields.io/badge/Email-Contact-green)](mailto:MboyaJeffers9@gmail.com)

---

## About Me

I'm a Data Engineer with full-stack pipeline ownership—from raw data ingestion through KPI computation to automated report delivery. I built **CMS Enterprise**, a production analytics platform processing data across 11 industries with 188+ KPIs, multi-currency support, and real-time market data integration.

This repository contains **proof of capability**: real analytics outputs I've generated across multiple industries, demonstrating enterprise-grade data engineering and analytics engineering practices.

### What I Bring

| Capability | Evidence |
|------------|----------|
| **Large-Scale Processing** | 23.6M rows (Netflix), 7.7M rows (Disney) processed with production pipelines |
| **Multi-Industry Expertise** | Finance, Betting, Sports, Compliance, Solar, Weather, Media |
| **End-to-End Ownership** | Ingestion → Validation → Transformation → KPIs → Executive Reports |
| **Production Systems** | 11 analytics engines, 188+ KPIs, 30+ currencies in production |

---

## Portfolio Highlights

### Enterprise Scale Analytics

| Project | Dataset Size | Processing Time | Key Deliverables |
|---------|--------------|-----------------|------------------|
| [Netflix Streaming Analysis](./Netflix_Analysis/) | **23.6M rows** | ~2.5 hours | Content performance, viewing patterns, engagement metrics |
| [Disney Media Analysis](./Enterprise_Scale/) | **7.7M rows** | ~50 min | Cross-platform analytics, revenue correlation, audience segmentation |

### Industry-Specific Analytics

| Industry | Folder | Sample KPIs |
|----------|--------|-------------|
| [Finance](./Finance/) | Stock analytics, risk metrics | VaR, Sharpe Ratio, Beta, Max Drawdown |
| [Betting](./Betting/) | Sports betting analytics | GGR, Hold %, Player LTV, Bonus ROI |
| [Sports](./Sports/) | Sports data intelligence | Win rates, performance trends, market analysis |
| [Compliance](./Compliance/) | AML/KYC analytics | Transaction patterns, risk scoring, audit trails |
| [Solar](./Solar/) | Renewable energy analytics | Capacity factor, generation forecasts, efficiency |
| [Weather](./Weather/) | Climate analytics | Heat stress, freeze risk, precipitation patterns |

---

## Technical Skills Demonstrated

### Data Engineering
- **Pipeline Design**: Idempotent, re-runnable ETL/ELT pipelines with checkpoint recovery
- **Schema Enforcement**: Contract-driven validation with 150+ column aliases for normalization
- **Data Quality**: Multi-stage validation (nulls, duplicates, ranges, referential integrity)
- **Scale**: Chunked processing for multi-million row datasets
- **Observability**: Structured logging, telemetry, and audit trails

### Analytics Engineering
- **KPI Frameworks**: 188+ standardized metrics across 11 industry verticals
- **Financial Analytics**: VaR, Sharpe, Sortino, Beta using CFA/Basel standards
- **Compliance Metrics**: AML pattern detection, threshold monitoring, audit reporting
- **Report Generation**: Automated PDF/CSV/Excel/Parquet delivery

### Infrastructure
- **Cloud**: GCP (Compute Engine, Cloud Storage), Nginx, Gunicorn
- **APIs**: Flask REST APIs, Google Drive/Sheets integration, webhooks
- **DevOps**: Git, CI/CD, systemd services, automated backups
- **Real-Time Data**: Yahoo Finance, FRED, NREL, NOAA, NASA POWER, Open-Meteo APIs

---

## The Platform I Built: CMS Enterprise

All analytics in this portfolio were generated using **CMS Enterprise**, a platform I architected and built:

| Metric | Value |
|--------|-------|
| Industry Engines | 11 (Fintech, Brokerage, Crypto, Gaming, Betting, Ecommerce, Oil & Gas, Solar, Compliance, Media, Weather) |
| Total KPIs | 188+ computed metrics |
| Currencies | 32 supported with real-time FX |
| Countries | 14 supported |
| API Endpoints | 30 REST blueprints |
| Test Coverage | 22 passing tests |

**Architecture**: Python/Flask backend, GCP infrastructure, SQLite metadata, GCS storage, automated PDF generation, three-tier deployment (PROD → DEV → LOCAL).

---

## Sample Outputs

### Executive Reports
Each industry folder contains production-quality deliverables:
- **Executive Summary PDFs** — Board-ready analytics summaries
- **KPI Dashboards** — Metric breakdowns with visualizations
- **Data Quality Scorecards** — Validation results and data health
- **Cleaned Datasets** — Normalized, validated data exports

### Example: Netflix 23.6M Row Analysis
```
Netflix_Analysis/
├── Netflix_Streaming_Analysis_Executive_Summary.pdf
├── Netflix_Streaming_Analysis_Report.pdf
├── LINKEDIN_ARTICLE.md
├── README.md
└── data/
    └── netflix_titles.csv
```

---

## Repository Structure

```
Proof_Package/
├── README.md                    # You are here
├── Netflix_Analysis/            # 23.6M row streaming analytics
├── Enterprise_Scale/            # 7.7M row Disney media analytics
├── Finance/                     # Stock & financial risk analytics
├── Betting/                     # Sports betting analytics
├── Sports/                      # Sports data intelligence
├── Compliance/                  # AML/KYC compliance analytics
├── Solar/                       # Renewable energy analytics
├── Weather/                     # Climate & weather analytics
├── docs/                        # Methodology, architecture, KPI catalog
├── configs/                     # Schema contracts, validation rules
├── generate_*.py                # Report generation scripts
└── LinkedIn_Posts_Ready_To_Copy.md  # Content for professional sharing
```

---

## Contact

**Mboya Jeffers** — Data Engineer / Analytics Engineer

- **Email**: MboyaJeffers9@gmail.com
- **LinkedIn**: [linkedin.com/in/mboyajeffers](https://linkedin.com/in/mboyajeffers)
- **GitHub**: [github.com/mboyajeffers](https://github.com/mboyajeffers)
- **Location**: Remote (US-based)

---

## What I'm Looking For

**Target Roles**: Data Engineer, Analytics Engineer, Senior Data Engineer

**Ideal Environment**:
- Companies solving complex data problems at scale
- Teams that value clean architecture and production discipline
- Roles with end-to-end pipeline ownership
- Remote-first organizations

**Compensation**: $125K+ base (negotiable based on total package)

---

*This portfolio demonstrates real capabilities with synthetic data. All outputs were generated using pipelines I designed and built.*
