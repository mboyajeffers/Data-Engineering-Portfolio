# Data Engineering Portfolio — Mboya Jeffers

![CI](https://github.com/mboyajeffers/Data-Engineering-Portfolio/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

**4.3M+ verified rows | 8 industries | 100+ public tests | ML pipelines | Weekly Intelligence Reports**

---

## What's In This Folder

```
Experience_Folder/
│
├── projects/                  ← THE WORK. 8 data engineering projects (4.3M+ rows)
│   ├── v2_foundation/         P01-P04: Finance, Gov, Health, Energy (7.5M rows)
│   └── v3_scale/              P05-P08: Gaming, Betting, Media, Crypto (31M rows)
│
├── pipelines/                 ← REUSABLE ETL FRAMEWORK. Shared extractors, transformers, schemas
│   ├── etl_framework/         Base classes, star-schema definitions, pipeline registry
│   ├── sec_financial/         SEC EDGAR pipeline
│   ├── federal_awards/        USASpending pipeline
│   ├── healthcare_quality/    Medicare Part D pipeline
│   ├── energy_grid/           EIA-930 pipeline
│   └── microsoft_gaming/      Gaming risk metrics (standalone sub-project w/ tests)
│
├── platform/                  ← PRODUCTION INFRASTRUCTURE. Everything needed to run at scale
│   ├── ci-cd/                 GitHub Actions workflows (lint, test, security, SBOM, deploy)
│   ├── infrastructure/        Terraform (GCP), systemd service units
│   ├── monitoring/            SLI/SLO tracker, alerting, cron health checks
│   ├── security/              RBAC, immutable audit trail, GCP Secret Manager
│   └── operations/            Deploy, rollback, and backup scripts
│
├── docs/                      ← ARCHITECTURE DOCS. Deep-dive design docs per pipeline
│   └── architecture/          9 architecture docs (SEC, Federal, Healthcare, Energy,
│                              Gaming, Betting, Media, Crypto, Microsoft Gaming)
│
├── reports/                   ← SAMPLE PDFS. Generated reports demonstrating output quality
│   ├── weekly_intelligence/   Monday morning reports (Finance, Crypto, Executive Summary)
│   ├── executive_reports/     Executive summaries (SEC, Federal Awards, Aviation)
│   ├── founder_summaries/     One-page briefs per project (6 PDFs)
│   ├── industry_analysis/     Industry deep-dives (Finance, Compliance, Solar)
│   └── gaming/                Gaming sector analysis (Xbox/Activision)
│
├── demos/                     ← STANDALONE CODE EXAMPLES. Reusable patterns
│   ├── data-validation/       Data quality validation patterns
│   ├── etl-pipeline-template/ Template for building new ETL pipelines
│   ├── financial-metrics/     12-KPI risk calculation module (with tests)
│   └── multi-currency-fx/     Multi-currency conversion utility
│
└── tests/                     ← TEST SUITE. Data quality + schema + surrogate key tests
```

---

## The 8 Projects

### v2 Foundation

| # | Project | Industry | Capacity | Data Source | What It Proves |
|---|---------|----------|----------|-------------|----------------|
| P01 | SEC Financial Intelligence | Finance | 1M | SEC EDGAR XBRL | XBRL parsing, 50+ financial KPIs |
| P02 | Federal Contract Awards | Government | 1M | USASpending.gov API | REST pagination, agency analytics |
| P03 | Medicare Prescriber Analysis | Healthcare | 5M | Medicare Part D | Bulk file processing, opioid patterns |
| P04 | Energy Grid Analytics | Energy | 500K | EIA-930 API | Time-series, generation mix tracking |

### v3 Scale

| # | Project | Industry | Capacity | Data Source | What It Proves |
|---|---------|----------|----------|-------------|----------------|
| P05 | Gaming Analytics | Gaming | 8M | Steam API, SteamSpy | Multi-source aggregation, retention |
| P06 | Betting & Sports | Betting | 8M | ESPN API | Odds modeling, spread accuracy |
| P07 | Media & Streaming | Media | 10M | IMDB Datasets | Bayesian ratings, content trends |
| P08 | Crypto & Blockchain | Crypto | 5M | CoinGecko API | Volatility, DeFi TVL, on-chain |

*Capacity = design-scale target per pipeline. Verified ETL output across all projects: **4.3M+ rows**.*

---

## Each Project Contains

```
P0X_{name}/
├── src/
│   ├── extract.py       # Data source integration (API, bulk file)
│   ├── transform.py     # Kimball star-schema dimensional modeling
│   ├── analytics.py     # Industry-specific KPI calculations
│   └── main.py          # Pipeline orchestration
├── sql/
│   └── schema.sql       # Dimension + fact table DDL
├── evidence/
│   └── P0X_evidence.json  # Extraction metadata + validation
└── README.md
```

---

## Weekly Intelligence Reports

Live pipeline execution producing Monday morning reports from 8 independent data sources. Each report is generated end-to-end: API extraction, Kimball star schema transformation, KPI computation, PDF rendering.

| Report | Data Source | Records | What It Shows |
|--------|-----------|---------|--------------|
| Finance Weekly Intelligence | FRED API | 368K | 50 macro series — GDP, CPI, yield curve, labor, money supply |
| Trading Performance | Yahoo Finance | 529K | 200 securities, 5yr OHLCV, sector rotation |
| Crypto Market Intelligence | CoinGecko | 21K | Top coins, market cap, volatility |
| Gaming Industry Metrics | Steam/SteamSpy | 37K | Player engagement, pricing, genre distribution |
| Weekend Sports Recap | ESPN | 21K | 4 leagues, standings, conference rankings |
| Climate & Weather Summary | Open-Meteo | 2.7M | 30 cities, hourly + daily, 10-year history |
| Solar Generation Report | NREL PVWatts | 500 | 10 US locations, monthly AC output |
| Regulatory Filing Monitor | SEC EDGAR | 570K | XBRL financial facts, filing patterns |
| **Executive Summary** | **All 8 sources** | **4.3M** | **Cross-industry overview** |

89 total reports: 23 recurring + 39 industry analysis + 12 methodology/summaries + 11 enterprise showcase + 4 samples. Sample reports: [`reports/weekly_intelligence/`](reports/weekly_intelligence/)

### Related Repositories

| Repo | Focus | Content |
|------|-------|---------|
| [financial-data-engineering](https://github.com/mboyajeffers/financial-data-engineering) | Engineering | ETL pipeline code, data quality framework, star schema modeling, 68 tests |
| [financial-market-analysis](https://github.com/mboyajeffers/financial-market-analysis) | Analysis | 89 intelligence reports (PDFs), 3 white glove live-data demos, enterprise showcase |

---

## ML & Trading Analytics System

Production ML pipeline running on GCP — built on the same ETL foundation as the 8 portfolio projects above.

### Day Trading Signals

| Model | Accuracy | Description |
|-------|----------|-------------|
| Momentum Classifier | **78.3%** | Direction signal for intraday breakout entries |
| Momentum Classifier (Scalp) | **74.7%** | Short-window variant for high-frequency setups |
| Intraday VaR | — | Position-level value-at-risk, parametric |
| Volume Anomaly | — | Unusual volume detection for signal confirmation |

25 tickers. Daily signal generation cron (5PM ET). Live paper execution via broker API.

### Swing Trading Models

| Model | In-Sample | Walk-Forward OOS | Description |
|-------|-----------|-----------------|-------------|
| swing_classifier v2 | **69.1%** | 30.5% (5-fold, 1yr/1qtr) | 5-day direction classification, 17 features |
| swing_var v2 | — | — | GARCH(1,1) volatility model, 25 tickers |

12,500+ daily bars seeded (2yr history). Walk-forward backtested across 5 folds. Paper trading active.

### Infrastructure

```
GCP VM (e2-highmem-2, 16GB RAM, 50GB SSD)
├── PostgreSQL — time-series bars, model registry, trade journal
├── Flask API — 14+ endpoints (signals, execution, risk, journal)
├── Nginx — reverse proxy + SSL
├── systemd — 4 services (orchestrator, data sync, intake watcher, scheduler)
├── Cron — daily 5PM data fetch + weekly retrain
└── Electron desktop app — native macOS wrapper for cockpit UI
```

---

## Skills Demonstrated

| Category | Technologies |
|----------|-------------|
| Languages | Python 3.x, SQL |
| Data Processing | Pandas, NumPy, Parquet |
| Databases | PostgreSQL 16 |
| Data Modeling | Kimball Star Schema, surrogate keys, bridge tables |
| APIs | REST clients, rate limiting, pagination, circuit breakers |
| Infrastructure | Terraform (GCP), systemd, Nginx, GitHub Actions CI/CD |
| Observability | SLI/SLO tracking, error budgets, structured alerting |
| Security | RBAC, immutable audit trails, GCP Secret Manager |
| Machine Learning | scikit-learn, statsmodels, GARCH(1,1), feature engineering, walk-forward backtesting, live paper execution |
| Desktop | Electron (macOS native app, session auth, real-time cockpit) |
| Reporting | Automated PDF generation, KPI dashboards, WeasyPrint |
| Industries | Finance, Government, Healthcare, Energy, Gaming, Betting, Media, Crypto |

---

## Contact

**Mboya Jeffers** — Data & ML Engineer

- **Email:** MboyaJeffers9@gmail.com
- **LinkedIn:** linkedin.com/in/mboya-jeffers-6377ba325
- **GitHub:** github.com/mboyajeffers
- **Location:** Remote (US-based)

---

*All data sourced from public APIs — independently verifiable*
