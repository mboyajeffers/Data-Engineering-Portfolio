# Project2 Execution Plan - CMS Public Data Enterprise Portfolio
## Version: 1.0.0 | Created: 2026-02-01

---

## Overview

Three enterprise-grade portfolio projects using **public, verifiable data** processed through **CMS** to demonstrate mid→senior data engineering skills.

| Project | Data Source | Enterprise Problem | Target Rows |
|---------|-------------|-------------------|-------------|
| P2-FED | USAspending.gov API | Procurement Spend Intelligence + DQ SLA | ~3M rows |
| P2-SEC | SEC EDGAR XBRL APIs | Financial Facts + Cohort Benchmarking | ~500K rows |
| P2-AIR | BTS TranStats + NOAA CDO | Airline Reliability + Weather Attribution | ~7M rows |

---

## Folder Structure

```
L_P_2/
├── {Vertical}/reports/
│   ├── Project1/  (v2.0 existing reports)
│   └── Project2/  (NEW - these 3 projects)
│       ├── P2-FED/  (Federal Procurement)
│       ├── P2-SEC/  (SEC EDGAR)
│       └── P2-AIR/  (Airline + Weather)
├── Project2_EXECUTION_PLAN.md
├── Project2_CHECKPOINT.md
└── Project2_ISSUES_LOG.md
```

---

## Phase 1: Infrastructure Setup

### 1.1 Create Project2 Structure
- [ ] Create P2-FED, P2-SEC, P2-AIR folders in each vertical's Project2
- [ ] Create data/, reports/, docs/ subfolders
- [ ] Initialize checkpoint and issues log

### 1.2 API Key Verification
- [ ] Verify NOAA API key (already in CMS: fhALNmXtkuwaBufVbeGmEyyPvOHuYGjM)
- [ ] USAspending.gov: No key required (public)
- [ ] SEC EDGAR: User-Agent header required (no key)

### 1.3 CMS Engine Assessment
- [ ] Map projects to existing CMS engines
  - P2-FED → compliance/finance engines
  - P2-SEC → finance/brokerage engines
  - P2-AIR → weather engine + new ops engine

---

## Phase 2: Project P2-FED (Federal Procurement)

### 2.1 Data Ingestion
**Source:** USAspending.gov API
**Endpoints:**
- `/api/v2/search/spending_by_award/`
- `/api/v2/search/spending_by_geography/`
- `/api/v2/awards/`
- `/api/v2/recipient/`

**Partition Strategy:** Fiscal Year + Month
**Target Agencies:** DoD, HHS, DHS (initial scope)

### 2.2 Data Model
```
award_fact (award_id, agency_id, recipient_id, award_type, obligation, outlay, action_date, naics, psc)
recipient_dim (uei, name, category, parent)
agency_dim (toptier_code, subtier_code, name)
time_dim (date, fiscal_year, fiscal_quarter, month)
```

### 2.3 Quality Gates
- [ ] Schema drift detection
- [ ] Freshness check (action_date within window)
- [ ] Completeness (% nulls for key dims)
- [ ] Duplicate detection (award_id + action_date)
- [ ] Value sanity (negative obligations, z-score spikes)
- [ ] Referential integrity

### 2.4 KPIs
- Spend trend by agency/category/time
- Vendor concentration (Top-10 share, HHI)
- Change detection (MoM spikes, new dominant vendors)
- Data reliability score (weighted gate composite)

### 2.5 Deliverables
- [ ] Exec Summary PDF (1 page)
- [ ] Methodology/QA PDF (1-2 pages)
- [ ] Pipeline diagram (PNG)
- [ ] Data dictionary (MD)
- [ ] Sample data (<= 1,000 rows)

---

## Phase 3: Project P2-SEC (SEC EDGAR XBRL)

### 3.1 Data Ingestion
**Source:** SEC EDGAR data.sec.gov
**Endpoints:**
- `https://data.sec.gov/submissions/CIK##########.json`
- `https://data.sec.gov/api/xbrl/companyfacts/CIK##########.json`
- `https://data.sec.gov/api/xbrl/frames/{taxonomy}/{tag}/{unit}/{period}.json`

**Cohort:** 25-50 public companies (diverse sectors)
**Headers:** User-Agent with app name + contact email

### 3.2 Data Model
```
filings_dim (cik, accession, form, filed_at, report_period_end)
xbrl_facts (cik, taxonomy, tag, unit, period_start, period_end, value, accession)
company_dim (cik, name, ticker, sector)
concept_map (raw_tag → canonical_metric)
```

### 3.3 Quality Gates
- [ ] Unit consistency per concept
- [ ] Period logic (quarterly vs annual vs instant)
- [ ] Coverage (% required metrics present)
- [ ] Restatement detection (same concept+period, different values)
- [ ] Outlier detection (QoQ deltas, margin flips)

### 3.4 KPIs
- Revenue growth, gross/operating/net margin
- CFO, CapEx, FCF proxy
- Cohort benchmarking (percentile ranks)
- Volatility measures
- Data reliability score

### 3.5 Deliverables
- [ ] Exec Summary PDF (company deep-dive + cohort)
- [ ] Methodology/QA PDF
- [ ] Lineage graphic (CIK → filings → facts → KPIs)
- [ ] Concept mapping docs
- [ ] Sample data

---

## Phase 4: Project P2-AIR (Airline + Weather)

### 4.1 Data Ingestion
**Sources:**
- BTS TranStats: https://www.transtats.bts.gov/ontime/
- NOAA CDO API v2: https://www.ncdc.noaa.gov/cdo-web/webservices/v2

**Partition:** Month
**Weather:** Station selection near major airports

### 4.2 Data Model
```
flight_fact (flight_date, carrier, flight_num, origin, dest, dep_delay, arr_delay, cancelled, diversion, delay_causes)
airport_dim (code, city, state, lat, lon)
carrier_dim (code, name)
weather_fact (station, date_hour, temp, precip, wind, visibility)
flight_weather_fact (airport, hour, joined grain)
```

### 4.3 Quality Gates
- [ ] Timezone normalization (local vs UTC)
- [ ] Late data handling (idempotent reruns)
- [ ] Join coverage (% flights with weather match)
- [ ] Outlier checks (delays > 24h, negative values)
- [ ] Delay reason code coverage

### 4.4 KPIs
- Reliability leaderboard (on-time %, p50/p90 delay, cancel rate)
- Bottleneck analysis (worst airports/routes/carriers)
- Weather attribution (high-precip/wind vs baseline)
- Incident detection (anomaly flags)

### 4.5 Deliverables
- [ ] Exec Summary PDF
- [ ] Methodology/QA PDF
- [ ] Attribution note (correlation vs causation)
- [ ] Pipeline diagram
- [ ] Sample data

---

## Phase 5: CMS Integration

### 5.1 Pipeline Execution
- [ ] Create intake jobs for each project
- [ ] Configure engine parameters
- [ ] Set up quality gate thresholds
- [ ] Configure PDF generation templates

### 5.2 Traceability Panel (All PDFs)
Each PDF includes:
- Source(s) + documentation links
- Parameters (date range, filters, entities)
- As-of timestamp (UTC)
- Row counts (raw → cleaned → modeled)
- Quality gates (pass/fail + top 3 issues)
- Output version (report ID, git commit)

### 5.3 CMS Endpoints Used
- `/api/health` - Service health
- `/api/engines` - Engine registry
- `/api/jobs` - Run orchestration
- `/api/pdf/generate` - Artifact generation

---

## Phase 6: Documentation & Publishing

### 6.1 Per-Project Docs
- [ ] Pipeline diagram (Mermaid → PNG)
- [ ] Data dictionary (MD)
- [ ] Quality checks list + thresholds (MD)
- [ ] Sample rows (CSV, <= 1,000 rows)
- [ ] Schema definition (YAML/JSON)

### 6.2 LinkedIn Post Templates
- Problem statement (1-2 lines)
- What I built (2-4 lines)
- Engineering highlights (bullets)
- Artifacts list
- Disclosure statement

### 6.3 Safety Checklist
**Publish:**
- PDFs (exec + methodology)
- Diagrams
- Data dictionaries
- Sample data only

**Keep Private:**
- CMS engine internals
- Secrets/tokens/keys
- Auth endpoints
- Proprietary implementation

---

## Timeline Estimate

| Phase | Description | Complexity |
|-------|-------------|------------|
| 1 | Infrastructure Setup | Low |
| 2 | P2-FED (Federal) | High |
| 3 | P2-SEC (EDGAR) | High |
| 4 | P2-AIR (Airline) | High |
| 5 | CMS Integration | Medium |
| 6 | Documentation | Medium |

---

## Success Criteria

- [ ] All 3 projects have Exec Summary + Methodology PDFs
- [ ] Traceability panels on all PDFs
- [ ] Quality gates documented and passing
- [ ] Pipeline diagrams created
- [ ] Data dictionaries complete
- [ ] Sample data (<= 1,000 rows) exported
- [ ] LinkedIn posts drafted
- [ ] No secrets/tokens in published artifacts

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0.0 | 2026-02-01 | Initial execution plan |

---

*Plan maintained by Claude Code*
