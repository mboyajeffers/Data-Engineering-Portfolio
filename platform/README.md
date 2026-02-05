# Production Platform Engineering

**Author:** Mboya Jeffers
**Stack:** Python 3.12 | Flask | PostgreSQL 16 | GCP | Terraform | GitHub Actions

---

## What This Is

This directory contains the operational infrastructure I built and run for a **production analytics platform** serving 11 industry verticals with 188+ KPIs. Every file here runs in production on GCP.

This isn't a tutorial or a toy project. This is how I operate real systems.

---

## Platform at a Glance

| Metric | Value |
|--------|-------|
| **Industry Engines** | 11 (Finance, Brokerage, Compliance, Crypto, Ecommerce, Gaming, Media, Oil & Gas, Solar, Weather, Betting) |
| **KPIs Computed** | 188+ across all verticals |
| **ETL Pipelines** | 11 registered, fully orchestrated with lineage tracking |
| **Automated Tests** | 523 (security, integration, data quality, contracts, RBAC) |
| **API Endpoints** | 60+ across 7 blueprint groups |
| **Uptime SLO** | 99.5% availability, p95 < 500ms |
| **Infrastructure** | GCP e2-medium, Ubuntu 24.04, 6 systemd services |

---

## Directory Structure

```
platform/
|
+-- ci-cd/                        # Continuous Integration & Deployment
|   +-- ci.yml                    # 5-job CI: lint, test, security, SBOM, build
|   +-- cd.yml                    # CD: test -> deploy -> smoke test
|   +-- dependabot.yml            # Automated dependency updates
|
+-- infrastructure/               # Infrastructure as Code
|   +-- terraform/                # GCP provisioning (VM, firewall, GCS)
|   |   +-- main.tf              # Compute instance + startup script
|   |   +-- firewall.tf          # 6 network security rules
|   |   +-- gcs.tf               # 3 storage buckets with lifecycle policies
|   |   +-- variables.tf         # Parameterized configuration
|   |   +-- outputs.tf           # Exported resource attributes
|   +-- systemd/                  # Service management units
|       +-- app-web.service       # Flask/Gunicorn web application
|       +-- app-orchestrator.service  # Job processing engine
|       +-- app-intake.service    # File upload detection
|
+-- monitoring/                   # Observability & SLO Tracking
|   +-- health_monitor.py         # SLI/SLO engine (p50/p95/p99, error budget)
|   +-- alerting.py               # Multi-channel alerts (email + JSONL)
|   +-- monitoring_cron.py        # 5-minute automated health checks
|
+-- security/                     # Access Control & Audit
|   +-- rbac.py                   # 4-role RBAC with wildcard permissions
|   +-- audit.py                  # Immutable audit trail (PostgreSQL triggers)
|   +-- secrets.py                # GCP Secret Manager + env fallback
|
+-- operations/                   # Deployment & Recovery
|   +-- deploy.sh                 # Zero-downtime deploy with auto-rollback
|   +-- rollback.sh               # Safe rollback with backup tagging
|   +-- backup.sh                 # PostgreSQL dump + GCS upload + retention
|
+-- docs/                         # Operational Documentation
    +-- ADRs.md                   # 5 Architecture Decision Records
    +-- RUNBOOK.md                # 8-section incident response playbook
```

---

## Engineering Highlights

### CI/CD Pipeline (5-Job CI + Automated CD)

The CI pipeline runs **5 parallel jobs**: ruff linting, pytest with coverage against PostgreSQL 16, bandit + pip-audit security scanning, CycloneDX SBOM generation, and build validation. The CD pipeline deploys to GCP via SSH, runs the full test suite on the VM, and auto-rolls-back on failure.

```
Push to main
  |
  +-> Lint (ruff) ----+
  +-> Test (523) -----+-> Build Validation -> CD Deploy -> Smoke Test
  +-> Security -------+
  +-> SBOM -----------+
```

### SLO Monitoring with Error Budgets

The health monitor tracks 3 SLIs in a rolling 10,000-request window:

| SLI | Target | Tracking |
|-----|--------|----------|
| Availability | >= 99.5% | % of non-5xx responses |
| p95 Latency | < 500ms | Response time percentiles |
| Error Rate | < 1% | 5xx response percentage |

Error budgets are calculated per SLO with burn-rate alerting (ok/warning/critical). Job queue metrics track processing throughput with p95 latency.

### RBAC + Immutable Audit Trail

4-role hierarchy with wildcard permission matching (`jobs:*`, `*:read`). Every RBAC denial, auth event, API mutation, and job state transition is written to an append-only PostgreSQL table protected by database triggers that prevent UPDATE and DELETE.

### Infrastructure as Code

Full Terraform configuration for the GCP stack: compute instance with startup provisioning, 6 firewall rules (HTTP, HTTPS, SSH, app, deny-RDP, internal-only PostgreSQL), and 3 GCS buckets with versioning and lifecycle policies.

### Operational Maturity

- **Deploy**: Pull, install, test, restart, smoke test — auto-rollback on test failure
- **Rollback**: Creates backup tag, restores target, re-tests, reverts if tests fail
- **Backup**: Daily PostgreSQL dump + config archive, GCS upload, 30-day local retention
- **Monitoring**: 5-minute cron checks disk, memory, services, SSL expiry, database health
- **Alerting**: 4 severity levels, email for WARNING+, JSONL audit log, alert history API

---

## How I Operate This Platform

This isn't a "deploy and forget" system. I actively operate it:

- **523 automated tests** gate every deployment (security, integration, data quality, contracts)
- **SLO dashboard** tracks availability, latency, and error budgets in real-time
- **Immutable audit trail** logs every auth event, API mutation, and job transition
- **Automated backups** run daily with GCS upload and 30-day retention
- **Dependency scanning** via Dependabot (pip + GitHub Actions, weekly)
- **SBOM generation** on every CI run (CycloneDX format)
- **Performance baselines** documented with alerting thresholds

---

## For Hiring Managers

If you're evaluating my engineering capabilities, this directory demonstrates:

| Capability | Evidence |
|------------|----------|
| **Platform Engineering** | Full-stack production system with 11 engines and 188 KPIs |
| **DevOps/SRE** | CI/CD, IaC (Terraform), monitoring, alerting, SLOs, error budgets |
| **Security** | RBAC, immutable audit trail, secrets management, security testing |
| **Data Engineering** | 11 ETL pipelines with lineage, idempotency, contracts, DLQ |
| **Operational Excellence** | Runbooks, ADRs, deploy/rollback scripts, automated backup |
| **Testing Discipline** | 523 tests across 8 categories including security and data quality |
| **Code Quality** | ruff linting, pre-commit hooks, SBOM, dependency auditing |

---

*Built and operated by Mboya Jeffers*
*Contact: MboyaJeffers9@gmail.com | linkedin.com/in/mboya-jeffers*
