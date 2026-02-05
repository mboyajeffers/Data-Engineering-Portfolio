# Architecture Decision Records

**Author:** Mboya Jeffers

---

# ADR-001: SQLite to PostgreSQL Migration

**Status:** Accepted
**Date:** 2026-02-05

## Context

The platform originally used SQLite as its primary data store. As the system grew to 11 industry engines, 188+ KPIs, and 6 concurrent systemd services, SQLite's single-writer limitation became a bottleneck. The orchestrator, intake watcher, and web API all needed concurrent write access. Additionally, the platform required an immutable audit trail with database-level triggers and RBAC enforced at the database layer -- capabilities SQLite does not natively support.

## Decision

Migrate to PostgreSQL 16, installed locally on the same GCP VM. The database schema was migrated in full, including new tables (`audit_log_immutable`, `roles`, `user_roles`) that leverage PostgreSQL-specific features such as triggers and row-level security.

## Consequences

**Positive:**
- Concurrent writes from all 6 services without locking conflicts
- Native trigger support for the immutable audit trail (INSERT-only trigger, no UPDATE/DELETE allowed)
- RBAC enforcement via PostgreSQL roles, mapping to the 4-role hierarchy (admin, operator, viewer, auditor)
- JSONB support for flexible metadata storage
- Production-grade backup and recovery tooling (pg_dump, WAL archiving)

**Negative:**
- Additional operational overhead: PostgreSQL requires memory tuning on a constrained VM (3.8 GB RAM)
- Backup scripts needed rewriting from simple file copies to pg_dump routines

## Alternatives Considered

| Alternative | Reason Rejected |
|-------------|-----------------|
| **MySQL 8** | Less feature-rich for audit triggers and row-level security |
| **Cloud SQL (managed)** | $50+/month unnecessary at single-VM scale; adds network latency |
| **Keep SQLite** | Concurrent write failures made this untenable for production |

---

# ADR-002: VM Deployment vs Containerization

**Status:** Accepted
**Date:** 2026-02-05

## Context

The system runs on a single GCP e2-small VM (2 vCPUs, 3.8 GB RAM). It comprises 6 systemd services, Flask/Gunicorn, PostgreSQL, and Nginx with SSL. The question was whether to containerize with Docker/Kubernetes or continue with direct VM deployment.

## Decision

Stay with direct VM deployment using systemd service units. Each component runs as a systemd service with automatic restart, logging via journald, and environment variables loaded from a centralized EnvironmentFile.

## Consequences

**Positive:**
- Extremely low cost: ~$7/month for the VM (vs $70+/month for GKE)
- Simple operations: `systemctl status/restart/logs` for all services
- No container runtime overhead on a constrained 3.8 GB VM
- Fast debugging cycle: SSH, edit, restart, verify -- no image rebuild

**Negative:**
- Manual horizontal scaling: cannot spin up replicas without a new VM
- No container image portability
- No built-in blue-green or canary deployment

## Alternatives Considered

| Alternative | Reason Rejected |
|-------------|-----------------|
| **Docker Compose** | Docker daemon would consume 200-500 MB of limited RAM; no practical benefit for single-node |
| **GKE** | Overkill at current scale; $70+/month minimum; massive operational complexity |
| **Cloud Run** | Cold start latency unacceptable for always-on services |

---

# ADR-003: Monolith Architecture

**Status:** Accepted
**Date:** 2026-02-05

## Context

The platform encompasses 11 industry engines, a web UI with 15 pages, 31+ API blueprints, ETL pipelines, PDF report generation, cloud storage integration, a job orchestrator, and a file intake watcher. The total codebase spans 9,000+ Python files. The question was whether to decompose into microservices.

## Decision

Maintain a monolith architecture with clear internal module boundaries. While the codebase is a single deployable monolith, it runs as 5 distinct systemd units plus Nginx -- a "modular monolith" approach with process-level isolation.

```
/opt/app/
+-- engines/
|   +-- core/          # Shared utilities (25+ modules)
|   +-- industry/      # 11 engine modules
+-- src/
|   +-- api/           # 31+ API blueprints
|   +-- templates/     # 15 Jinja2 pages
+-- services/          # Orchestrator, watchers, sync
+-- integrations/      # Cloud storage, external APIs
+-- etl/               # Extractors, transformers, registry
```

## Consequences

**Positive:**
- Single deployment: `git pull && systemctl restart app-web`
- Shared utilities across all 11 engines without versioning headaches
- 523 tests run against the entire system in one pytest invocation
- Simple dependency management: one requirements.txt, one virtual environment

**Negative:**
- All-or-nothing deploys (mitigated by Gunicorn graceful restart)
- Cannot independently scale workloads
- Large codebase can be slow to navigate

## Alternatives Considered

| Alternative | Reason Rejected |
|-------------|-----------------|
| **Microservices** | Premature at team size of 1; multiplies operational complexity |
| **Separate processes per engine** | Current 5-unit approach provides process isolation where needed |

---

# ADR-004: EnvironmentFile for Secrets Management

**Status:** Accepted
**Date:** 2026-02-05

## Context

The platform had secrets hardcoded in multiple locations: API keys in application code, passwords in systemd unit files, and credentials scattered across config files. A security audit identified 25 occurrences of secrets across 6 files.

## Decision

Two-phase approach:

**Phase 1 (Implemented):** Migrate all secrets to a single `.env` file, loaded via systemd `EnvironmentFile` directive and `python-dotenv`. All 25 hardcoded occurrences eliminated.

**Phase 2 (Planned):** Migrate to GCP Secret Manager for versioned, auditable secret access with IAM-based authorization.

## Consequences

**Positive:**
- Secrets fully removed from source code and Git history
- Single source of truth: one `.env` file for all services
- Easy credential rotation: update file, restart services
- Boot documentation now safe to share without redaction

**Negative:**
- Secrets stored as plaintext on disk (mitigated: GCE disk encryption at rest)
- No automated rotation
- File permissions must be carefully managed (`chmod 600`)

## Alternatives Considered

| Alternative | Reason Rejected |
|-------------|-----------------|
| **HashiCorp Vault** | Overkill for single-VM; requires its own infrastructure |
| **Encrypted env files (sops)** | Key management creates chicken-and-egg problem |
| **GCP Secret Manager** | Planned as Phase 2 once EnvironmentFile foundation is stable |

---

# ADR-005: RBAC Role Hierarchy

**Status:** Accepted
**Date:** 2026-02-05

## Context

The platform originally used single-password authentication. This was insufficient for enterprise readiness: no way to grant read-only access to stakeholders, restrict destructive operations, or maintain an audit trail of who performed which actions.

## Decision

Implement a 4-role hierarchy:

| Role | Level | Permissions |
|------|-------|-------------|
| **admin** | Full access | All operations including user and system management |
| **operator** | Operational | Job management, engine execution, report generation |
| **viewer** | Read-only | Dashboard viewing, report access, status monitoring |
| **auditor** | Cross-cutting read | Audit logs, compliance reports, system metrics |

Implementation:
- PostgreSQL tables: `roles` and `user_roles` (many-to-many)
- Python decorator: `@require_role('admin')` applied to Flask routes
- Immutable audit trail: all role changes logged (INSERT-only, trigger-protected)
- 33 security tests validate enforcement

## Consequences

**Positive:**
- Least-privilege access for stakeholders and clients
- Audit compliance via dedicated auditor role
- Scalable foundation extensible with additional roles
- Database-enforced with foreign key constraints

**Negative:**
- Added complexity for what is currently a single-user system
- Role management requires API calls (no self-service UI yet)

## Alternatives Considered

| Alternative | Reason Rejected |
|-------------|-----------------|
| **Simple admin/user split** | Insufficient granularity for enterprise scenarios |
| **OAuth 2.0 / OIDC** | Planned as future enhancement; significant implementation complexity |
| **ABAC** | Over-engineered for 4 roles and 31 endpoints |

---

*Architecture Decision Records maintained by Mboya Jeffers*
