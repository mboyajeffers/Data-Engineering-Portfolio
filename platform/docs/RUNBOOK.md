# Production Runbook

**Author:** Mboya Jeffers
**Last Updated:** 2026-02-05

---

## 1. Service Overview

| Service | Unit | Port | Health Check |
|---------|------|------|-------------|
| Web (Flask/Gunicorn) | `app-web.service` | 8080 | `GET /api/engines` |
| Orchestrator | `app-orchestrator.service` | — | journald logs |
| Intake Watcher | `app-intake.service` | — | journald logs |
| PostgreSQL | `postgresql.service` | 5432 | `pg_isready` |
| Nginx (reverse proxy) | `nginx.service` | 80/443 | `curl -s localhost` |

### Quick Status

```bash
systemctl status app-web app-orchestrator app-intake postgresql nginx
```

---

## 2. Common Operations

### Restart a service

```bash
sudo systemctl restart app-web
sudo journalctl -u app-web -f   # Watch logs
```

### Deploy latest code

```bash
cd /opt/app && ./scripts/deploy.sh
# Auto-rolls back on test failure
```

### Rollback to previous release

```bash
./scripts/rollback.sh             # Rolls back 1 commit
./scripts/rollback.sh v5.3.0      # Rolls back to specific tag
./scripts/rollback.sh --list      # Show recent tags/commits
```

### Manual backup

```bash
./scripts/backup.sh
```

---

## 3. Incident Response

### Severity Levels

| Level | Definition | Response Time | Example |
|-------|-----------|---------------|---------|
| **P1 - Critical** | Platform down, no data processing | 15 min | All services crashed, DB unreachable |
| **P2 - High** | Major feature degraded | 1 hour | ETL pipelines failing, API errors > 5% |
| **P3 - Medium** | Minor degradation | 4 hours | Single engine failing, slow queries |
| **P4 - Low** | Cosmetic or non-urgent | 24 hours | Log noise, minor UI issue |

### Incident Checklist

1. **Assess** — Check service status, error logs, monitoring dashboard
2. **Communicate** — Log incident in alert system
3. **Mitigate** — Restart service, rollback, or scale
4. **Investigate** — Root cause analysis from logs and metrics
5. **Resolve** — Deploy fix or workaround
6. **Document** — Post-incident review with timeline

---

## 4. Troubleshooting

### Web application returns 502

```bash
# Check if Gunicorn is running
systemctl status app-web

# Check Nginx upstream
sudo nginx -t
sudo journalctl -u nginx --since "10 min ago"

# Restart stack
sudo systemctl restart app-web && sudo systemctl restart nginx
```

### Database connection refused

```bash
# Check PostgreSQL status
sudo systemctl status postgresql
pg_isready -h localhost -p 5432

# Check connections
sudo -u postgres psql -c "SELECT count(*) FROM pg_stat_activity;"

# Restart if needed
sudo systemctl restart postgresql
```

### Disk space full

```bash
# Check disk usage
df -h /
du -sh /opt/app/logs/* | sort -rh | head -10

# Clean old logs (keep 7 days)
find /opt/app/logs -name "*.log" -mtime +7 -delete

# Clean old backups (keep 30 days)
find /opt/app/backups -name "*.gz" -mtime +30 -delete

# Verify
df -h /
```

### ETL pipeline stuck

```bash
# Check orchestrator logs
sudo journalctl -u app-orchestrator --since "30 min ago" | tail -50

# Check for stuck jobs in database
sudo -u postgres psql -d analytics -c \
  "SELECT id, status, created_at FROM jobs WHERE status = 'processing' AND created_at < NOW() - INTERVAL '1 hour';"

# Reset stuck jobs
sudo -u postgres psql -d analytics -c \
  "UPDATE jobs SET status = 'failed' WHERE status = 'processing' AND created_at < NOW() - INTERVAL '2 hours';"

# Restart orchestrator
sudo systemctl restart app-orchestrator
```

### High memory usage

```bash
# Check per-process memory
ps aux --sort=-%mem | head -10

# Check Gunicorn workers
ps aux | grep gunicorn

# If Gunicorn is consuming too much, graceful restart
sudo systemctl restart app-web
```

---

## 5. Monitoring & Alerts

### SLO Targets

| SLI | Target | Alert Threshold |
|-----|--------|----------------|
| Availability | >= 99.5% | < 99.0% triggers WARNING, < 98.0% triggers CRITICAL |
| p95 Latency | < 500ms | > 800ms triggers WARNING, > 1500ms triggers CRITICAL |
| Error Rate | < 1% | > 2% triggers WARNING, > 5% triggers CRITICAL |

### Check current SLO status

```bash
curl -s http://localhost:8080/api/monitoring/slo | python3 -m json.tool
```

### Alert channels

- **Email**: WARNING and above (SMTP configured in .env)
- **JSONL log**: All alerts written to `/opt/app/logs/alerts.jsonl`
- **Monitoring API**: `GET /api/monitoring/alerts`

### Uptime check

```bash
# External health check (runs every 5 minutes via cron)
curl -sf http://localhost:8080/api/engines > /dev/null && echo "UP" || echo "DOWN"
```

---

## 6. Backup & Recovery

### Automated backup (daily at 2 AM)

- PostgreSQL dump (pg_dump + gzip)
- Config files (.env, systemd units, nginx config, requirements.txt)
- Uploaded to GCS bucket
- 30-day local retention with automatic cleanup

### Verify backup

```bash
# List recent backups
ls -lh /opt/app/backups/ | tail -5

# Verify latest dump integrity
gunzip -t /opt/app/backups/db_latest.sql.gz && echo "OK" || echo "CORRUPT"
```

### Restore from backup

```bash
# Stop services
sudo systemctl stop app-web app-orchestrator app-intake

# Restore database
gunzip -c /opt/app/backups/db_YYYY-MM-DD.sql.gz | sudo -u postgres psql -d analytics

# Restart services
sudo systemctl start app-web app-orchestrator app-intake

# Verify
curl -s http://localhost:8080/api/engines
```

---

## 7. Security Procedures

### Credential rotation

```bash
# 1. Update .env file with new credentials
sudo nano /opt/app/.env

# 2. Restart all services to pick up new env
sudo systemctl restart app-web app-orchestrator app-intake

# 3. Verify services came back healthy
systemctl status app-web app-orchestrator app-intake
```

### Check for RBAC denials

```bash
curl -s http://localhost:8080/api/monitoring/audit?action=rbac_denied | python3 -m json.tool
```

### Review active sessions

```bash
# Check PostgreSQL connections
sudo -u postgres psql -c "SELECT usename, client_addr, state FROM pg_stat_activity WHERE datname = 'analytics';"
```

---

## 8. Maintenance Windows

### Planned maintenance procedure

1. **Announce** — Log maintenance window start
2. **Backup** — Run manual backup: `./scripts/backup.sh`
3. **Execute** — Perform maintenance (updates, migrations, etc.)
4. **Test** — Run full test suite: `pytest tests/ -v`
5. **Verify** — Smoke test all endpoints
6. **Close** — Log maintenance window end

### System updates

```bash
# Update OS packages
sudo apt update && sudo apt upgrade -y

# Update Python dependencies
source /opt/app/venv/bin/activate
pip install -r requirements.txt --upgrade

# Run tests before restarting
pytest tests/ -v

# Restart services
sudo systemctl restart app-web app-orchestrator app-intake
```

---

*Runbook maintained by Mboya Jeffers*
