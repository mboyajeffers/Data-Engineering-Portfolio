#!/bin/bash
# Automated Backup Script
# PostgreSQL dump + config archive, GCS upload, 30-day retention
# Run via cron: 0 2 * * *
#
# Author: Mboya Jeffers

set -euo pipefail

BACKUP_DIR="/opt/app/backups"
GCS_BUCKET="${GCS_BACKUP_BUCKET:-gs://app-backups}"
DB_NAME="${DB_NAME:-analytics_platform}"
DB_USER="${DB_USER:-app_user}"
RETENTION_DAYS=30
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="/opt/app/logs/backup.log"

export PGPASSWORD="${DB_PASSWORD}"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

mkdir -p "$BACKUP_DIR"
mkdir -p "$(dirname "$LOG_FILE")"

log "=== Starting backup ==="

# 1. PostgreSQL database dump
DB_BACKUP="$BACKUP_DIR/db_${TIMESTAMP}.sql.gz"
log "Backing up PostgreSQL database..."
if pg_dump -h localhost -U "$DB_USER" "$DB_NAME" | gzip > "$DB_BACKUP"; then
    DB_SIZE=$(du -h "$DB_BACKUP" | cut -f1)
    log "Database backup complete: $DB_BACKUP ($DB_SIZE)"
else
    log "ERROR: Database backup failed"
    exit 1
fi

# 2. Config files backup
CONFIG_BACKUP="$BACKUP_DIR/config_${TIMESTAMP}.tar.gz"
log "Backing up config files..."
tar -czf "$CONFIG_BACKUP" \
    /opt/app/.env \
    /opt/app/requirements.txt \
    /etc/systemd/system/app-*.service \
    /etc/nginx/sites-available/ \
    2>/dev/null || true
CONFIG_SIZE=$(du -h "$CONFIG_BACKUP" | cut -f1)
log "Config backup complete: $CONFIG_BACKUP ($CONFIG_SIZE)"

# 3. Upload to GCS
if command -v gsutil &> /dev/null; then
    log "Uploading to GCS..."
    gsutil cp "$DB_BACKUP" "${GCS_BUCKET}/db/" 2>/dev/null && log "DB backup uploaded" || log "WARNING: GCS upload failed"
    gsutil cp "$CONFIG_BACKUP" "${GCS_BUCKET}/config/" 2>/dev/null && log "Config backup uploaded" || log "WARNING: GCS upload failed"
else
    log "WARNING: gsutil not available, skipping GCS upload"
fi

# 4. Cleanup old local backups
log "Cleaning up backups older than ${RETENTION_DAYS} days..."
DELETED=$(find "$BACKUP_DIR" -name "*.gz" -mtime +${RETENTION_DAYS} -type f -delete -print | wc -l)
log "Deleted $DELETED old backup files"

# 5. Summary
TOTAL_BACKUPS=$(ls -1 "$BACKUP_DIR"/*.gz 2>/dev/null | wc -l)
TOTAL_SIZE=$(du -sh "$BACKUP_DIR" 2>/dev/null | cut -f1)
log "=== Backup complete ==="
log "Total backups: $TOTAL_BACKUPS files ($TOTAL_SIZE)"
log "Retention: ${RETENTION_DAYS} days local"
