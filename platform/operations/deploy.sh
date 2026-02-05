#!/bin/bash
# Production Deployment Script
# Pulls latest code, installs deps, runs tests, restarts services
# Usage: ./deploy.sh [--skip-tests] [--dry-run]
#
# Author: Mboya Jeffers

set -euo pipefail

INSTALL_DIR="/opt/app"
LOG_FILE="/opt/app/logs/deploy.log"
SKIP_TESTS=false
DRY_RUN=false

# Parse flags
for arg in "$@"; do
    case $arg in
        --skip-tests) SKIP_TESTS=true ;;
        --dry-run) DRY_RUN=true ;;
        *) echo "Unknown flag: $arg"; exit 1 ;;
    esac
done

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

fail() {
    log "DEPLOY FAILED: $1"
    exit 1
}

mkdir -p "$(dirname "$LOG_FILE")"
log "=== Starting deployment ==="
log "Options: skip-tests=$SKIP_TESTS, dry-run=$DRY_RUN"

cd "$INSTALL_DIR"

# 1. Pre-deploy checks
log "Running pre-deploy checks..."
if ! systemctl is-active --quiet app-web; then
    log "WARNING: app-web not running before deploy"
fi

# Save current commit for rollback
PREV_COMMIT=$(git rev-parse HEAD 2>/dev/null || echo "unknown")
log "Current commit: $PREV_COMMIT"

if $DRY_RUN; then
    log "DRY RUN: Would pull, install deps, test, restart services"
    git fetch origin
    AHEAD=$(git rev-list HEAD..origin/main --count 2>/dev/null || echo "?")
    log "Commits behind origin/main: $AHEAD"
    exit 0
fi

# 2. Pull latest code
log "Pulling latest code..."
git pull origin main || fail "git pull failed"
NEW_COMMIT=$(git rev-parse HEAD)
log "Updated to commit: $NEW_COMMIT"

# 3. Install/update dependencies
log "Installing dependencies..."
source venv/bin/activate
pip install -r requirements.txt --quiet || fail "pip install failed"

# 4. Run tests (unless skipped)
if ! $SKIP_TESTS; then
    log "Running test suite..."
    if pytest tests/ -v --tb=short 2>&1 | tee -a "$LOG_FILE"; then
        log "All tests passed"
    else
        log "TESTS FAILED - rolling back to $PREV_COMMIT"
        git checkout "$PREV_COMMIT"
        fail "Tests failed, rolled back to $PREV_COMMIT"
    fi
else
    log "Tests skipped (--skip-tests flag)"
fi

# 5. Restart services
log "Restarting services..."
sudo systemctl restart app-web
sudo systemctl restart app-orchestrator
sudo systemctl restart app-intake

# Wait for services
sleep 3

# 6. Post-deploy smoke test
log "Running smoke tests..."
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/api/engines)
if [ "$HTTP_CODE" == "200" ]; then
    log "Smoke test passed: /api/engines returned $HTTP_CODE"
else
    log "WARNING: Smoke test returned $HTTP_CODE (expected 200)"
fi

# 7. Summary
SERVICES_ACTIVE=$(systemctl list-units --type=service --state=active | grep -c "app-" || true)
log "=== Deploy complete ==="
log "Commit: $NEW_COMMIT"
log "Active services: $SERVICES_ACTIVE"
log "Deploy finished at $(date)"
