#!/usr/bin/env bash
# Production Rollback Script
# Reverts the application to a previous git commit/tag.
#
# Usage:
#   bash rollback.sh                  # Rollback to previous commit
#   bash rollback.sh <commit-or-tag>  # Rollback to specific ref
#   bash rollback.sh --list           # List recent tags/commits
#
# Safety:
#   - Creates a backup tag before rolling back
#   - Runs tests after rollback (aborts if tests fail)
#   - Restarts services only after successful rollback
#
# Author: Mboya Jeffers

set -euo pipefail

DEPLOY_DIR="/opt/app"
LOG_FILE="/var/log/app-rollback.log"
VENV="${DEPLOY_DIR}/venv/bin/activate"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { local msg="[$(date '+%Y-%m-%d %H:%M:%S')] $*"; echo -e "$msg"; echo "$msg" >> "$LOG_FILE" 2>/dev/null || true; }
error() { log "${RED}ERROR: $*${NC}"; }
success() { log "${GREEN}$*${NC}"; }
warn() { log "${YELLOW}WARNING: $*${NC}"; }

list_releases() {
    echo "=== Recent Tags ==="
    cd "$DEPLOY_DIR"
    git tag -l --sort=-creatordate | head -10
    echo ""
    echo "=== Recent Commits ==="
    git log --oneline -10
}

rollback() {
    local target="${1:-}"
    cd "$DEPLOY_DIR"
    local current_commit=$(git rev-parse HEAD)
    local current_short=$(git rev-parse --short HEAD)

    if [ -z "$target" ]; then
        target=$(git rev-parse HEAD~1)
        log "Rolling back to previous commit: $(git rev-parse --short HEAD~1)"
    fi

    if ! git rev-parse --verify "$target" >/dev/null 2>&1; then
        error "Target ref '$target' not found. Use --list to see available refs."
        exit 1
    fi

    local target_short=$(git rev-parse --short "$target")

    log "=== Production Rollback ==="
    log "Current: ${current_short}"
    log "Target:  ${target_short}"

    # Create safety tag
    local backup_tag="pre-rollback-$(date '+%Y%m%d-%H%M%S')"
    git tag "$backup_tag" "$current_commit"
    log "Created backup tag: ${backup_tag}"

    # Checkout target
    log "Checking out target commit..."
    git checkout "$target" -- . 2>&1 || {
        error "Failed to checkout target. Aborting."
        git tag -d "$backup_tag" 2>/dev/null || true
        exit 1
    }

    # Install dependencies
    log "Installing dependencies..."
    source "$VENV"
    pip install -r requirements.txt --quiet 2>&1 || warn "pip install had warnings"

    # Run tests
    log "Running test suite..."
    if pytest tests/ -v --tb=short 2>&1; then
        success "Tests PASSED"
    else
        error "Tests FAILED. Reverting to original state..."
        git checkout "$current_commit" -- . 2>&1
        pip install -r requirements.txt --quiet 2>&1
        error "Rollback ABORTED. System restored to ${current_short}."
        exit 1
    fi

    # Restart services
    log "Restarting services..."
    local services=(app-web app-orchestrator app-intake)
    for svc in "${services[@]}"; do
        if systemctl is-active --quiet "$svc" 2>/dev/null; then
            sudo systemctl restart "$svc"
            log "  Restarted: $svc"
        else
            warn "  Skipped (not active): $svc"
        fi
    done

    # Smoke test
    log "Running smoke test..."
    sleep 5
    local http_code=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/api/engines 2>/dev/null || echo "000")

    if [ "$http_code" = "200" ]; then
        success "Smoke test PASSED (HTTP $http_code)"
    else
        warn "Smoke test returned HTTP $http_code"
    fi

    success "=== Rollback Complete ==="
    log "Rolled back: ${current_short} -> ${target_short}"
    log "To undo: bash rollback.sh ${backup_tag}"
}

case "${1:-}" in
    --list|-l) list_releases ;;
    --help|-h) echo "Usage: bash rollback.sh [target-commit-or-tag]" ;;
    *) rollback "${1:-}" ;;
esac
