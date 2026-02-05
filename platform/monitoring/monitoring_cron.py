#!/usr/bin/env python3
"""
Production Analytics Platform - Monitoring Cron Script
Runs periodic health checks and sends alerts when thresholds are exceeded.

Usage:
    # Run manually
    python /opt/app/scripts/monitoring_cron.py

    # Add to crontab (every 5 minutes)
    */5 * * * * /opt/app/venv/bin/python /opt/app/scripts/monitoring_cron.py

Author: Mboya Jeffers
"""

import sys
import os
import shutil
import subprocess
import json
from datetime import datetime

sys.path.insert(0, '/opt/app')

from monitoring.alerting import get_alert_manager, AlertLevel

import logging
logger = logging.getLogger('monitoring-cron')

# Thresholds
DISK_WARN_PCT = 80
DISK_CRIT_PCT = 90
MEMORY_WARN_PCT = 85
MEMORY_CRIT_PCT = 95
REQUIRED_SERVICES = [
    'app-web',
    'app-orchestrator',
    'app-intake',
    'nginx',
]


def check_disk():
    """Check disk usage and alert if above threshold."""
    usage = shutil.disk_usage('/')
    used_pct = (usage.used / usage.total) * 100
    free_gb = usage.free / (1024 ** 3)

    if used_pct >= DISK_CRIT_PCT:
        mgr = get_alert_manager()
        mgr.send_alert(
            AlertLevel.CRITICAL,
            f'Disk usage CRITICAL: {used_pct:.1f}%',
            details={'used_pct': round(used_pct, 1), 'free_gb': round(free_gb, 1)},
            message=f'Disk usage is at {used_pct:.1f}% ({free_gb:.1f} GB free). Immediate action required.'
        )
        logger.error('disk_critical: used_pct=%.1f free_gb=%.1f', used_pct, free_gb)
    elif used_pct >= DISK_WARN_PCT:
        mgr = get_alert_manager()
        mgr.send_alert(
            AlertLevel.WARNING,
            f'Disk usage WARNING: {used_pct:.1f}%',
            details={'used_pct': round(used_pct, 1), 'free_gb': round(free_gb, 1)},
            message=f'Disk usage is at {used_pct:.1f}% ({free_gb:.1f} GB free). Consider cleanup.'
        )
        logger.warning('disk_warning: used_pct=%.1f free_gb=%.1f', used_pct, free_gb)
    else:
        logger.info('disk_ok: used_pct=%.1f free_gb=%.1f', used_pct, free_gb)


def check_memory():
    """Check memory usage (Linux only)."""
    try:
        with open('/proc/meminfo', 'r') as f:
            meminfo = {}
            for line in f:
                parts = line.split(':')
                if len(parts) == 2:
                    key = parts[0].strip()
                    val = int(parts[1].strip().split()[0])
                    meminfo[key] = val

        total = meminfo.get('MemTotal', 0)
        available = meminfo.get('MemAvailable', 0)
        if total > 0:
            used_pct = ((total - available) / total) * 100
            if used_pct >= MEMORY_CRIT_PCT:
                mgr = get_alert_manager()
                mgr.send_alert(
                    AlertLevel.CRITICAL,
                    f'Memory usage CRITICAL: {used_pct:.1f}%',
                    details={'used_pct': round(used_pct, 1), 'total_mb': round(total / 1024, 0)},
                    message=f'Memory usage is at {used_pct:.1f}%. Immediate action required.'
                )
                logger.error('memory_critical: used_pct=%.1f', used_pct)
            elif used_pct >= MEMORY_WARN_PCT:
                mgr = get_alert_manager()
                mgr.send_alert(
                    AlertLevel.WARNING,
                    f'Memory usage WARNING: {used_pct:.1f}%',
                    details={'used_pct': round(used_pct, 1)},
                    message=f'Memory usage is at {used_pct:.1f}%.'
                )
                logger.warning('memory_warning: used_pct=%.1f', used_pct)
            else:
                logger.info('memory_ok: used_pct=%.1f', used_pct)
    except FileNotFoundError:
        logger.info('memory_check_skipped: reason=not_linux')


def check_services():
    """Check required systemd services are running."""
    down_services = []
    for svc in REQUIRED_SERVICES:
        try:
            result = subprocess.run(
                ['systemctl', 'is-active', svc],
                capture_output=True, text=True, timeout=5
            )
            if result.stdout.strip() != 'active':
                down_services.append(svc)
        except (subprocess.TimeoutExpired, FileNotFoundError):
            down_services.append(svc)

    if down_services:
        mgr = get_alert_manager()
        mgr.send_alert(
            AlertLevel.ERROR,
            f'{len(down_services)} service(s) down',
            details={'down_services': down_services},
            message=f'Services not running: {", ".join(down_services)}'
        )
        logger.error('services_down: %s', down_services)
    else:
        logger.info('services_ok: count=%d', len(REQUIRED_SERVICES))


def check_ssl():
    """Check SSL certificate expiry."""
    try:
        hostname = os.environ.get('SSL_HOSTNAME', 'localhost')
        result = subprocess.run(
            ['openssl', 's_client', '-connect', f'{hostname}:443', '-servername', hostname],
            input='',
            capture_output=True, text=True, timeout=10
        )
        # Extract expiry from certificate
        cert_result = subprocess.run(
            ['openssl', 'x509', '-noout', '-enddate'],
            input=result.stdout,
            capture_output=True, text=True, timeout=5
        )
        if cert_result.returncode == 0:
            # Parse: notAfter=Apr 12 00:00:00 2026 GMT
            expiry_str = cert_result.stdout.strip().replace('notAfter=', '')
            expiry = datetime.strptime(expiry_str, '%b %d %H:%M:%S %Y %Z')
            days_left = (expiry - datetime.utcnow()).days

            if days_left <= 7:
                mgr = get_alert_manager()
                mgr.send_alert(
                    AlertLevel.CRITICAL,
                    f'SSL certificate expires in {days_left} days',
                    details={'days_left': days_left, 'expiry': expiry_str},
                    message=f'SSL certificate expires {expiry_str}. Renew immediately.'
                )
                logger.error('ssl_critical: days_left=%d', days_left)
            elif days_left <= 30:
                mgr = get_alert_manager()
                mgr.send_alert(
                    AlertLevel.WARNING,
                    f'SSL certificate expires in {days_left} days',
                    details={'days_left': days_left},
                    message=f'SSL certificate expires {expiry_str}. Renewal recommended.'
                )
                logger.warning('ssl_warning: days_left=%d', days_left)
            else:
                logger.info('ssl_ok: days_left=%d', days_left)
    except Exception as e:
        logger.warning('ssl_check_failed: error=%s', str(e))


def check_database():
    """Check PostgreSQL connectivity."""
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=os.environ.get('DB_HOST', 'localhost'),
            port=os.environ.get('DB_PORT', '5432'),
            database=os.environ.get('DB_NAME', 'analytics_platform'),
            user=os.environ.get('DB_USER', 'app_user'),
            password=os.environ.get('DB_PASSWORD', ''),
        )
        cur = conn.cursor()
        cur.execute('SELECT 1')
        cur.close()
        conn.close()
        logger.info('database_ok')
    except Exception as e:
        mgr = get_alert_manager()
        mgr.send_alert(
            AlertLevel.CRITICAL,
            'Database connection failed',
            details={'error': str(e)},
            message=f'Cannot connect to PostgreSQL: {e}'
        )
        logger.error('database_failed: error=%s', str(e))


def check_stuck_jobs(max_minutes=30):
    """Alert on jobs stuck in PROCESSING state for too long."""
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=os.environ.get('DB_HOST', 'localhost'),
            port=os.environ.get('DB_PORT', '5432'),
            database=os.environ.get('DB_NAME', 'analytics_platform'),
            user=os.environ.get('DB_USER', 'app_user'),
            password=os.environ.get('DB_PASSWORD', ''),
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT id, status, updated_at
            FROM jobs
            WHERE status IN ('processing', 'validating')
              AND updated_at < NOW() - INTERVAL '%s minutes'
        """, (max_minutes,))
        stuck = cur.fetchall()
        cur.close()
        conn.close()

        if stuck:
            mgr = get_alert_manager()
            mgr.send_alert(
                AlertLevel.WARNING,
                f'{len(stuck)} stuck job(s) detected',
                details={'stuck_jobs': [{'id': j[0], 'status': j[1], 'updated_at': str(j[2])} for j in stuck]},
                message=f'{len(stuck)} job(s) stuck in processing for >{max_minutes}min'
            )
            logger.warning('stuck_jobs_detected: count=%d', len(stuck))
        else:
            logger.info('no_stuck_jobs')
    except Exception as e:
        logger.debug('stuck_jobs_check_skipped: error=%s', str(e))


def main():
    """Run all monitoring checks."""
    logger.info('monitoring_cron_start')
    check_disk()
    check_memory()
    check_services()
    check_ssl()
    check_database()
    check_stuck_jobs()
    logger.info('monitoring_cron_complete')


if __name__ == '__main__':
    main()
