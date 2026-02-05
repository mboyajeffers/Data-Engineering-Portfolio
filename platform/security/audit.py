"""
Immutable Audit Logger
Append-only audit trail for security events, API mutations, and job transitions.
PostgreSQL backend with UPDATE/DELETE triggers preventing modification.

Author: Mboya Jeffers
"""

import json
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': os.environ.get('DB_PORT', '5432'),
    'database': os.environ.get('DB_NAME', 'analytics_platform'),
    'user': os.environ.get('DB_USER', 'app_user'),
    'password': os.environ.get('DB_PASSWORD', ''),
}


def log_event(event_type, action, actor=None, actor_role=None,
              resource=None, resource_id=None, details=None,
              ip_address=None, user_agent=None, status='success'):
    """
    Append an immutable audit log entry.

    Args:
        event_type: 'auth', 'api', 'job', 'system', 'etl'
        action: What happened ('login', 'create_job', 'run_engine', etc.)
        actor: Who did it (username or 'system')
        actor_role: Role at time of action
        resource: What was acted on ('job', 'engine', 'client', etc.)
        resource_id: ID of the resource
        details: Dict with event-specific data
        ip_address: Source IP address
        user_agent: Client user agent string
        status: 'success', 'failure', 'denied'
    """
    try:
        import psycopg2
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute('''
            INSERT INTO audit_log_immutable
            (event_type, action, actor, actor_role, resource, resource_id,
             details, ip_address, user_agent, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            event_type, action, actor or 'system', actor_role,
            resource, resource_id,
            json.dumps(details) if details else None,
            ip_address, user_agent, status,
        ))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f'Audit log write failed: {e}')


def log_auth(action, actor, ip_address=None, user_agent=None,
             status='success', details=None):
    """Log authentication events."""
    log_event('auth', action, actor=actor, ip_address=ip_address,
              user_agent=user_agent, status=status, details=details)


def log_api(action, actor=None, actor_role=None, resource=None,
            resource_id=None, ip_address=None, status='success', details=None):
    """Log API mutation events."""
    log_event('api', action, actor=actor, actor_role=actor_role,
              resource=resource, resource_id=resource_id,
              ip_address=ip_address, status=status, details=details)


def log_job(action, job_id, actor=None, details=None, status='success'):
    """Log job state transitions."""
    log_event('job', action, actor=actor, resource='job',
              resource_id=str(job_id), status=status, details=details)


def query_audit_log(event_type=None, actor=None, since=None, limit=100):
    """Query the audit log (read-only). Returns list of dicts."""
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        query = 'SELECT * FROM audit_log_immutable WHERE 1=1'
        params = []

        if event_type:
            query += ' AND event_type = %s'
            params.append(event_type)
        if actor:
            query += ' AND actor = %s'
            params.append(actor)
        if since:
            query += ' AND timestamp >= %s'
            params.append(since)

        query += ' ORDER BY timestamp DESC LIMIT %s'
        params.append(limit)

        cur.execute(query, params)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f'Audit log query failed: {e}')
        return []
