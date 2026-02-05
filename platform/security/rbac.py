"""
Role-Based Access Control (RBAC)
4-role hierarchy with wildcard permission matching.
Roles: admin, operator, viewer, auditor.

Author: Mboya Jeffers
"""

import logging
from functools import wraps

from flask import jsonify, request, session

logger = logging.getLogger(__name__)

# Role hierarchy (higher level = more access)
ROLES = {
    'admin': {
        'level': 100,
        'description': 'Full system access',
        'permissions': ['*'],
    },
    'operator': {
        'level': 50,
        'description': 'Run engines, manage jobs, generate reports',
        'permissions': ['engines:read', 'engines:run', 'jobs:*', 'etl:run', 'reports:generate', 'clients:read'],
    },
    'auditor': {
        'level': 20,
        'description': 'Read access plus audit log viewing',
        'permissions': ['*:read', 'audit:*'],
    },
    'viewer': {
        'level': 10,
        'description': 'Read-only access to all resources',
        'permissions': ['engines:read', 'jobs:read', 'clients:read', 'etl:read', 'reports:read'],
    },
}


def get_user_role():
    """Get the current user's role from session."""
    return session.get('user_role', 'admin')


def require_role(*allowed_roles):
    """
    Decorator to enforce role-based access on endpoints.

    Usage:
        @app.route('/api/admin/something')
        @require_role('admin')
        def admin_only():
            ...
    """
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            user_role = get_user_role()

            # Admin always has access
            if user_role == 'admin':
                return f(*args, **kwargs)

            # Check if user's role is in allowed list
            if user_role not in allowed_roles:
                logger.warning(
                    f'RBAC denied: role={user_role} tried {request.method} {request.path} '
                    f'(requires: {allowed_roles})'
                )
                # Log to audit trail
                try:
                    from core.audit import log_event
                    log_event(
                        'auth', 'rbac_denied',
                        actor=session.get('username', 'unknown'),
                        actor_role=user_role,
                        resource=request.path,
                        ip_address=request.remote_addr,
                        status='denied',
                        details={'required_roles': list(allowed_roles), 'method': request.method}
                    )
                except Exception:
                    pass
                return jsonify({
                    'error': 'Insufficient permissions',
                    'required_roles': list(allowed_roles),
                    'your_role': user_role,
                }), 403

            return f(*args, **kwargs)
        return wrapper
    return decorator


def has_permission(user_role, permission):
    """Check if a role has a specific permission."""
    if user_role == 'admin':
        return True

    role_config = ROLES.get(user_role, {})
    role_perms = role_config.get('permissions', [])

    for perm in role_perms:
        if perm == '*':
            return True
        if perm == permission:
            return True
        # Wildcard matching: 'jobs:*' matches 'jobs:read', 'jobs:create', etc.
        if ':' in perm and perm.endswith(':*'):
            prefix = perm.split(':')[0]
            if permission.startswith(prefix + ':'):
                return True
        # Read wildcard: '*:read' matches 'jobs:read', 'engines:read', etc.
        if perm == '*:read' and permission.endswith(':read'):
            return True

    return False
