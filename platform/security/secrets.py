"""
Production Analytics Platform - Secrets Manager Wrapper
Reads secrets from GCP Secret Manager with .env file fallback.

Usage:
    from security.secrets import get_secret
    db_password = get_secret('DB_PASSWORD')
    api_key = get_secret('FRED_API_KEY')

Author: Mboya Jeffers
"""
import os
import logging

logger = logging.getLogger(__name__)

# Cache for loaded secrets
_cache = {}

# Whether GCP Secret Manager is available
_sm_client = None
_sm_checked = False
_project_id = None


def _init_secret_manager():
    """Lazy-initialize the Secret Manager client."""
    global _sm_client, _sm_checked, _project_id
    if _sm_checked:
        return _sm_client is not None
    _sm_checked = True
    try:
        from google.cloud import secretmanager
        _sm_client = secretmanager.SecretManagerServiceClient()
        _project_id = os.getenv('GCP_PROJECT_ID', '')
        logger.info("Secret Manager client initialized")
        return True
    except Exception as e:
        logger.debug(f"Secret Manager unavailable, using .env fallback: {e}")
        return False


def get_secret(name, default=None):
    """
    Get a secret value. Checks in order:
    1. Environment variable (already loaded from .env by dotenv)
    2. GCP Secret Manager
    3. Default value

    Args:
        name: Secret name (e.g., 'DB_PASSWORD')
        default: Default value if not found anywhere

    Returns:
        Secret value as string, or default
    """
    # Check cache first
    if name in _cache:
        return _cache[name]

    # 1. Check environment variable (highest priority - allows override)
    env_val = os.getenv(name)
    if env_val is not None:
        _cache[name] = env_val
        return env_val

    # 2. Try GCP Secret Manager
    if _init_secret_manager():
        try:
            secret_path = f"projects/{_project_id}/secrets/{name}/versions/latest"
            response = _sm_client.access_secret_version(
                request={"name": secret_path}
            )
            value = response.payload.data.decode("UTF-8")
            _cache[name] = value
            logger.debug(f"Secret '{name}' loaded from Secret Manager")
            return value
        except Exception as e:
            logger.debug(f"Secret '{name}' not in Secret Manager: {e}")

    # 3. Return default
    if default is not None:
        return default

    logger.warning(f"Secret '{name}' not found in env or Secret Manager")
    return default


def list_secrets():
    """
    List available secrets in Secret Manager.

    Returns:
        List of secret names, or empty list if unavailable
    """
    if not _init_secret_manager():
        return []
    try:
        parent = f"projects/{_project_id}"
        secrets = _sm_client.list_secrets(request={"parent": parent})
        return [s.name.split('/')[-1] for s in secrets]
    except Exception as e:
        logger.debug(f"Cannot list secrets: {e}")
        return []


def clear_cache():
    """Clear the secrets cache. Useful for testing."""
    _cache.clear()


# Known platform secrets for documentation
PLATFORM_SECRETS = [
    'APP_PASSWORD',
    'DB_PASSWORD',
    'FLASK_SECRET_KEY',
    'FRED_API_KEY',
    'NREL_API_KEY',
    'NOAA_API_KEY',
]
