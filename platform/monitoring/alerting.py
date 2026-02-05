"""
Production Analytics Platform - Alerting Module
Multi-channel alert delivery for operational events.

Channels:
- Email (SMTP) for WARNING and above
- JSONL log file for all alerts
- Standard logging for all alerts

Author: Mboya Jeffers
"""

import json
import logging
import os
import smtplib
from dataclasses import dataclass, field
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from enum import Enum
from typing import Any, Dict, List, Optional


class AlertLevel(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class Alert:
    """An alert event."""
    level: AlertLevel
    title: str
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")
    source: str = "analytics-platform"
    acknowledged: bool = False


class AlertManager:
    """
    Manages alert delivery across channels.
    Supports email (SMTP), JSONL log, and standard logging.
    """

    def __init__(self):
        self.logger = logging.getLogger("AlertManager")
        self.alert_history: List[Alert] = []

        # Email config from environment
        self.smtp_host = os.environ.get('SMTP_HOST', 'smtp.gmail.com')
        self.smtp_port = int(os.environ.get('SMTP_PORT', '587'))
        self.smtp_user = os.environ.get('SMTP_USER', '')
        self.smtp_password = os.environ.get('SMTP_PASSWORD', '')
        self.alert_recipients = os.environ.get('ALERT_RECIPIENTS', '').split(',')
        self.alert_from = os.environ.get('ALERT_FROM', 'alerts@example.com')

        # Alert log file
        self.alert_log_path = os.environ.get(
            'ALERT_LOG_PATH', '/opt/app/logs/alerts.jsonl'
        )
        os.makedirs(os.path.dirname(self.alert_log_path), exist_ok=True)

    def send_alert(self, level: AlertLevel, title: str,
                   details: Optional[Dict[str, Any]] = None,
                   message: str = "") -> Alert:
        """Send an alert through all configured channels."""
        alert = Alert(
            level=level, title=title,
            message=message or title, details=details or {},
        )
        self.alert_history.append(alert)
        self._log_alert(alert)
        self._write_alert_log(alert)

        # Send email for WARNING and above
        if level in (AlertLevel.WARNING, AlertLevel.ERROR, AlertLevel.CRITICAL):
            self._send_email(alert)
        return alert

    def get_recent_alerts(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent alerts."""
        alerts = sorted(self.alert_history, key=lambda a: a.timestamp, reverse=True)[:limit]
        return [
            {
                'level': a.level.value, 'title': a.title,
                'message': a.message, 'details': a.details,
                'timestamp': a.timestamp, 'acknowledged': a.acknowledged,
            }
            for a in alerts
        ]

    def _log_alert(self, alert: Alert):
        """Log alert to standard logger."""
        log_method = {
            AlertLevel.INFO: self.logger.info,
            AlertLevel.WARNING: self.logger.warning,
            AlertLevel.ERROR: self.logger.error,
            AlertLevel.CRITICAL: self.logger.critical,
        }.get(alert.level, self.logger.info)
        log_method(f"[ALERT:{alert.level.value.upper()}] {alert.title}")

    def _write_alert_log(self, alert: Alert):
        """Append alert to JSONL log file."""
        try:
            entry = {
                'timestamp': alert.timestamp, 'level': alert.level.value,
                'title': alert.title, 'message': alert.message,
                'details': alert.details, 'source': alert.source,
            }
            with open(self.alert_log_path, 'a') as f:
                f.write(json.dumps(entry) + '\n')
        except Exception as e:
            self.logger.error(f"Failed to write alert log: {e}")

    def _send_email(self, alert: Alert):
        """Send alert via email."""
        if not self.smtp_user or not self.smtp_password:
            self.logger.debug("Email not configured, skipping")
            return

        recipients = [r.strip() for r in self.alert_recipients if r.strip()]
        if not recipients:
            return

        try:
            msg = MIMEMultipart()
            msg['From'] = self.alert_from
            msg['To'] = ', '.join(recipients)
            msg['Subject'] = f"[{alert.level.value.upper()}] {alert.title}"

            body = f"""Platform Alert
{'=' * 40}
Level: {alert.level.value.upper()}
Time: {alert.timestamp}
Source: {alert.source}

{alert.title}
{'-' * len(alert.title)}
{alert.message}

Details:
{json.dumps(alert.details, indent=2)}
"""
            msg.attach(MIMEText(body, 'plain'))

            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)

            self.logger.info(f"Alert email sent to {recipients}")
        except Exception as e:
            self.logger.error(f"Failed to send alert email: {e}")


# Global singleton
_alert_manager = None

def get_alert_manager() -> AlertManager:
    """Get the global alert manager instance."""
    global _alert_manager
    if _alert_manager is None:
        _alert_manager = AlertManager()
    return _alert_manager
