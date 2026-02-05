"""
Production Analytics Platform - Health Monitor & SLO Tracking
Monitors system health metrics and tracks SLO compliance.

SLIs Tracked:
- Availability: % of successful (non-5xx) responses
- Latency: p50, p95, p99 response times
- Error Rate: % of 5xx responses

SLOs (targets):
- Availability >= 99.5%
- p95 Latency < 500ms
- Error Rate < 1%

Author: Mboya Jeffers
"""

import os
import shutil
import subprocess
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class SLOTarget:
    """SLO target definition."""
    name: str
    metric: str
    target: float
    unit: str
    comparison: str  # 'gte' (>=) or 'lte' (<=)


# Define SLO targets
SLO_TARGETS = [
    SLOTarget("Availability", "availability_pct", 99.5, "%", "gte"),
    SLOTarget("p95 Latency", "p95_latency_ms", 500.0, "ms", "lte"),
    SLOTarget("Error Rate", "error_rate_pct", 1.0, "%", "lte"),
]


class HealthMonitor:
    """
    Tracks system health metrics and SLO compliance.
    Maintains a rolling window of request data for real-time SLI calculation.
    """

    def __init__(self, window_size: int = 10000):
        self.window_size = window_size
        self._requests: deque = deque(maxlen=window_size)
        self._start_time = datetime.utcnow()
        self._job_metrics: deque = deque(maxlen=1000)
        self._error_window: deque = deque(maxlen=100)

    def record_request(self, status_code: int, duration_ms: float):
        """Record a completed request for SLI tracking."""
        self._requests.append({
            'timestamp': time.time(),
            'status': status_code,
            'duration_ms': duration_ms,
        })

    def get_sli_metrics(self) -> Dict[str, float]:
        """Calculate current SLI metrics from the rolling window."""
        if not self._requests:
            return {
                'availability_pct': 100.0,
                'error_rate_pct': 0.0,
                'p50_latency_ms': 0.0,
                'p95_latency_ms': 0.0,
                'p99_latency_ms': 0.0,
                'total_requests': 0,
            }

        total = len(self._requests)
        errors = sum(1 for r in self._requests if r['status'] >= 500)
        durations = sorted(r['duration_ms'] for r in self._requests)

        return {
            'availability_pct': round((1 - errors / total) * 100, 3),
            'error_rate_pct': round((errors / total) * 100, 3),
            'p50_latency_ms': round(self._percentile(durations, 50), 2),
            'p95_latency_ms': round(self._percentile(durations, 95), 2),
            'p99_latency_ms': round(self._percentile(durations, 99), 2),
            'total_requests': total,
        }

    def get_slo_status(self) -> Dict[str, Any]:
        """Check SLO compliance against targets."""
        metrics = self.get_sli_metrics()
        slo_results = []

        for slo in SLO_TARGETS:
            actual = metrics.get(slo.metric, 0)
            if slo.comparison == 'gte':
                met = actual >= slo.target
            else:
                met = actual <= slo.target

            slo_results.append({
                'name': slo.name,
                'target': f"{slo.comparison.replace('gte', '>=')} {slo.target}{slo.unit}",
                'actual': f"{actual}{slo.unit}",
                'met': met,
            })

        all_met = all(s['met'] for s in slo_results)
        return {
            'status': 'healthy' if all_met else 'degraded',
            'slos': slo_results,
            'metrics': metrics,
            'window_size': self.window_size,
            'uptime_seconds': (datetime.utcnow() - self._start_time).total_seconds(),
        }

    def get_error_budget(self) -> Dict[str, Any]:
        """
        Calculate error budget status.
        Error budget = 100% - SLO target.
        """
        metrics = self.get_sli_metrics()
        budgets = []

        for slo in SLO_TARGETS:
            actual = metrics.get(slo.metric, 0)

            if slo.comparison == 'gte':
                budget_total = 100.0 - slo.target
                budget_used = max(0, slo.target - actual)
                budget_remaining = max(0, budget_total - budget_used)
                burn_rate = (budget_used / budget_total * 100) if budget_total > 0 else 0
            else:
                budget_total = slo.target
                budget_used = max(0, actual)
                budget_remaining = max(0, budget_total - budget_used)
                burn_rate = (budget_used / budget_total * 100) if budget_total > 0 else 0

            budgets.append({
                'slo': slo.name,
                'budget_total_pct': round(budget_total, 3),
                'budget_used_pct': round(budget_used, 3),
                'budget_remaining_pct': round(budget_remaining, 3),
                'burn_rate_pct': round(burn_rate, 1),
                'status': 'ok' if burn_rate < 50 else ('warning' if burn_rate < 80 else 'critical'),
            })

        overall_status = 'ok'
        for b in budgets:
            if b['status'] == 'critical':
                overall_status = 'critical'
                break
            elif b['status'] == 'warning' and overall_status != 'critical':
                overall_status = 'warning'

        return {
            'budgets': budgets,
            'overall_status': overall_status,
            'total_requests': metrics['total_requests'],
            'window_size': self.window_size,
        }

    def record_job_event(self, job_id: str, event: str, engine: str = '', duration_ms: float = 0):
        """Record a job processing event for queue metrics."""
        self._job_metrics.append({
            'timestamp': time.time(),
            'job_id': job_id,
            'event': event,
            'engine': engine,
            'duration_ms': duration_ms,
        })

    def get_job_queue_metrics(self) -> Dict[str, Any]:
        """Calculate job queue metrics from recent events."""
        if not self._job_metrics:
            return {
                'queue_depth': 0, 'jobs_processing': 0,
                'jobs_completed_1h': 0, 'jobs_failed_1h': 0,
                'avg_processing_ms': 0, 'p95_processing_ms': 0,
            }

        now = time.time()
        one_hour_ago = now - 3600
        active_jobs = {}
        completed_1h = 0
        failed_1h = 0
        processing_times = []

        for event in self._job_metrics:
            jid = event['job_id']
            if event['event'] == 'queued':
                active_jobs[jid] = 'queued'
            elif event['event'] == 'started':
                active_jobs[jid] = 'processing'
            elif event['event'] == 'completed':
                active_jobs.pop(jid, None)
                if event['timestamp'] >= one_hour_ago:
                    completed_1h += 1
                    if event['duration_ms'] > 0:
                        processing_times.append(event['duration_ms'])
            elif event['event'] == 'failed':
                active_jobs.pop(jid, None)
                if event['timestamp'] >= one_hour_ago:
                    failed_1h += 1

        queue_depth = sum(1 for s in active_jobs.values() if s == 'queued')
        jobs_processing = sum(1 for s in active_jobs.values() if s == 'processing')
        avg_time = sum(processing_times) / len(processing_times) if processing_times else 0
        p95_time = self._percentile(sorted(processing_times), 95) if processing_times else 0

        return {
            'queue_depth': queue_depth,
            'jobs_processing': jobs_processing,
            'jobs_completed_1h': completed_1h,
            'jobs_failed_1h': failed_1h,
            'avg_processing_ms': round(avg_time, 2),
            'p95_processing_ms': round(p95_time, 2),
        }

    def record_error(self, error_type: str, endpoint: str = ''):
        """Record an error for spike detection."""
        self._error_window.append({
            'timestamp': time.time(),
            'error_type': error_type,
            'endpoint': endpoint,
        })

    def check_error_spike(self, threshold: int = 10, window_seconds: int = 60) -> Optional[Dict]:
        """Check if there's an error spike (too many errors in a short window)."""
        if not self._error_window:
            return None

        now = time.time()
        cutoff = now - window_seconds
        recent_errors = [e for e in self._error_window if e['timestamp'] >= cutoff]

        if len(recent_errors) >= threshold:
            by_type = {}
            for e in recent_errors:
                by_type[e['error_type']] = by_type.get(e['error_type'], 0) + 1
            return {
                'spike_detected': True,
                'error_count': len(recent_errors),
                'window_seconds': window_seconds,
                'threshold': threshold,
                'by_type': by_type,
                'most_common': max(by_type, key=by_type.get),
            }
        return None

    def get_system_health(self) -> Dict[str, Any]:
        """Get comprehensive system health including disk, memory, services."""
        health = {'timestamp': datetime.utcnow().isoformat() + 'Z'}

        # Disk usage
        try:
            disk = shutil.disk_usage('/opt/app')
            health['disk'] = {
                'total_gb': round(disk.total / (1024**3), 1),
                'used_gb': round(disk.used / (1024**3), 1),
                'free_gb': round(disk.free / (1024**3), 1),
                'used_pct': round(disk.used / disk.total * 100, 1),
            }
        except Exception:
            health['disk'] = {'error': 'unavailable'}

        # Memory (Linux-specific)
        try:
            with open('/proc/meminfo', 'r') as f:
                meminfo = {}
                for line in f:
                    parts = line.split(':')
                    if len(parts) == 2:
                        key = parts[0].strip()
                        val = int(parts[1].strip().split()[0])
                        meminfo[key] = val

                total = meminfo.get('MemTotal', 0) / (1024 * 1024)
                available = meminfo.get('MemAvailable', 0) / (1024 * 1024)
                health['memory'] = {
                    'total_gb': round(total, 1),
                    'available_gb': round(available, 1),
                    'used_pct': round((1 - available / total) * 100, 1) if total else 0,
                }
        except Exception:
            health['memory'] = {'error': 'unavailable'}

        # Service status
        services = ['app-web', 'app-orchestrator', 'app-intake', 'nginx']
        service_status = {}
        for svc in services:
            try:
                result = subprocess.run(
                    ['systemctl', 'is-active', svc],
                    capture_output=True, text=True, timeout=5
                )
                service_status[svc] = result.stdout.strip()
            except Exception:
                service_status[svc] = 'unknown'

        health['services'] = service_status
        health['services_active'] = sum(1 for s in service_status.values() if s == 'active')
        health['services_total'] = len(services)
        return health

    @staticmethod
    def _percentile(sorted_data: List[float], pct: float) -> float:
        """Calculate percentile from sorted data."""
        if not sorted_data:
            return 0.0
        k = (len(sorted_data) - 1) * (pct / 100)
        f = int(k)
        c = f + 1
        if c >= len(sorted_data):
            return sorted_data[-1]
        return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])


# Global singleton
_monitor = None

def get_monitor() -> HealthMonitor:
    """Get the global health monitor instance."""
    global _monitor
    if _monitor is None:
        _monitor = HealthMonitor()
    return _monitor
