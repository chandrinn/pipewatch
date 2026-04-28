"""Alert notification module for pipewatch."""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from pipewatch.metrics import MetricEvaluation


@dataclass
class Alert:
    """Represents a triggered alert for a pipeline."""

    pipeline_name: str
    severity: str  # "warning" or "critical"
    message: str
    triggered_at: str
    violations: list[str]


def _severity_from_evaluation(evaluation: MetricEvaluation) -> Optional[str]:
    """Determine alert severity based on evaluation result."""
    if evaluation.is_healthy:
        return None
    violation_count = len(evaluation.violations)
    if violation_count >= 2:
        return "critical"
    return "warning"


def build_alert(evaluation: MetricEvaluation) -> Optional[Alert]:
    """Build an Alert from a MetricEvaluation, or None if healthy."""
    severity = _severity_from_evaluation(evaluation)
    if severity is None:
        return None

    message = (
        f"Pipeline '{evaluation.pipeline_name}' has {len(evaluation.violations)} "
        f"violation(s) detected."
    )
    return Alert(
        pipeline_name=evaluation.pipeline_name,
        severity=severity,
        message=message,
        triggered_at=datetime.utcnow().isoformat(),
        violations=list(evaluation.violations),
    )


def format_alert(alert: Alert) -> str:
    """Format an alert for console output."""
    icon = "🔴" if alert.severity == "critical" else "🟡"
    lines = [
        f"{icon} [{alert.severity.upper()}] {alert.pipeline_name}",
        f"   {alert.message}",
        f"   Triggered at: {alert.triggered_at}",
    ]
    for v in alert.violations:
        lines.append(f"   • {v}")
    return "\n".join(lines)


def process_alerts(evaluations: list[MetricEvaluation]) -> list[Alert]:
    """Generate alerts for all unhealthy evaluations."""
    alerts = []
    for evaluation in evaluations:
        alert = build_alert(evaluation)
        if alert is not None:
            alerts.append(alert)
    return alerts
