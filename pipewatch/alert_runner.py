"""Orchestrates alert processing and notification for a monitoring run."""

from pipewatch.alert import process_alerts
from pipewatch.metrics import MetricEvaluation
from pipewatch.notifier import NotifierConfig, NotifyResult, notify_all


def run_alerts(
    evaluations: list[MetricEvaluation],
    notifier_config: NotifierConfig,
) -> dict:
    """Process evaluations, build alerts, and dispatch notifications.

    Returns a summary dict with alert count and notify results.
    """
    alerts = process_alerts(evaluations)
    results: list[NotifyResult] = []

    if alerts:
        results = notify_all(alerts, notifier_config)

    return {
        "total_evaluations": len(evaluations),
        "alerts_triggered": len(alerts),
        "notify_results": results,
    }


def alert_summary_line(summary: dict) -> str:
    """Return a human-readable one-line summary of the alert run."""
    total = summary["total_evaluations"]
    triggered = summary["alerts_triggered"]
    if triggered == 0:
        return f"✅ All {total} pipeline(s) healthy — no alerts triggered."
    return (
        f"⚠️  {triggered} alert(s) triggered out of {total} pipeline(s) evaluated."
    )
