"""Formats and prints pipeline metric evaluation results to stdout."""
from datetime import datetime
from typing import Iterable

from pipewatch.metrics import MetricEvaluation, PipelineMetrics

STATUS_ICONS = {
    "ok": "✅",
    "warning": "⚠️",
    "critical": "❌",
}


def _status_icon(status: str) -> str:
    return STATUS_ICONS.get(status, "?")


def format_metrics_summary(metrics: PipelineMetrics) -> str:
    """Return a single-line summary of raw metrics."""
    return (
        f"[{metrics.timestamp.strftime('%Y-%m-%d %H:%M:%S')}] "
        f"{metrics.pipeline_name}: "
        f"rows={metrics.row_count}, "
        f"duration={metrics.duration_seconds:.2f}s, "
        f"errors={metrics.error_count}"
    )


def format_evaluation(evaluation: MetricEvaluation) -> str:
    """Return a formatted string for a single pipeline evaluation."""
    icon = _status_icon(evaluation.status)
    lines = [f"{icon}  {evaluation.pipeline_name} — {evaluation.status.upper()}"]
    for violation in evaluation.violations:
        lines.append(f"     • {violation}")
    return "\n".join(lines)


def print_report(
    evaluations: Iterable[MetricEvaluation],
    title: str = "PipeWatch Report",
) -> None:
    """Print a full report for multiple pipeline evaluations."""
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    separator = "─" * 50
    print(f"\n{separator}")
    print(f"  {title}  —  {now}")
    print(separator)
    evaluations = list(evaluations)
    for evaluation in evaluations:
        print(format_evaluation(evaluation))
    print(separator)
    healthy = sum(1 for e in evaluations if e.is_healthy)
    print(f"  {healthy}/{len(evaluations)} pipelines healthy")
    print(f"{separator}\n")
