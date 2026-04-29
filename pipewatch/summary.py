"""Summary report generation for pipewatch CLI output."""

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.metrics import MetricEvaluation
from pipewatch.trend import TrendSummary, format_trend


@dataclass
class PipelineSummaryRow:
    name: str
    status: str
    violations: int
    rows_processed: int
    duration_seconds: float
    trend: Optional[str]


@dataclass
class OverallSummary:
    total_pipelines: int
    healthy: int
    unhealthy: int
    rows: List[PipelineSummaryRow]


def _status_label(evaluation: MetricEvaluation) -> str:
    return "OK" if evaluation.healthy else "CRITICAL"


def build_summary_row(
    evaluation: MetricEvaluation,
    trend: Optional[TrendSummary] = None,
) -> PipelineSummaryRow:
    metrics = evaluation.metrics
    return PipelineSummaryRow(
        name=metrics.pipeline_name,
        status=_status_label(evaluation),
        violations=len(evaluation.violations),
        rows_processed=metrics.rows_processed,
        duration_seconds=round(metrics.duration_seconds, 2),
        trend=format_trend(trend) if trend else None,
    )


def build_overall_summary(
    evaluations: List[MetricEvaluation],
    trends: Optional[dict] = None,
) -> OverallSummary:
    trends = trends or {}
    rows = [
        build_summary_row(ev, trends.get(ev.metrics.pipeline_name))
        for ev in evaluations
    ]
    healthy = sum(1 for ev in evaluations if ev.healthy)
    return OverallSummary(
        total_pipelines=len(evaluations),
        healthy=healthy,
        unhealthy=len(evaluations) - healthy,
        rows=rows,
    )


def format_summary_table(summary: OverallSummary) -> str:
    header = f"{'Pipeline':<24} {'Status':<10} {'Rows':>8} {'Duration(s)':>12} {'Violations':>10} {'Trend':<20}"
    separator = "-" * len(header)
    lines = [header, separator]
    for row in summary.rows:
        trend_str = row.trend or "n/a"
        lines.append(
            f"{row.name:<24} {row.status:<10} {row.rows_processed:>8} "
            f"{row.duration_seconds:>12.2f} {row.violations:>10} {trend_str:<20}"
        )
    lines.append(separator)
    lines.append(
        f"Total: {summary.total_pipelines}  "
        f"Healthy: {summary.healthy}  "
        f"Unhealthy: {summary.unhealthy}"
    )
    return "\n".join(lines)
