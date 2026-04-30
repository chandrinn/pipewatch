"""Trend analysis utilities derived from pipeline history records."""

from __future__ import annotations

from dataclasses import dataclass
from statistics import mean, stdev
from typing import List, Optional


@dataclass
class TrendSummary:
    """Aggregated trend statistics for a single pipeline."""

    pipeline_name: str
    total_runs: int
    healthy_runs: int
    failure_rate: float          # 0.0 – 1.0
    avg_rows_processed: float
    avg_duration_seconds: float
    avg_error_count: float
    rows_stddev: Optional[float]  # None when fewer than 2 data points

    @property
    def health_percentage(self) -> float:
        """Return health as a 0–100 percentage."""
        return (1.0 - self.failure_rate) * 100.0

    @property
    def is_degraded(self) -> bool:
        """Return True when the failure rate exceeds 20%.

        Useful for quickly flagging pipelines that need attention without
        having to inspect the raw ``failure_rate`` value.
        """
        return self.failure_rate > 0.20


def compute_trend(records: List[dict]) -> Optional[TrendSummary]:
    """Compute a :class:`TrendSummary` from a list of history records.

    Returns *None* when *records* is empty.

    Each record is expected to contain the following keys:

    * ``pipeline_name`` (str) – name of the pipeline (taken from the first record)
    * ``healthy`` (bool) – whether the run completed without failures
    * ``rows_processed`` (int | float) – number of rows handled in the run
    * ``duration_seconds`` (float) – wall-clock time of the run
    * ``error_count`` (int) – number of errors logged during the run
    """
    if not records:
        return None

    pipeline_name: str = records[0]["pipeline_name"]
    total = len(records)
    healthy = sum(1 for r in records if r.get("healthy", False))

    rows_list = [r["rows_processed"] for r in records]
    durations = [r["duration_seconds"] for r in records]
    errors = [r["error_count"] for r in records]

    rows_sd: Optional[float] = stdev(rows_list) if len(rows_list) >= 2 else None

    return TrendSummary(
        pipeline_name=pipeline_name,
        total_runs=total,
        healthy_runs=healthy,
        failure_rate=1.0 - (healthy / total),
        avg_rows_processed=mean(rows_list),
        avg_duration_seconds=mean(durations),
        avg_error_count=mean(errors),
        rows_stddev=rows_sd,
    )


def format_trend(summary: TrendSummary) -> str:
    """Return a human-readable multi-line trend report."""
    stddev_str = (
        f"{summary.rows_stddev:.1f}" if summary.rows_stddev is not None else "n/a"
    )
    lines = [
        f"Trend Report — {summary.pipeline_name}",
        f"  Runs analysed : {summary.total_runs}",
        f"  Health        : {summary.health_percentage:.1f}% ({summary.healthy_runs}/{summary.total_runs} OK)",
        f"  Avg rows      : {summary.avg_rows_processed:.0f}  (±{stddev_str})",
        f"  Avg duration  : {summary.avg_duration_seconds:.2f}s",
        f"  Avg errors    : {summary.avg_error_count:.2f}",
    ]
    return "\n".join(lines)
