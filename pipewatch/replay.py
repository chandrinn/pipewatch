"""Replay historical pipeline evaluations for debugging and analysis."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.history import load_history
from pipewatch.metrics import MetricEvaluation, PipelineMetrics
from pipewatch.reporter import format_evaluation


@dataclass
class ReplayOptions:
    pipeline_name: Optional[str] = None
    last_n: Optional[int] = None
    only_violations: bool = False


@dataclass
class ReplayResult:
    records_shown: int
    records_total: int
    pipeline_name: Optional[str]


def _record_to_evaluation(record: dict) -> MetricEvaluation:
    """Reconstruct a MetricEvaluation from a history record dict."""
    metrics = PipelineMetrics(
        pipeline_name=record["pipeline_name"],
        row_count=record["row_count"],
        duration_seconds=record["duration_seconds"],
        start_time=record["start_time"],
        end_time=record["end_time"],
    )
    return MetricEvaluation(
        metrics=metrics,
        healthy=record["healthy"],
        violations=record.get("violations", []),
    )


def filter_records(records: List[dict], options: ReplayOptions) -> List[dict]:
    """Apply replay filter options to a list of history records."""
    filtered = records

    if options.pipeline_name:
        filtered = [r for r in filtered if r["pipeline_name"] == options.pipeline_name]

    if options.only_violations:
        filtered = [r for r in filtered if not r.get("healthy", True)]

    if options.last_n is not None:
        filtered = filtered[-options.last_n :]

    return filtered


def replay_history(options: ReplayOptions, history_dir: Optional[str] = None) -> ReplayResult:
    """Load and replay history records, printing formatted evaluations."""
    all_records = load_history(history_dir=history_dir)
    filtered = filter_records(all_records, options)

    for record in filtered:
        evaluation = _record_to_evaluation(record)
        print(format_evaluation(evaluation))

    return ReplayResult(
        records_shown=len(filtered),
        records_total=len(all_records),
        pipeline_name=options.pipeline_name,
    )
