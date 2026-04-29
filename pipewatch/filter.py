"""Pipeline filtering utilities for pipewatch CLI."""

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.metrics import MetricEvaluation


@dataclass
class FilterOptions:
    names: Optional[List[str]] = None
    status: Optional[str] = None  # "ok", "critical", or None for all


def _matches_name(evaluation: MetricEvaluation, names: Optional[List[str]]) -> bool:
    """Return True if the evaluation pipeline name is in the given list (or no filter set)."""
    if not names:
        return True
    return evaluation.pipeline_name in names


def _matches_status(evaluation: MetricEvaluation, status: Optional[str]) -> bool:
    """Return True if the evaluation health status matches the filter."""
    if status is None:
        return True
    if status == "ok":
        return evaluation.healthy
    if status == "critical":
        return not evaluation.healthy
    return True


def filter_evaluations(
    evaluations: List[MetricEvaluation],
    options: FilterOptions,
) -> List[MetricEvaluation]:
    """Filter a list of MetricEvaluation objects based on FilterOptions."""
    return [
        ev for ev in evaluations
        if _matches_name(ev, options.names) and _matches_status(ev, options.status)
    ]


def parse_filter_options(
    names_arg: Optional[str],
    status_arg: Optional[str],
) -> FilterOptions:
    """Build a FilterOptions from raw CLI string arguments."""
    names: Optional[List[str]] = None
    if names_arg:
        names = [n.strip() for n in names_arg.split(",") if n.strip()]

    status: Optional[str] = None
    if status_arg and status_arg.lower() in ("ok", "critical"):
        status = status_arg.lower()

    return FilterOptions(names=names, status=status)
