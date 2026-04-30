"""Tests for pipewatch/compare.py and pipewatch/cli_compare.py."""

import json
from pathlib import Path
from datetime import datetime, timezone

import pytest

from pipewatch.compare import build_compare_report, format_compare_report
from pipewatch.snapshot import Snapshot
from pipewatch.metrics import MetricEvaluation


def _make_evaluation(name: str, healthy: bool, violations: list[str] | None = None) -> MetricEvaluation:
    return MetricEvaluation(
        pipeline_name=name,
        healthy=healthy,
        violations=violations or [],
    )


def _make_snapshot(snap_id: str, evaluations: list[MetricEvaluation]) -> Snapshot:
    healthy = sum(1 for e in evaluations if e.healthy)
    return Snapshot(
        snapshot_id=snap_id,
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc).isoformat(),
        total_pipelines=len(evaluations),
        healthy_count=healthy,
        critical_count=len(evaluations) - healthy,
        pipelines=[e.pipeline_name for e in evaluations],
        evaluations=evaluations,
    )


@pytest.fixture
def previous_snapshot():
    return _make_snapshot(
        "snap-001",
        [
            _make_evaluation("etl_users", healthy=True),
            _make_evaluation("etl_orders", healthy=False, violations=["duration exceeded"]),
        ],
    )


@pytest.fixture
def current_snapshot():
    return _make_snapshot(
        "snap-002",
        [
            _make_evaluation("etl_users", healthy=False, violations=["row count too low"]),
            _make_evaluation("etl_orders", healthy=True),
        ],
    )


def test_build_compare_report_returns_report(previous_snapshot, current_snapshot):
    report = build_compare_report(previous_snapshot, current_snapshot)
    assert report is not None


def test_build_compare_report_ids(previous_snapshot, current_snapshot):
    report = build_compare_report(previous_snapshot, current_snapshot)
    assert report.previous_id == "snap-001"
    assert report.current_id == "snap-002"


def test_build_compare_report_counts(previous_snapshot, current_snapshot):
    report = build_compare_report(previous_snapshot, current_snapshot)
    assert report.total_pipelines == 2
    assert report.improved == 1
    assert report.degraded == 1
    assert report.unchanged == 0


def test_build_compare_report_lines_not_empty(previous_snapshot, current_snapshot):
    report = build_compare_report(previous_snapshot, current_snapshot)
    assert len(report.lines) > 0


def test_format_compare_report_is_string(previous_snapshot, current_snapshot):
    report = build_compare_report(previous_snapshot, current_snapshot)
    text = format_compare_report(report)
    assert isinstance(text, str)
    assert "snap-001" in text
    assert "snap-002" in text


def test_format_compare_report_contains_pipeline_names(previous_snapshot, current_snapshot):
    report = build_compare_report(previous_snapshot, current_snapshot)
    text = format_compare_report(report)
    assert "etl_users" in text
    assert "etl_orders" in text


def test_compare_report_no_changes_all_unchanged():
    snap = _make_snapshot(
        "snap-x",
        [_make_evaluation("pipe_a", healthy=True)],
    )
    report = build_compare_report(snap, snap)
    assert report.improved == 0
    assert report.degraded == 0
    assert report.unchanged == 1
