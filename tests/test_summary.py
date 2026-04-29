"""Tests for pipewatch.summary module."""

import pytest
from pipewatch.metrics import MetricEvaluation, PipelineMetrics
from pipewatch.trend import TrendSummary
from pipewatch.summary import (
    build_summary_row,
    build_overall_summary,
    format_summary_table,
    OverallSummary,
    PipelineSummaryRow,
)
from datetime import datetime


def _make_metrics(name: str, rows: int = 1000, duration: float = 30.0) -> PipelineMetrics:
    return PipelineMetrics(
        pipeline_name=name,
        rows_processed=rows,
        duration_seconds=duration,
        started_at=datetime(2024, 1, 1, 12, 0, 0),
        finished_at=datetime(2024, 1, 1, 12, 0, 30),
    )


def _make_eval(name: str, healthy: bool = True, violations=None) -> MetricEvaluation:
    return MetricEvaluation(
        metrics=_make_metrics(name),
        healthy=healthy,
        violations=violations or [],
    )


@pytest.fixture
def evaluations():
    return [
        _make_eval("pipeline_a", healthy=True),
        _make_eval("pipeline_b", healthy=False, violations=["duration exceeded"]),
        _make_eval("pipeline_c", healthy=True),
    ]


def test_build_summary_row_ok():
    ev = _make_eval("pipe_x", healthy=True)
    row = build_summary_row(ev)
    assert row.name == "pipe_x"
    assert row.status == "OK"
    assert row.violations == 0


def test_build_summary_row_critical():
    ev = _make_eval("pipe_y", healthy=False, violations=["low rows"])
    row = build_summary_row(ev)
    assert row.status == "CRITICAL"
    assert row.violations == 1


def test_build_summary_row_with_trend():
    ev = _make_eval("pipe_z")
    trend = TrendSummary(total_runs=5, healthy_runs=4, unhealthy_runs=1, health_pct=80.0)
    row = build_summary_row(ev, trend=trend)
    assert row.trend is not None
    assert "80" in row.trend


def test_build_summary_row_no_trend():
    ev = _make_eval("pipe_z")
    row = build_summary_row(ev, trend=None)
    assert row.trend is None


def test_build_overall_summary_counts(evaluations):
    summary = build_overall_summary(evaluations)
    assert summary.total_pipelines == 3
    assert summary.healthy == 2
    assert summary.unhealthy == 1


def test_build_overall_summary_rows_length(evaluations):
    summary = build_overall_summary(evaluations)
    assert len(summary.rows) == 3


def test_build_overall_summary_with_trends(evaluations):
    trends = {
        "pipeline_a": TrendSummary(total_runs=3, healthy_runs=3, unhealthy_runs=0, health_pct=100.0)
    }
    summary = build_overall_summary(evaluations, trends=trends)
    row_a = next(r for r in summary.rows if r.name == "pipeline_a")
    assert row_a.trend is not None


def test_format_summary_table_contains_pipeline_names(evaluations):
    summary = build_overall_summary(evaluations)
    table = format_summary_table(summary)
    assert "pipeline_a" in table
    assert "pipeline_b" in table
    assert "pipeline_c" in table


def test_format_summary_table_contains_totals(evaluations):
    summary = build_overall_summary(evaluations)
    table = format_summary_table(summary)
    assert "Total: 3" in table
    assert "Healthy: 2" in table
    assert "Unhealthy: 1" in table


def test_format_summary_table_header(evaluations):
    summary = build_overall_summary(evaluations)
    table = format_summary_table(summary)
    assert "Pipeline" in table
    assert "Status" in table
    assert "Rows" in table
