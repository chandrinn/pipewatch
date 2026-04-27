"""Tests for pipewatch.reporter module."""
from datetime import datetime

import pytest

from pipewatch.metrics import MetricEvaluation, PipelineMetrics
from pipewatch.reporter import format_evaluation, format_metrics_summary, print_report


@pytest.fixture
def ok_evaluation() -> MetricEvaluation:
    return MetricEvaluation(pipeline_name="orders_etl", status="ok", violations=[])


@pytest.fixture
def critical_evaluation() -> MetricEvaluation:
    return MetricEvaluation(
        pipeline_name="users_sync",
        status="critical",
        violations=["duration 120.0s exceeds max 60.0s", "error count 5 exceeds max 0"],
    )


def test_format_evaluation_ok(ok_evaluation):
    result = format_evaluation(ok_evaluation)
    assert "orders_etl" in result
    assert "OK" in result
    assert "✅" in result


def test_format_evaluation_critical(critical_evaluation):
    result = format_evaluation(critical_evaluation)
    assert "users_sync" in result
    assert "CRITICAL" in result
    assert "❌" in result
    assert "duration" in result
    assert "error count" in result


def test_format_evaluation_no_violations_in_ok(ok_evaluation):
    result = format_evaluation(ok_evaluation)
    assert "•" not in result


def test_format_metrics_summary():
    metrics = PipelineMetrics(
        pipeline_name="orders_etl",
        row_count=1000,
        duration_seconds=45.5,
        error_count=2,
        timestamp=datetime(2024, 1, 15, 12, 0, 0),
    )
    result = format_metrics_summary(metrics)
    assert "orders_etl" in result
    assert "rows=1000" in result
    assert "duration=45.50s" in result
    assert "errors=2" in result
    assert "2024-01-15" in result


def test_print_report_output(capsys, ok_evaluation, critical_evaluation):
    print_report([ok_evaluation, critical_evaluation], title="Test Report")
    captured = capsys.readouterr()
    assert "Test Report" in captured.out
    assert "orders_etl" in captured.out
    assert "users_sync" in captured.out
    assert "1/2 pipelines healthy" in captured.out


def test_print_report_all_healthy(capsys, ok_evaluation):
    print_report([ok_evaluation, ok_evaluation])
    captured = capsys.readouterr()
    assert "2/2 pipelines healthy" in captured.out
