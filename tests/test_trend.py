"""Tests for pipewatch.trend."""

from __future__ import annotations

import pytest

from pipewatch.trend import TrendSummary, compute_trend, format_trend


@pytest.fixture()
def records():
    base = {"pipeline_name": "etl_main", "error_count": 0}
    return [
        {**base, "healthy": True,  "rows_processed": 1000, "duration_seconds": 20.0},
        {**base, "healthy": True,  "rows_processed": 1200, "duration_seconds": 22.0},
        {**base, "healthy": False, "rows_processed": 400,  "duration_seconds": 45.0},
        {**base, "healthy": True,  "rows_processed": 1100, "duration_seconds": 21.0},
    ]


def test_compute_trend_returns_summary(records):
    summary = compute_trend(records)
    assert isinstance(summary, TrendSummary)


def test_compute_trend_empty_returns_none():
    assert compute_trend([]) is None


def test_compute_trend_total_runs(records):
    summary = compute_trend(records)
    assert summary.total_runs == 4


def test_compute_trend_healthy_runs(records):
    summary = compute_trend(records)
    assert summary.healthy_runs == 3


def test_compute_trend_failure_rate(records):
    summary = compute_trend(records)
    assert summary.failure_rate == pytest.approx(0.25)


def test_compute_trend_health_percentage(records):
    summary = compute_trend(records)
    assert summary.health_percentage == pytest.approx(75.0)


def test_compute_trend_avg_rows(records):
    summary = compute_trend(records)
    assert summary.avg_rows_processed == pytest.approx(925.0)


def test_compute_trend_avg_duration(records):
    summary = compute_trend(records)
    assert summary.avg_duration_seconds == pytest.approx(27.0)


def test_compute_trend_rows_stddev_present(records):
    summary = compute_trend(records)
    assert summary.rows_stddev is not None
    assert summary.rows_stddev > 0


def test_compute_trend_rows_stddev_none_single_record():
    record = [{"pipeline_name": "p", "healthy": True, "rows_processed": 500,
               "duration_seconds": 10.0, "error_count": 0}]
    summary = compute_trend(record)
    assert summary.rows_stddev is None


def test_format_trend_contains_pipeline_name(records):
    summary = compute_trend(records)
    report = format_trend(summary)
    assert "etl_main" in report


def test_format_trend_contains_health_percentage(records):
    summary = compute_trend(records)
    report = format_trend(summary)
    assert "75.0%" in report
