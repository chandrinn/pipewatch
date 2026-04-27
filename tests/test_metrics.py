"""Tests for pipewatch.metrics module."""
import pytest

from pipewatch.config import PipelineConfig, ThresholdConfig
from pipewatch.metrics import MetricEvaluation, PipelineMetrics, evaluate_metrics


@pytest.fixture
def base_config() -> PipelineConfig:
    return PipelineConfig(
        name="test_pipeline",
        source="db://localhost/test",
        thresholds=ThresholdConfig(
            max_duration_seconds=60.0,
            min_row_count=100,
            max_error_count=0,
        ),
    )


@pytest.fixture
def healthy_metrics() -> PipelineMetrics:
    return PipelineMetrics(
        pipeline_name="test_pipeline",
        row_count=500,
        duration_seconds=30.0,
        error_count=0,
    )


def test_evaluate_metrics_healthy(base_config, healthy_metrics):
    result = evaluate_metrics(healthy_metrics, base_config)
    assert result.status == "ok"
    assert result.violations == []
    assert result.is_healthy is True


def test_evaluate_metrics_duration_exceeded(base_config):
    metrics = PipelineMetrics(
        pipeline_name="test_pipeline",
        row_count=500,
        duration_seconds=120.0,
        error_count=0,
    )
    result = evaluate_metrics(metrics, base_config)
    assert result.status == "critical"
    assert any("duration" in v for v in result.violations)


def test_evaluate_metrics_low_row_count(base_config):
    metrics = PipelineMetrics(
        pipeline_name="test_pipeline",
        row_count=10,
        duration_seconds=30.0,
        error_count=0,
    )
    result = evaluate_metrics(metrics, base_config)
    assert result.status == "critical"
    assert any("row count" in v for v in result.violations)


def test_evaluate_metrics_errors_exceeded(base_config):
    metrics = PipelineMetrics(
        pipeline_name="test_pipeline",
        row_count=500,
        duration_seconds=30.0,
        error_count=3,
    )
    result = evaluate_metrics(metrics, base_config)
    assert result.status == "critical"
    assert any("error count" in v for v in result.violations)


def test_evaluate_metrics_multiple_violations(base_config):
    metrics = PipelineMetrics(
        pipeline_name="test_pipeline",
        row_count=5,
        duration_seconds=200.0,
        error_count=10,
    )
    result = evaluate_metrics(metrics, base_config)
    assert len(result.violations) == 3


def test_rows_per_second():
    metrics = PipelineMetrics(
        pipeline_name="p", row_count=100, duration_seconds=10.0, error_count=0
    )
    assert metrics.rows_per_second == pytest.approx(10.0)


def test_rows_per_second_zero_duration():
    metrics = PipelineMetrics(
        pipeline_name="p", row_count=100, duration_seconds=0.0, error_count=0
    )
    assert metrics.rows_per_second == 0.0
