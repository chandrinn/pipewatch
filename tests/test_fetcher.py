"""Tests for pipewatch.fetcher module."""

import pytest
from datetime import datetime

from pipewatch.config import PipelineConfig, ThresholdConfig
from pipewatch.fetcher import fetch_metrics, fetch_all_metrics, _simulate_metrics
from pipewatch.metrics import PipelineMetrics


@pytest.fixture
def pipeline_config() -> PipelineConfig:
    return PipelineConfig(
        name="orders",
        source="mock",
        thresholds=ThresholdConfig(
            max_duration_seconds=120,
            min_rows=1000,
            max_errors=5,
        ),
    )


def test_simulate_metrics_returns_pipeline_metrics(pipeline_config):
    metrics = _simulate_metrics(pipeline_config, seed=42)
    assert isinstance(metrics, PipelineMetrics)


def test_simulate_metrics_pipeline_name(pipeline_config):
    metrics = _simulate_metrics(pipeline_config, seed=42)
    assert metrics.pipeline_name == "orders"


def test_simulate_metrics_has_timestamps(pipeline_config):
    metrics = _simulate_metrics(pipeline_config, seed=42)
    assert isinstance(metrics.started_at, datetime)
    assert isinstance(metrics.finished_at, datetime)
    assert metrics.finished_at > metrics.started_at


def test_simulate_metrics_deterministic(pipeline_config):
    m1 = _simulate_metrics(pipeline_config, seed=7)
    m2 = _simulate_metrics(pipeline_config, seed=7)
    assert m1.rows_processed == m2.rows_processed
    assert m1.error_count == m2.error_count


def test_fetch_metrics_mock_source(pipeline_config):
    metrics = fetch_metrics(pipeline_config)
    assert isinstance(metrics, PipelineMetrics)
    assert metrics.pipeline_name == "orders"


def test_fetch_metrics_unsupported_source(pipeline_config):
    pipeline_config.source = "postgres"
    with pytest.raises(NotImplementedError, match="postgres"):
        fetch_metrics(pipeline_config)


def test_fetch_all_metrics_returns_list(pipeline_config):
    configs = [pipeline_config]
    results = fetch_all_metrics(configs)
    assert len(results) == 1
    assert isinstance(results[0], PipelineMetrics)


def test_fetch_all_metrics_skips_failed_sources(pipeline_config):
    bad_config = PipelineConfig(
        name="broken",
        source="unsupported",
        thresholds=pipeline_config.thresholds,
    )
    results = fetch_all_metrics([pipeline_config, bad_config])
    assert len(results) == 1
    assert results[0].pipeline_name == "orders"
