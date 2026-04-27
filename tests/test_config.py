"""Tests for pipewatch configuration loader."""

import os
import textwrap
import pytest

from pipewatch.config import load_config, AppConfig, PipelineConfig, ThresholdConfig


@pytest.fixture
def config_file(tmp_path):
    """Write a sample config YAML and return its path."""
    content = textwrap.dedent("""\
        refresh_interval_seconds: 30
        log_level: DEBUG
        pipelines:
          orders_etl:
            source: postgres://localhost/orders
            thresholds:
              max_duration_seconds: 120.0
              max_error_rate: 0.01
            tags:
              team: data-eng
          events_stream:
            source: kafka://broker:9092/events
    """)
    cfg = tmp_path / "config.yaml"
    cfg.write_text(content)
    return str(cfg)


def test_load_config_returns_app_config(config_file):
    cfg = load_config(config_file)
    assert isinstance(cfg, AppConfig)


def test_global_settings(config_file):
    cfg = load_config(config_file)
    assert cfg.refresh_interval_seconds == 30
    assert cfg.log_level == "DEBUG"


def test_pipeline_names(config_file):
    cfg = load_config(config_file)
    assert set(cfg.pipelines.keys()) == {"orders_etl", "events_stream"}


def test_pipeline_source(config_file):
    cfg = load_config(config_file)
    assert cfg.pipelines["orders_etl"].source == "postgres://localhost/orders"


def test_custom_thresholds(config_file):
    cfg = load_config(config_file)
    t = cfg.pipelines["orders_etl"].thresholds
    assert t.max_duration_seconds == 120.0
    assert t.max_error_rate == 0.01


def test_default_thresholds_applied(config_file):
    cfg = load_config(config_file)
    t = cfg.pipelines["events_stream"].thresholds
    assert t.max_duration_seconds == ThresholdConfig().max_duration_seconds
    assert t.max_error_rate == ThresholdConfig().max_error_rate


def test_tags_loaded(config_file):
    cfg = load_config(config_file)
    assert cfg.pipelines["orders_etl"].tags == {"team": "data-eng"}


def test_missing_file_raises():
    with pytest.raises(FileNotFoundError):
        load_config("/nonexistent/path/config.yaml")


def test_missing_source_raises(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text("pipelines:\n  broken_pipe:\n    thresholds: {}\n")
    with pytest.raises(ValueError, match="missing required field 'source'"):
        load_config(str(bad))
