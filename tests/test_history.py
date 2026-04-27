"""Tests for pipewatch.history."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from pipewatch.history import clear_history, load_history, save_evaluation
from pipewatch.metrics import MetricEvaluation, PipelineMetrics


@pytest.fixture()
def tmp_history_dir(tmp_path: Path) -> Path:
    return tmp_path / "history"


@pytest.fixture()
def sample_evaluation() -> MetricEvaluation:
    metrics = PipelineMetrics(
        pipeline_name="test_pipe",
        rows_processed=1000,
        duration_seconds=30.0,
        error_count=0,
        started_at=datetime(2024, 1, 1, 12, 0, 0),
        finished_at=datetime(2024, 1, 1, 12, 0, 30),
    )
    return MetricEvaluation(
        pipeline_name="test_pipe",
        metrics=metrics,
        healthy=True,
        violations=[],
    )


def test_save_evaluation_creates_file(tmp_history_dir, sample_evaluation):
    path = save_evaluation(sample_evaluation, history_dir=tmp_history_dir)
    assert path.exists()


def test_save_evaluation_record_structure(tmp_history_dir, sample_evaluation):
    path = save_evaluation(sample_evaluation, history_dir=tmp_history_dir)
    records = json.loads(path.read_text())
    assert len(records) == 1
    record = records[0]
    assert record["pipeline_name"] == "test_pipe"
    assert record["healthy"] is True
    assert record["rows_processed"] == 1000
    assert record["duration_seconds"] == 30.0
    assert record["error_count"] == 0
    assert "timestamp" in record


def test_save_evaluation_appends(tmp_history_dir, sample_evaluation):
    save_evaluation(sample_evaluation, history_dir=tmp_history_dir)
    save_evaluation(sample_evaluation, history_dir=tmp_history_dir)
    records = load_history("test_pipe", history_dir=tmp_history_dir)
    assert len(records) == 2


def test_load_history_empty_when_no_file(tmp_history_dir):
    records = load_history("nonexistent", history_dir=tmp_history_dir)
    assert records == []


def test_load_history_limit(tmp_history_dir, sample_evaluation):
    for _ in range(10):
        save_evaluation(sample_evaluation, history_dir=tmp_history_dir)
    records = load_history("test_pipe", history_dir=tmp_history_dir, limit=5)
    assert len(records) == 5


def test_clear_history_removes_file(tmp_history_dir, sample_evaluation):
    save_evaluation(sample_evaluation, history_dir=tmp_history_dir)
    removed = clear_history("test_pipe", history_dir=tmp_history_dir)
    assert removed is True
    assert load_history("test_pipe", history_dir=tmp_history_dir) == []


def test_clear_history_returns_false_when_missing(tmp_history_dir):
    removed = clear_history("ghost_pipe", history_dir=tmp_history_dir)
    assert removed is False
