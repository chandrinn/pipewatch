"""Tests for pipewatch.snapshot module."""

from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone

import pytest

from pipewatch.metrics import MetricEvaluation, PipelineMetrics
from pipewatch.snapshot import (
    Snapshot,
    build_snapshot,
    save_snapshot,
    load_snapshot,
    format_snapshot,
)


@pytest.fixture()
def sample_metrics() -> PipelineMetrics:
    return PipelineMetrics(
        pipeline_name="orders",
        rows_processed=1000,
        duration_seconds=30.0,
        started_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        finished_at=datetime(2024, 1, 1, 12, 0, 30, tzinfo=timezone.utc),
    )


@pytest.fixture()
def evaluations(sample_metrics) -> list:
    healthy = MetricEvaluation(
        pipeline_name="orders",
        metrics=sample_metrics,
        healthy=True,
        violations=[],
    )
    unhealthy_metrics = PipelineMetrics(
        pipeline_name="returns",
        rows_processed=5,
        duration_seconds=120.0,
        started_at=sample_metrics.started_at,
        finished_at=sample_metrics.finished_at,
    )
    unhealthy = MetricEvaluation(
        pipeline_name="returns",
        metrics=unhealthy_metrics,
        healthy=False,
        violations=["row_count below threshold"],
    )
    return [healthy, unhealthy]


def test_build_snapshot_returns_snapshot(evaluations):
    snap = build_snapshot(evaluations)
    assert isinstance(snap, Snapshot)


def test_build_snapshot_counts(evaluations):
    snap = build_snapshot(evaluations)
    assert snap.pipeline_count == 2
    assert snap.healthy_count == 1
    assert snap.unhealthy_count == 1


def test_build_snapshot_pipelines_list(evaluations):
    snap = build_snapshot(evaluations)
    names = [p["pipeline"] for p in snap.pipelines]
    assert "orders" in names
    assert "returns" in names


def test_build_snapshot_captured_at_is_iso(evaluations):
    snap = build_snapshot(evaluations)
    # Should parse without raising
    datetime.fromisoformat(snap.captured_at)


def test_save_snapshot_creates_file(tmp_path, evaluations):
    snap = build_snapshot(evaluations)
    path = save_snapshot(snap, snapshot_dir=tmp_path)
    assert path.exists()


def test_save_snapshot_valid_json(tmp_path, evaluations):
    snap = build_snapshot(evaluations)
    path = save_snapshot(snap, snapshot_dir=tmp_path)
    data = json.loads(path.read_text())
    assert "captured_at" in data
    assert "pipelines" in data


def test_load_snapshot_roundtrip(tmp_path, evaluations):
    snap = build_snapshot(evaluations)
    path = save_snapshot(snap, snapshot_dir=tmp_path)
    loaded = load_snapshot(path)
    assert loaded.pipeline_count == snap.pipeline_count
    assert loaded.healthy_count == snap.healthy_count
    assert loaded.captured_at == snap.captured_at


def test_format_snapshot_contains_pipeline_names(evaluations):
    snap = build_snapshot(evaluations)
    text = format_snapshot(snap)
    assert "orders" in text
    assert "returns" in text


def test_format_snapshot_contains_counts(evaluations):
    snap = build_snapshot(evaluations)
    text = format_snapshot(snap)
    assert "2" in text  # total pipelines
