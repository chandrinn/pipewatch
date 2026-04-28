"""Tests for pipewatch.export."""

from __future__ import annotations

import json
import csv
from pathlib import Path

import pytest

from pipewatch.export import (
    export_snapshot_json,
    export_snapshot_csv,
    export_trend_json,
)
from pipewatch.metrics import MetricEvaluation
from pipewatch.snapshot import Snapshot
from pipewatch.trend import TrendSummary


@pytest.fixture()
def sample_snapshot() -> Snapshot:
    evals = [
        MetricEvaluation(pipeline_name="alpha", healthy=True, violations=[]),
        MetricEvaluation(
            pipeline_name="beta", healthy=False, violations=["duration exceeded"]
        ),
    ]
    return Snapshot(
        timestamp="2024-01-01T00:00:00",
        total_pipelines=2,
        healthy=1,
        degraded=1,
        evaluations=evals,
    )


@pytest.fixture()
def sample_trend() -> TrendSummary:
    return TrendSummary(
        pipeline_name="alpha",
        total_runs=10,
        healthy_runs=8,
        health_pct=80.0,
        last_seen="2024-01-01T00:00:00",
    )


def test_export_snapshot_json_creates_file(tmp_path, sample_snapshot):
    out = tmp_path / "out" / "snap.json"
    export_snapshot_json(sample_snapshot, out)
    assert out.exists()


def test_export_snapshot_json_content(tmp_path, sample_snapshot):
    out = tmp_path / "snap.json"
    export_snapshot_json(sample_snapshot, out)
    data = json.loads(out.read_text())
    assert data["total_pipelines"] == 2
    assert data["healthy"] == 1
    assert len(data["pipelines"]) == 2


def test_export_snapshot_json_pipeline_names(tmp_path, sample_snapshot):
    out = tmp_path / "snap.json"
    export_snapshot_json(sample_snapshot, out)
    data = json.loads(out.read_text())
    names = [p["name"] for p in data["pipelines"]]
    assert "alpha" in names and "beta" in names


def test_export_snapshot_csv_creates_file(tmp_path, sample_snapshot):
    out = tmp_path / "snap.csv"
    export_snapshot_csv(sample_snapshot, out)
    assert out.exists()


def test_export_snapshot_csv_row_count(tmp_path, sample_snapshot):
    out = tmp_path / "snap.csv"
    export_snapshot_csv(sample_snapshot, out)
    with out.open() as fh:
        rows = list(csv.DictReader(fh))
    assert len(rows) == 2


def test_export_snapshot_csv_violations_joined(tmp_path, sample_snapshot):
    out = tmp_path / "snap.csv"
    export_snapshot_csv(sample_snapshot, out)
    with out.open() as fh:
        rows = list(csv.DictReader(fh))
    beta_row = next(r for r in rows if r["pipeline"] == "beta")
    assert beta_row["violations"] == "duration exceeded"


def test_export_trend_json_creates_file(tmp_path, sample_trend):
    out = tmp_path / "trend.json"
    export_trend_json(sample_trend, out)
    assert out.exists()


def test_export_trend_json_content(tmp_path, sample_trend):
    out = tmp_path / "trend.json"
    export_trend_json(sample_trend, out)
    data = json.loads(out.read_text())
    assert data["pipeline_name"] == "alpha"
    assert data["health_pct"] == 80.0
    assert data["total_runs"] == 10
