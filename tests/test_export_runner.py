"""Tests for pipewatch.export_runner."""

from __future__ import annotations

from pathlib import Path

import pytest

from pipewatch.export_runner import export_run, export_summary_line
from pipewatch.metrics import MetricEvaluation
from pipewatch.snapshot import Snapshot
from pipewatch.trend import TrendSummary


@pytest.fixture()
def snapshot() -> Snapshot:
    evals = [
        MetricEvaluation(pipeline_name="pipe1", healthy=True, violations=[]),
    ]
    return Snapshot(
        timestamp="2024-06-01T12:00:00",
        total_pipelines=1,
        healthy=1,
        degraded=0,
        evaluations=evals,
    )


@pytest.fixture()
def trends() -> list[TrendSummary]:
    return [
        TrendSummary(
            pipeline_name="pipe1",
            total_runs=5,
            healthy_runs=5,
            health_pct=100.0,
            last_seen="2024-06-01T12:00:00",
        )
    ]


def test_export_run_returns_paths(tmp_path, snapshot, trends):
    paths = export_run(snapshot, trends, tmp_path)
    assert len(paths) == 2


def test_export_run_json_snapshot_exists(tmp_path, snapshot, trends):
    export_run(snapshot, trends, tmp_path, fmt="json")
    assert (tmp_path / "snapshot.json").exists()


def test_export_run_csv_snapshot_exists(tmp_path, snapshot, trends):
    export_run(snapshot, trends, tmp_path, fmt="csv")
    assert (tmp_path / "snapshot.csv").exists()


def test_export_run_trend_file_exists(tmp_path, snapshot, trends):
    export_run(snapshot, trends, tmp_path)
    assert (tmp_path / "trend_pipe1.json").exists()


def test_export_run_creates_output_dir(tmp_path, snapshot, trends):
    out = tmp_path / "exports" / "run1"
    export_run(snapshot, trends, out)
    assert out.is_dir()


def test_export_summary_line_no_files():
    assert export_summary_line([]) == "No files exported."


def test_export_summary_line_counts(tmp_path):
    paths = [tmp_path / "a.json", tmp_path / "b.json"]
    line = export_summary_line(paths)
    assert "2 file(s)" in line


def test_export_summary_line_includes_names(tmp_path):
    paths = [tmp_path / "snapshot.json"]
    line = export_summary_line(paths)
    assert "snapshot.json" in line
