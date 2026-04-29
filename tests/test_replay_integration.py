"""Integration tests: replay reads real history files and formats output."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.replay import ReplayOptions, replay_history


RECORDS = [
    {
        "pipeline_name": "orders",
        "row_count": 1000,
        "duration_seconds": 8.0,
        "start_time": "2024-06-01T10:00:00",
        "end_time": "2024-06-01T10:00:08",
        "healthy": True,
        "violations": [],
        "timestamp": "2024-06-01T10:00:08",
    },
    {
        "pipeline_name": "orders",
        "row_count": 50,
        "duration_seconds": 120.0,
        "start_time": "2024-06-01T11:00:00",
        "end_time": "2024-06-01T11:02:00",
        "healthy": False,
        "violations": ["duration exceeded", "row_count below minimum"],
        "timestamp": "2024-06-01T11:02:00",
    },
    {
        "pipeline_name": "users",
        "row_count": 300,
        "duration_seconds": 4.0,
        "start_time": "2024-06-01T10:05:00",
        "end_time": "2024-06-01T10:05:04",
        "healthy": True,
        "violations": [],
        "timestamp": "2024-06-01T10:05:04",
    },
]


@pytest.fixture()
def history_dir(tmp_path: Path) -> str:
    hist = tmp_path / "history"
    hist.mkdir()
    for i, rec in enumerate(RECORDS):
        (hist / f"eval_{i}.json").write_text(json.dumps(rec))
    return str(hist)


def test_replay_all_records(history_dir: str, capsys):
    result = replay_history(ReplayOptions(), history_dir=history_dir)
    assert result.records_total == 3
    assert result.records_shown == 3


def test_replay_output_contains_pipeline_name(history_dir: str, capsys):
    replay_history(ReplayOptions(pipeline_name="orders"), history_dir=history_dir)
    captured = capsys.readouterr()
    assert "orders" in captured.out


def test_replay_violations_only_skips_healthy(history_dir: str):
    result = replay_history(
        ReplayOptions(only_violations=True), history_dir=history_dir
    )
    assert result.records_shown == 1


def test_replay_last_n_limits_output(history_dir: str):
    result = replay_history(ReplayOptions(last_n=1), history_dir=history_dir)
    assert result.records_shown == 1


def test_replay_combined_filters(history_dir: str):
    result = replay_history(
        ReplayOptions(pipeline_name="orders", only_violations=True),
        history_dir=history_dir,
    )
    assert result.records_shown == 1
