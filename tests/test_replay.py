"""Tests for pipewatch.replay module."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from pipewatch.replay import (
    ReplayOptions,
    ReplayResult,
    filter_records,
    replay_history,
    _record_to_evaluation,
)


SAMPLE_RECORD = {
    "pipeline_name": "orders",
    "row_count": 500,
    "duration_seconds": 10.0,
    "start_time": "2024-01-01T00:00:00",
    "end_time": "2024-01-01T00:00:10",
    "healthy": True,
    "violations": [],
    "timestamp": "2024-01-01T00:00:10",
}

VIOLATION_RECORD = {
    **SAMPLE_RECORD,
    "pipeline_name": "users",
    "healthy": False,
    "violations": ["row_count below minimum"],
}


@pytest.fixture()
def history_dir(tmp_path: Path) -> str:
    records = [SAMPLE_RECORD, VIOLATION_RECORD, {**SAMPLE_RECORD, "pipeline_name": "orders"}]
    history = tmp_path / "history"
    history.mkdir()
    for i, rec in enumerate(records):
        (history / f"eval_{i}.json").write_text(json.dumps(rec))
    return str(history)


def test_record_to_evaluation_healthy():
    ev = _record_to_evaluation(SAMPLE_RECORD)
    assert ev.healthy is True
    assert ev.metrics.pipeline_name == "orders"


def test_record_to_evaluation_violations():
    ev = _record_to_evaluation(VIOLATION_RECORD)
    assert ev.healthy is False
    assert len(ev.violations) == 1


def test_filter_no_options_returns_all():
    records = [SAMPLE_RECORD, VIOLATION_RECORD]
    result = filter_records(records, ReplayOptions())
    assert len(result) == 2


def test_filter_by_pipeline_name():
    records = [SAMPLE_RECORD, VIOLATION_RECORD]
    result = filter_records(records, ReplayOptions(pipeline_name="orders"))
    assert all(r["pipeline_name"] == "orders" for r in result)


def test_filter_violations_only():
    records = [SAMPLE_RECORD, VIOLATION_RECORD]
    result = filter_records(records, ReplayOptions(only_violations=True))
    assert len(result) == 1
    assert result[0]["healthy"] is False


def test_filter_last_n():
    records = [SAMPLE_RECORD, VIOLATION_RECORD, SAMPLE_RECORD]
    result = filter_records(records, ReplayOptions(last_n=2))
    assert len(result) == 2


def test_replay_history_returns_result(history_dir: str):
    result = replay_history(ReplayOptions(), history_dir=history_dir)
    assert isinstance(result, ReplayResult)
    assert result.records_total == 3
    assert result.records_shown == 3


def test_replay_history_filter_pipeline(history_dir: str):
    result = replay_history(ReplayOptions(pipeline_name="orders"), history_dir=history_dir)
    assert result.records_shown == 2


def test_replay_history_empty_match(history_dir: str):
    result = replay_history(ReplayOptions(pipeline_name="nonexistent"), history_dir=history_dir)
    assert result.records_shown == 0
