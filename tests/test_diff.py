"""Tests for pipewatch.diff — snapshot diffing."""

import pytest
from unittest.mock import MagicMock

from pipewatch.diff import (
    PipelineDiff,
    SnapshotDiff,
    diff_snapshots,
    format_diff,
)


def _make_evaluation(name: str, healthy: bool):
    ev = MagicMock()
    ev.pipeline_name = name
    ev.is_healthy = healthy
    return ev


def _make_snapshot(timestamp: str, evaluations):
    snap = MagicMock()
    snap.timestamp = timestamp
    snap.evaluations = evaluations
    return snap


@pytest.fixture
def previous_snapshot():
    return _make_snapshot("2024-01-01T10:00:00", [
        _make_evaluation("orders", True),
        _make_evaluation("users", True),
    ])


@pytest.fixture
def current_snapshot():
    return _make_snapshot("2024-01-01T11:00:00", [
        _make_evaluation("orders", False),
        _make_evaluation("users", True),
    ])


def test_diff_snapshots_returns_snapshot_diff(previous_snapshot, current_snapshot):
    result = diff_snapshots(previous_snapshot, current_snapshot)
    assert isinstance(result, SnapshotDiff)


def test_diff_snapshots_timestamps(previous_snapshot, current_snapshot):
    result = diff_snapshots(previous_snapshot, current_snapshot)
    assert result.previous_timestamp == "2024-01-01T10:00:00"
    assert result.current_timestamp == "2024-01-01T11:00:00"


def test_diff_snapshots_pipeline_count(previous_snapshot, current_snapshot):
    result = diff_snapshots(previous_snapshot, current_snapshot)
    assert len(result.pipelines) == 2


def test_diff_snapshots_changed_count(previous_snapshot, current_snapshot):
    result = diff_snapshots(previous_snapshot, current_snapshot)
    assert result.changed_count == 1


def test_diff_snapshots_degraded_count(previous_snapshot, current_snapshot):
    result = diff_snapshots(previous_snapshot, current_snapshot)
    assert result.degraded_count == 1


def test_diff_snapshots_improved_count(previous_snapshot, current_snapshot):
    result = diff_snapshots(previous_snapshot, current_snapshot)
    assert result.improved_count == 0


def test_pipeline_diff_degraded_flag(previous_snapshot, current_snapshot):
    result = diff_snapshots(previous_snapshot, current_snapshot)
    orders_diff = next(p for p in result.pipelines if p.name == "orders")
    assert orders_diff.degraded is True
    assert orders_diff.improved is False


def test_pipeline_diff_no_change(previous_snapshot, current_snapshot):
    result = diff_snapshots(previous_snapshot, current_snapshot)
    users_diff = next(p for p in result.pipelines if p.name == "users")
    assert users_diff.changed is False


def test_format_diff_contains_timestamps(previous_snapshot, current_snapshot):
    result = diff_snapshots(previous_snapshot, current_snapshot)
    output = format_diff(result)
    assert "2024-01-01T10:00:00" in output
    assert "2024-01-01T11:00:00" in output


def test_format_diff_shows_changed_pipeline(previous_snapshot, current_snapshot):
    result = diff_snapshots(previous_snapshot, current_snapshot)
    output = format_diff(result)
    assert "orders" in output


def test_format_diff_no_changes_message():
    snap = _make_snapshot("2024-01-01T10:00:00", [_make_evaluation("orders", True)])
    snap2 = _make_snapshot("2024-01-01T11:00:00", [_make_evaluation("orders", True)])
    result = diff_snapshots(snap, snap2)
    output = format_diff(result)
    assert "No changes detected" in output
