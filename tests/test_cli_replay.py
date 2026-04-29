"""Tests for pipewatch.cli_replay module."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from pipewatch.cli_replay import add_replay_subparser, run_replay


SAMPLE_RECORD = {
    "pipeline_name": "orders",
    "row_count": 100,
    "duration_seconds": 5.0,
    "start_time": "2024-01-01T00:00:00",
    "end_time": "2024-01-01T00:00:05",
    "healthy": True,
    "violations": [],
    "timestamp": "2024-01-01T00:00:05",
}


def _make_args(**kwargs) -> argparse.Namespace:
    defaults = {
        "pipeline": None,
        "last": None,
        "violations_only": False,
        "history_dir": None,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def test_add_replay_subparser_registers_command():
    parser = argparse.ArgumentParser()
    subs = parser.add_subparsers()
    add_replay_subparser(subs)
    args = parser.parse_args(["replay"])
    assert hasattr(args, "func")


def test_add_replay_subparser_has_func():
    parser = argparse.ArgumentParser()
    subs = parser.add_subparsers()
    add_replay_subparser(subs)
    args = parser.parse_args(["replay"])
    assert args.func is run_replay


def test_run_replay_no_records_prints_message(capsys):
    with patch("pipewatch.cli_replay.replay_history") as mock_replay:
        from pipewatch.replay import ReplayResult
        mock_replay.return_value = ReplayResult(
            records_shown=0, records_total=5, pipeline_name=None
        )
        run_replay(_make_args())
    captured = capsys.readouterr()
    assert "No records matched" in captured.out


def test_run_replay_with_records_prints_summary(capsys):
    with patch("pipewatch.cli_replay.replay_history") as mock_replay:
        from pipewatch.replay import ReplayResult
        mock_replay.return_value = ReplayResult(
            records_shown=3, records_total=3, pipeline_name=None
        )
        run_replay(_make_args())
    captured = capsys.readouterr()
    assert "3 of 3" in captured.out


def test_run_replay_passes_options():
    with patch("pipewatch.cli_replay.replay_history") as mock_replay:
        from pipewatch.replay import ReplayResult
        mock_replay.return_value = ReplayResult(
            records_shown=1, records_total=1, pipeline_name="orders"
        )
        run_replay(_make_args(pipeline="orders", last=5, violations_only=True))
        call_options = mock_replay.call_args[0][0]
        assert call_options.pipeline_name == "orders"
        assert call_options.last_n == 5
        assert call_options.only_violations is True
