"""Tests for pipewatch.cli_summary module."""

import argparse
import pytest
from unittest.mock import patch, MagicMock

from pipewatch.cli_summary import add_summary_subparser, run_summary
from pipewatch.metrics import MetricEvaluation, PipelineMetrics
from datetime import datetime


def _make_eval(name: str, healthy: bool = True) -> MetricEvaluation:
    metrics = PipelineMetrics(
        pipeline_name=name,
        rows_processed=500,
        duration_seconds=20.0,
        started_at=datetime(2024, 1, 1),
        finished_at=datetime(2024, 1, 1),
    )
    return MetricEvaluation(metrics=metrics, healthy=healthy, violations=[])


def test_add_summary_subparser_registers_command():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    add_summary_subparser(subparsers)
    args = parser.parse_args(["summary", "--config", "test.yaml"])
    assert args.config == "test.yaml"


def test_add_summary_subparser_default_config():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    add_summary_subparser(subparsers)
    args = parser.parse_args(["summary"])
    assert args.config == "pipewatch.yaml"


def test_add_summary_subparser_has_func():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    add_summary_subparser(subparsers)
    args = parser.parse_args(["summary"])
    assert hasattr(args, "func")
    assert args.func is run_summary


def _make_args(**kwargs):
    defaults = {
        "config": "pipewatch.yaml",
        "names": None,
        "status": None,
        "no_trend": True,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


@patch("pipewatch.cli_summary.load_config", side_effect=FileNotFoundError("missing"))
def test_run_summary_bad_config_returns_1(mock_load):
    args = _make_args()
    result = run_summary(args)
    assert result == 1


@patch("pipewatch.cli_summary.load_config")
@patch("pipewatch.cli_summary.fetch_all_metrics")
@patch("pipewatch.cli_summary.evaluate_metrics")
@patch("pipewatch.cli_summary.filter_evaluations")
@patch("pipewatch.cli_summary.build_overall_summary")
@patch("pipewatch.cli_summary.format_summary_table", return_value="table output")
def test_run_summary_prints_table(
    mock_fmt, mock_build, mock_filter, mock_eval, mock_fetch, mock_cfg, capsys
):
    mock_cfg.return_value = MagicMock(pipelines={"p1": MagicMock()})
    metrics = MagicMock(pipeline_name="p1")
    mock_fetch.return_value = [metrics]
    ev = _make_eval("p1")
    mock_eval.return_value = ev
    mock_filter.return_value = [ev]
    mock_build.return_value = MagicMock()

    args = _make_args(no_trend=True)
    result = run_summary(args)

    captured = capsys.readouterr()
    assert "table output" in captured.out
    assert result == 0


@patch("pipewatch.cli_summary.load_config")
@patch("pipewatch.cli_summary.fetch_all_metrics")
@patch("pipewatch.cli_summary.evaluate_metrics")
@patch("pipewatch.cli_summary.filter_evaluations")
@patch("pipewatch.cli_summary.load_history", return_value=[])
@patch("pipewatch.cli_summary.compute_trend", return_value=None)
@patch("pipewatch.cli_summary.build_overall_summary")
@patch("pipewatch.cli_summary.format_summary_table", return_value="with trend")
def test_run_summary_with_trend_calls_history(
    mock_fmt, mock_build, mock_trend, mock_hist, mock_filter, mock_eval, mock_fetch, mock_cfg
):
    mock_cfg.return_value = MagicMock(pipelines={"p1": MagicMock()})
    metrics = MagicMock(pipeline_name="p1")
    mock_fetch.return_value = [metrics]
    ev = _make_eval("p1")
    mock_eval.return_value = ev
    mock_filter.return_value = [ev]
    mock_build.return_value = MagicMock()

    args = _make_args(no_trend=False)
    run_summary(args)

    mock_hist.assert_called_once_with("p1")
