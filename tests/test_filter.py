"""Tests for pipewatch.filter module."""

import pytest
from pipewatch.filter import (
    FilterOptions,
    filter_evaluations,
    parse_filter_options,
)
from pipewatch.metrics import MetricEvaluation


def _make_eval(name: str, healthy: bool, violations=None) -> MetricEvaluation:
    return MetricEvaluation(
        pipeline_name=name,
        healthy=healthy,
        violations=violations or [],
    )


@pytest.fixture
def evaluations():
    return [
        _make_eval("pipeline_a", True),
        _make_eval("pipeline_b", False, violations=["duration exceeded"]),
        _make_eval("pipeline_c", True),
        _make_eval("pipeline_d", False, violations=["row count too low"]),
    ]


def test_filter_no_options_returns_all(evaluations):
    opts = FilterOptions()
    result = filter_evaluations(evaluations, opts)
    assert len(result) == 4


def test_filter_by_name_single(evaluations):
    opts = FilterOptions(names=["pipeline_a"])
    result = filter_evaluations(evaluations, opts)
    assert len(result) == 1
    assert result[0].pipeline_name == "pipeline_a"


def test_filter_by_name_multiple(evaluations):
    opts = FilterOptions(names=["pipeline_a", "pipeline_c"])
    result = filter_evaluations(evaluations, opts)
    assert {ev.pipeline_name for ev in result} == {"pipeline_a", "pipeline_c"}


def test_filter_by_name_nonexistent_returns_empty(evaluations):
    opts = FilterOptions(names=["pipeline_z"])
    result = filter_evaluations(evaluations, opts)
    assert result == []


def test_filter_status_ok(evaluations):
    opts = FilterOptions(status="ok")
    result = filter_evaluations(evaluations, opts)
    assert all(ev.healthy for ev in result)
    assert len(result) == 2


def test_filter_status_critical(evaluations):
    opts = FilterOptions(status="critical")
    result = filter_evaluations(evaluations, opts)
    assert all(not ev.healthy for ev in result)
    assert len(result) == 2


def test_filter_name_and_status_combined(evaluations):
    opts = FilterOptions(names=["pipeline_a", "pipeline_b"], status="ok")
    result = filter_evaluations(evaluations, opts)
    assert len(result) == 1
    assert result[0].pipeline_name == "pipeline_a"


def test_parse_filter_options_no_args():
    opts = parse_filter_options(None, None)
    assert opts.names is None
    assert opts.status is None


def test_parse_filter_options_names_csv():
    opts = parse_filter_options("pipeline_a, pipeline_b", None)
    assert opts.names == ["pipeline_a", "pipeline_b"]


def test_parse_filter_options_status_ok():
    opts = parse_filter_options(None, "ok")
    assert opts.status == "ok"


def test_parse_filter_options_status_critical():
    opts = parse_filter_options(None, "CRITICAL")
    assert opts.status == "critical"


def test_parse_filter_options_invalid_status_ignored():
    opts = parse_filter_options(None, "warning")
    assert opts.status is None
