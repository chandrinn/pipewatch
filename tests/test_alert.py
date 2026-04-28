"""Tests for pipewatch.alert module."""

import pytest

from pipewatch.alert import (
    Alert,
    _severity_from_evaluation,
    build_alert,
    format_alert,
    process_alerts,
)
from pipewatch.metrics import MetricEvaluation


@pytest.fixture
def healthy_evaluation():
    return MetricEvaluation(
        pipeline_name="pipeline_a",
        is_healthy=True,
        violations=[],
    )


@pytest.fixture
def single_violation_evaluation():
    return MetricEvaluation(
        pipeline_name="pipeline_b",
        is_healthy=False,
        violations=["Duration exceeded threshold"],
    )


@pytest.fixture
def multi_violation_evaluation():
    return MetricEvaluation(
        pipeline_name="pipeline_c",
        is_healthy=False,
        violations=["Duration exceeded threshold", "Row count below minimum"],
    )


def test_severity_healthy_returns_none(healthy_evaluation):
    assert _severity_from_evaluation(healthy_evaluation) is None


def test_severity_single_violation_is_warning(single_violation_evaluation):
    assert _severity_from_evaluation(single_violation_evaluation) == "warning"


def test_severity_multi_violation_is_critical(multi_violation_evaluation):
    assert _severity_from_evaluation(multi_violation_evaluation) == "critical"


def test_build_alert_healthy_returns_none(healthy_evaluation):
    assert build_alert(healthy_evaluation) is None


def test_build_alert_returns_alert_instance(single_violation_evaluation):
    alert = build_alert(single_violation_evaluation)
    assert isinstance(alert, Alert)


def test_build_alert_pipeline_name(single_violation_evaluation):
    alert = build_alert(single_violation_evaluation)
    assert alert.pipeline_name == "pipeline_b"


def test_build_alert_violations_preserved(multi_violation_evaluation):
    alert = build_alert(multi_violation_evaluation)
    assert len(alert.violations) == 2


def test_format_alert_contains_severity(single_violation_evaluation):
    alert = build_alert(single_violation_evaluation)
    output = format_alert(alert)
    assert "WARNING" in output


def test_format_alert_contains_pipeline_name(multi_violation_evaluation):
    alert = build_alert(multi_violation_evaluation)
    output = format_alert(alert)
    assert "pipeline_c" in output


def test_process_alerts_filters_healthy(healthy_evaluation, single_violation_evaluation):
    alerts = process_alerts([healthy_evaluation, single_violation_evaluation])
    assert len(alerts) == 1
    assert alerts[0].pipeline_name == "pipeline_b"


def test_process_alerts_empty_list():
    assert process_alerts([]) == []
