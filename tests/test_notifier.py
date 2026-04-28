"""Tests for pipewatch.notifier module."""

import pytest

from pipewatch.alert import Alert
from pipewatch.notifier import NotifierConfig, NotifyResult, notify, notify_all


@pytest.fixture
def sample_alert():
    return Alert(
        pipeline_name="pipeline_x",
        severity="warning",
        message="1 violation detected.",
        triggered_at="2024-01-01T00:00:00",
        violations=["Duration exceeded threshold"],
    )


def test_notify_console_sets_flag(sample_alert, capsys):
    config = NotifierConfig(console=True, log_file=None)
    result = notify(sample_alert, config)
    assert result.sent_to_console is True


def test_notify_console_prints_output(sample_alert, capsys):
    config = NotifierConfig(console=True, log_file=None)
    notify(sample_alert, config)
    captured = capsys.readouterr()
    assert "pipeline_x" in captured.out


def test_notify_no_console_skips_print(sample_alert, capsys):
    config = NotifierConfig(console=False, log_file=None)
    result = notify(sample_alert, config)
    assert result.sent_to_console is False
    captured = capsys.readouterr()
    assert captured.out == ""


def test_notify_log_file_creates_file(sample_alert, tmp_path):
    log_path = str(tmp_path / "alerts.log")
    config = NotifierConfig(console=False, log_file=log_path)
    result = notify(sample_alert, config)
    assert result.logged_to_file is True
    assert result.log_path == log_path


def test_notify_log_file_content(sample_alert, tmp_path):
    log_path = str(tmp_path / "alerts.log")
    config = NotifierConfig(console=False, log_file=log_path)
    notify(sample_alert, config)
    content = open(log_path).read()
    assert "pipeline_x" in content


def test_notify_log_file_appends(sample_alert, tmp_path):
    log_path = str(tmp_path / "alerts.log")
    config = NotifierConfig(console=False, log_file=log_path)
    notify(sample_alert, config)
    notify(sample_alert, config)
    content = open(log_path).read()
    assert content.count("pipeline_x") == 2


def test_notify_all_returns_results(sample_alert):
    config = NotifierConfig(console=False)
    results = notify_all([sample_alert, sample_alert], config)
    assert len(results) == 2
    assert all(isinstance(r, NotifyResult) for r in results)


def test_notify_all_empty(capsys):
    config = NotifierConfig(console=True)
    results = notify_all([], config)
    assert results == []
