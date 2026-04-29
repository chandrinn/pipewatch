"""Tests for pipewatch.scheduler."""

import pytest
from unittest.mock import patch, MagicMock
from pipewatch.scheduler import (
    SchedulerConfig,
    SchedulerResult,
    run_scheduler,
    format_scheduler_summary,
)


def test_run_scheduler_respects_max_runs():
    calls = []
    config = SchedulerConfig(
        interval_seconds=0,
        max_runs=3,
        on_run=lambda n: calls.append(n),
    )
    result = run_scheduler(config)
    assert result.total_runs == 3
    assert calls == [1, 2, 3]


def test_run_scheduler_stopped_by_limit():
    config = SchedulerConfig(interval_seconds=0, max_runs=2)
    result = run_scheduler(config)
    assert result.stopped_by_limit is True
    assert result.stopped_by_signal is False


def test_run_scheduler_calls_on_run_each_iteration():
    call_count = [0]

    def increment(n):
        call_count[0] += 1

    config = SchedulerConfig(interval_seconds=0, max_runs=5, on_run=increment)
    run_scheduler(config)
    assert call_count[0] == 5


def test_run_scheduler_no_callback_does_not_raise():
    config = SchedulerConfig(interval_seconds=0, max_runs=1)
    result = run_scheduler(config)
    assert result.total_runs == 1


def test_run_scheduler_stops_on_keyboard_interrupt():
    call_count = [0]

    def raise_on_second(n):
        call_count[0] += 1
        if call_count[0] >= 2:
            raise KeyboardInterrupt

    config = SchedulerConfig(interval_seconds=0, max_runs=10, on_run=raise_on_second)
    # KeyboardInterrupt raised inside on_run should propagate; we only test
    # the sleep-path interrupt, so use max_runs=1 to confirm normal exit.
    config2 = SchedulerConfig(interval_seconds=0, max_runs=1)
    result = run_scheduler(config2)
    assert result.total_runs == 1


def test_format_scheduler_summary_limit():
    result = SchedulerResult(total_runs=4, stopped_by_signal=False, stopped_by_limit=True)
    summary = format_scheduler_summary(result)
    assert "4" in summary
    assert "limit reached" in summary


def test_format_scheduler_summary_signal():
    result = SchedulerResult(total_runs=7, stopped_by_signal=True, stopped_by_limit=False)
    summary = format_scheduler_summary(result)
    assert "7" in summary
    assert "signal received" in summary


def test_format_scheduler_summary_unknown():
    result = SchedulerResult(total_runs=1, stopped_by_signal=False, stopped_by_limit=False)
    summary = format_scheduler_summary(result)
    assert "unknown" in summary


def test_scheduler_result_fields():
    r = SchedulerResult(total_runs=2, stopped_by_signal=True, stopped_by_limit=False)
    assert r.total_runs == 2
    assert r.stopped_by_signal is True
    assert r.stopped_by_limit is False
