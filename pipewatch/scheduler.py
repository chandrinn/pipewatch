"""Scheduler module for running pipewatch at configurable intervals."""

import time
import signal
import sys
from dataclasses import dataclass
from typing import Callable, Optional


@dataclass
class SchedulerConfig:
    interval_seconds: int
    max_runs: Optional[int] = None
    on_run: Optional[Callable[[int], None]] = None


@dataclass
class SchedulerResult:
    total_runs: int
    stopped_by_signal: bool
    stopped_by_limit: bool


def _setup_signal_handler(stop_flag: list) -> None:
    """Register SIGINT/SIGTERM handlers to gracefully stop the scheduler."""
    def _handler(signum, frame):
        stop_flag[0] = True

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)


def run_scheduler(config: SchedulerConfig) -> SchedulerResult:
    """Run a callback repeatedly at a fixed interval.

    Args:
        config: SchedulerConfig with interval, optional max_runs, and callback.

    Returns:
        SchedulerResult summarising how and how many times the loop ran.
    """
    stop_flag = [False]
    _setup_signal_handler(stop_flag)

    total_runs = 0
    stopped_by_signal = False
    stopped_by_limit = False

    while not stop_flag[0]:
        total_runs += 1

        if config.on_run is not None:
            config.on_run(total_runs)

        if config.max_runs is not None and total_runs >= config.max_runs:
            stopped_by_limit = True
            break

        if stop_flag[0]:
            stopped_by_signal = True
            break

        try:
            time.sleep(config.interval_seconds)
        except (KeyboardInterrupt, SystemExit):
            stopped_by_signal = True
            break

        if stop_flag[0]:
            stopped_by_signal = True
            break

    return SchedulerResult(
        total_runs=total_runs,
        stopped_by_signal=stopped_by_signal,
        stopped_by_limit=stopped_by_limit,
    )


def format_scheduler_summary(result: SchedulerResult) -> str:
    """Return a human-readable summary line for a completed scheduler run."""
    reason = "limit reached" if result.stopped_by_limit else "signal received" if result.stopped_by_signal else "unknown"
    return f"Scheduler stopped after {result.total_runs} run(s) [{reason}]."
