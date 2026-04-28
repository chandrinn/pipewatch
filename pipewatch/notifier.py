"""Notifier module: outputs alerts to configured channels."""

from dataclasses import dataclass, field
from typing import Optional

from pipewatch.alert import Alert, format_alert


@dataclass
class NotifierConfig:
    """Configuration for the notifier."""

    console: bool = True
    log_file: Optional[str] = None


@dataclass
class NotifyResult:
    """Result of a notification dispatch."""

    sent_to_console: bool = False
    logged_to_file: bool = False
    log_path: Optional[str] = None


def _write_alert_to_file(alert: Alert, path: str) -> None:
    """Append a formatted alert to a log file."""
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(format_alert(alert) + "\n" + "-" * 40 + "\n")


def notify(alert: Alert, config: NotifierConfig) -> NotifyResult:
    """Dispatch an alert to all configured channels."""
    result = NotifyResult()

    if config.console:
        print(format_alert(alert))
        result.sent_to_console = True

    if config.log_file:
        _write_alert_to_file(alert, config.log_file)
        result.logged_to_file = True
        result.log_path = config.log_file

    return result


def notify_all(alerts: list[Alert], config: NotifierConfig) -> list[NotifyResult]:
    """Dispatch all alerts using the given notifier config."""
    return [notify(alert, config) for alert in alerts]
