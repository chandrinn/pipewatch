"""History module for persisting and retrieving pipeline metric snapshots."""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from pipewatch.metrics import MetricEvaluation, PipelineMetrics

DEFAULT_HISTORY_DIR = Path.home() / ".pipewatch" / "history"


def _history_path(pipeline_name: str, history_dir: Path) -> Path:
    """Return the JSON file path for a given pipeline's history."""
    safe_name = pipeline_name.replace(" ", "_").replace("/", "_")
    return history_dir / f"{safe_name}.json"


def save_evaluation(evaluation: MetricEvaluation, history_dir: Optional[Path] = None) -> Path:
    """Persist a MetricEvaluation snapshot to disk.

    Returns the path where the record was written.
    """
    directory = history_dir or DEFAULT_HISTORY_DIR
    directory.mkdir(parents=True, exist_ok=True)

    path = _history_path(evaluation.pipeline_name, directory)

    records: List[dict] = []
    if path.exists():
        with open(path, "r") as fh:
            records = json.load(fh)

    records.append({
        "timestamp": datetime.utcnow().isoformat(),
        "pipeline_name": evaluation.pipeline_name,
        "healthy": evaluation.healthy,
        "violations": evaluation.violations,
        "rows_processed": evaluation.metrics.rows_processed,
        "duration_seconds": evaluation.metrics.duration_seconds,
        "error_count": evaluation.metrics.error_count,
    })

    with open(path, "w") as fh:
        json.dump(records, fh, indent=2)

    return path


def load_history(pipeline_name: str, history_dir: Optional[Path] = None, limit: int = 50) -> List[dict]:
    """Load the most recent *limit* snapshots for a pipeline.

    Returns an empty list when no history file exists.
    """
    directory = history_dir or DEFAULT_HISTORY_DIR
    path = _history_path(pipeline_name, directory)

    if not path.exists():
        return []

    with open(path, "r") as fh:
        records: List[dict] = json.load(fh)

    return records[-limit:]


def clear_history(pipeline_name: str, history_dir: Optional[Path] = None) -> bool:
    """Delete the history file for *pipeline_name*.

    Returns True if a file was removed, False if nothing existed.
    """
    directory = history_dir or DEFAULT_HISTORY_DIR
    path = _history_path(pipeline_name, directory)
    if path.exists():
        os.remove(path)
        return True
    return False
