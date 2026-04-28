"""Export pipeline snapshots and trend data to various formats."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from pipewatch.snapshot import Snapshot
from pipewatch.trend import TrendSummary


def _snapshot_to_dict(snapshot: Snapshot) -> dict[str, Any]:
    return {
        "timestamp": snapshot.timestamp,
        "total_pipelines": snapshot.total_pipelines,
        "healthy": snapshot.healthy,
        "degraded": snapshot.degraded,
        "pipelines": [
            {
                "name": e.pipeline_name,
                "healthy": e.healthy,
                "violations": e.violations,
            }
            for e in snapshot.evaluations
        ],
    }


def export_snapshot_json(snapshot: Snapshot, path: Path) -> None:
    """Write a snapshot to a JSON file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    data = _snapshot_to_dict(snapshot)
    path.write_text(json.dumps(data, indent=2))


def export_snapshot_csv(snapshot: Snapshot, path: Path) -> None:
    """Write per-pipeline evaluation rows from a snapshot to a CSV file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["timestamp", "pipeline", "healthy", "violations"]
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for e in snapshot.evaluations:
            writer.writerow(
                {
                    "timestamp": snapshot.timestamp,
                    "pipeline": e.pipeline_name,
                    "healthy": e.healthy,
                    "violations": "|".join(e.violations),
                }
            )


def export_trend_json(trend: TrendSummary, path: Path) -> None:
    """Write a TrendSummary to a JSON file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "pipeline_name": trend.pipeline_name,
        "total_runs": trend.total_runs,
        "healthy_runs": trend.healthy_runs,
        "health_pct": trend.health_pct,
        "last_seen": trend.last_seen,
    }
    path.write_text(json.dumps(data, indent=2))
