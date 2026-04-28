"""Snapshot module: capture and persist a point-in-time view of all pipeline evaluations."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from pipewatch.metrics import MetricEvaluation


_DEFAULT_SNAPSHOT_DIR = Path.home() / ".pipewatch" / "snapshots"


@dataclass
class Snapshot:
    captured_at: str
    pipeline_count: int
    healthy_count: int
    unhealthy_count: int
    pipelines: List[dict]


def _snapshot_path(snapshot_dir: Optional[Path] = None) -> Path:
    directory = Path(snapshot_dir) if snapshot_dir else _DEFAULT_SNAPSHOT_DIR
    directory.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return directory / f"snapshot_{timestamp}.json"


def build_snapshot(evaluations: List[MetricEvaluation]) -> Snapshot:
    """Build a Snapshot dataclass from a list of MetricEvaluation objects."""
    pipelines = []
    for ev in evaluations:
        pipelines.append({
            "pipeline": ev.pipeline_name,
            "healthy": ev.healthy,
            "violations": ev.violations,
            "rows_processed": ev.metrics.rows_processed,
            "duration_seconds": ev.metrics.duration_seconds,
        })

    healthy = sum(1 for ev in evaluations if ev.healthy)
    return Snapshot(
        captured_at=datetime.now(timezone.utc).isoformat(),
        pipeline_count=len(evaluations),
        healthy_count=healthy,
        unhealthy_count=len(evaluations) - healthy,
        pipelines=pipelines,
    )


def save_snapshot(
    snapshot: Snapshot,
    snapshot_dir: Optional[Path] = None,
) -> Path:
    """Persist a Snapshot to a timestamped JSON file and return the path."""
    path = _snapshot_path(snapshot_dir)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(asdict(snapshot), fh, indent=2)
    return path


def load_snapshot(path: Path) -> Snapshot:
    """Load a Snapshot from a JSON file."""
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    return Snapshot(**data)


def format_snapshot(snapshot: Snapshot) -> str:
    """Return a human-readable summary string for a Snapshot."""
    lines = [
        f"Snapshot captured at : {snapshot.captured_at}",
        f"Total pipelines      : {snapshot.pipeline_count}",
        f"Healthy              : {snapshot.healthy_count}",
        f"Unhealthy            : {snapshot.unhealthy_count}",
    ]
    for p in snapshot.pipelines:
        icon = "\u2705" if p["healthy"] else "\u274c"
        lines.append(f"  {icon} {p['pipeline']} — rows: {p['rows_processed']}, duration: {p['duration_seconds']:.1f}s")
    return "\n".join(lines)
