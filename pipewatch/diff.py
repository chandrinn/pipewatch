"""Snapshot diffing: compare two snapshots and highlight changes in pipeline health."""

from dataclasses import dataclass
from typing import Optional

from pipewatch.snapshot import Snapshot


@dataclass
class PipelineDiff:
    name: str
    previous_status: str
    current_status: str
    changed: bool

    @property
    def improved(self) -> bool:
        return self.previous_status != "ok" and self.current_status == "ok"

    @property
    def degraded(self) -> bool:
        return self.previous_status == "ok" and self.current_status != "ok"


@dataclass
class SnapshotDiff:
    previous_timestamp: str
    current_timestamp: str
    pipelines: list

    @property
    def changed_count(self) -> int:
        return sum(1 for p in self.pipelines if p.changed)

    @property
    def degraded_count(self) -> int:
        return sum(1 for p in self.pipelines if p.degraded)

    @property
    def improved_count(self) -> int:
        return sum(1 for p in self.pipelines if p.improved)


def _status_for(name: str, snapshot: Snapshot) -> Optional[str]:
    for ev in snapshot.evaluations:
        if ev.pipeline_name == name:
            return "ok" if ev.is_healthy else "critical"
    return None


def diff_snapshots(previous: Snapshot, current: Snapshot) -> SnapshotDiff:
    """Compare two snapshots and return a SnapshotDiff describing changes."""
    all_names = set()
    for ev in previous.evaluations:
        all_names.add(ev.pipeline_name)
    for ev in current.evaluations:
        all_names.add(ev.pipeline_name)

    diffs = []
    for name in sorted(all_names):
        prev_status = _status_for(name, previous) or "unknown"
        curr_status = _status_for(name, current) or "unknown"
        diffs.append(PipelineDiff(
            name=name,
            previous_status=prev_status,
            current_status=curr_status,
            changed=prev_status != curr_status,
        ))

    return SnapshotDiff(
        previous_timestamp=previous.timestamp,
        current_timestamp=current.timestamp,
        pipelines=diffs,
    )


def format_diff(diff: SnapshotDiff) -> str:
    """Return a human-readable string summarising the snapshot diff."""
    lines = [
        f"Diff: {diff.previous_timestamp} → {diff.current_timestamp}",
        f"  Changed: {diff.changed_count}  Degraded: {diff.degraded_count}  Improved: {diff.improved_count}",
    ]
    for p in diff.pipelines:
        if p.changed:
            arrow = "↓" if p.degraded else "↑"
            lines.append(f"  {arrow} {p.name}: {p.previous_status} → {p.current_status}")
    if diff.changed_count == 0:
        lines.append("  No changes detected.")
    return "\n".join(lines)
