"""Compare two snapshots and produce a human-readable diff report."""

from dataclasses import dataclass
from typing import Optional

from pipewatch.diff import SnapshotDiff, diff_snapshots
from pipewatch.snapshot import Snapshot


@dataclass
class CompareReport:
    previous_id: str
    current_id: str
    total_pipelines: int
    improved: int
    degraded: int
    unchanged: int
    lines: list[str]


def _change_icon(status: str) -> str:
    return {"improved": "✅", "degraded": "🔴", "unchanged": "➖"}.get(status, "❓")


def build_compare_report(previous: Snapshot, current: Snapshot) -> CompareReport:
    """Diff two snapshots and return a structured compare report."""
    sdiff: SnapshotDiff = diff_snapshots(previous, current)

    lines: list[str] = []
    lines.append(f"Snapshot comparison: {previous.snapshot_id} → {current.snapshot_id}")
    lines.append(
        f"  Pipelines: {sdiff.total} total, "
        f"{sdiff.improved_count} improved, "
        f"{sdiff.degraded_count} degraded, "
        f"{sdiff.unchanged_count} unchanged"
    )
    lines.append("")

    for pdiff in sdiff.pipeline_diffs:
        icon = _change_icon(pdiff.change_type)
        lines.append(f"  {icon} {pdiff.pipeline_name}: {pdiff.previous_status} → {pdiff.current_status}")
        if pdiff.new_violations:
            for v in pdiff.new_violations:
                lines.append(f"       + violation: {v}")
        if pdiff.resolved_violations:
            for v in pdiff.resolved_violations:
                lines.append(f"       - resolved:  {v}")

    return CompareReport(
        previous_id=previous.snapshot_id,
        current_id=current.snapshot_id,
        total_pipelines=sdiff.total,
        improved=sdiff.improved_count,
        degraded=sdiff.degraded_count,
        unchanged=sdiff.unchanged_count,
        lines=lines,
    )


def format_compare_report(report: CompareReport) -> str:
    """Return a printable string for the compare report."""
    return "\n".join(report.lines)
