"""High-level helpers to export all pipeline data from a run."""

from __future__ import annotations

from pathlib import Path

from pipewatch.export import export_snapshot_json, export_snapshot_csv, export_trend_json
from pipewatch.snapshot import Snapshot
from pipewatch.trend import TrendSummary


def export_run(
    snapshot: Snapshot,
    trends: list[TrendSummary],
    output_dir: Path,
    fmt: str = "json",
) -> list[Path]:
    """Export snapshot and all trend summaries to *output_dir*.

    Parameters
    ----------
    snapshot:   The current run snapshot.
    trends:     Per-pipeline trend summaries.
    output_dir: Directory where files will be written.
    fmt:        ``"json"`` or ``"csv"`` (snapshot only; trends are always JSON).

    Returns
    -------
    List of paths that were written.
    """
    written: list[Path] = []

    if fmt == "csv":
        snap_path = output_dir / "snapshot.csv"
        export_snapshot_csv(snapshot, snap_path)
    else:
        snap_path = output_dir / "snapshot.json"
        export_snapshot_json(snapshot, snap_path)
    written.append(snap_path)

    for trend in trends:
        trend_path = output_dir / f"trend_{trend.pipeline_name}.json"
        export_trend_json(trend, trend_path)
        written.append(trend_path)

    return written


def export_summary_line(written: list[Path]) -> str:
    """Return a human-readable summary of exported files."""
    if not written:
        return "No files exported."
    return f"Exported {len(written)} file(s): " + ", ".join(p.name for p in written)
