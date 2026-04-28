"""CLI sub-command: ``pipewatch export``.

Exports the latest snapshot and trend data to disk.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pipewatch.config import load_config
from pipewatch.export_runner import export_run, export_summary_line
from pipewatch.history import load_history
from pipewatch.snapshot import load_snapshot
from pipewatch.trend import compute_trend


def add_export_subparser(subparsers: argparse._SubParsersAction) -> None:  # noqa: SLF001
    parser = subparsers.add_parser(
        "export",
        help="Export the latest snapshot and trend data.",
    )
    parser.add_argument(
        "--output-dir",
        default="pipewatch_export",
        help="Directory to write exported files (default: pipewatch_export).",
    )
    parser.add_argument(
        "--format",
        choices=["json", "csv"],
        default="json",
        help="Output format for the snapshot (default: json).",
    )
    parser.add_argument(
        "--config",
        default="pipewatch.yaml",
        help="Path to config file.",
    )
    parser.set_defaults(func=run_export)


def run_export(args: argparse.Namespace) -> int:
    """Execute the export sub-command. Returns exit code."""
    config = load_config(Path(args.config))
    output_dir = Path(args.output_dir)

    snapshot = load_snapshot(config)
    if snapshot is None:
        print("No snapshot found. Run 'pipewatch run' first.")
        return 1

    trends = []
    for pipeline in config.pipelines:
        records = load_history(config, pipeline.name)
        trend = compute_trend(pipeline.name, records)
        if trend is not None:
            trends.append(trend)

    written = export_run(snapshot, trends, output_dir, fmt=args.format)
    print(export_summary_line(written))
    return 0
