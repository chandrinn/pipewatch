"""CLI sub-command: compare two saved snapshots."""

import argparse
import sys
from pathlib import Path

from pipewatch.compare import build_compare_report, format_compare_report
from pipewatch.snapshot import load_snapshot


def add_compare_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    parser = subparsers.add_parser(
        "compare",
        help="Compare two snapshots and show what changed.",
    )
    parser.add_argument("previous", type=str, help="Path or ID of the previous snapshot file.")
    parser.add_argument("current", type=str, help="Path or ID of the current snapshot file.")
    parser.add_argument(
        "--degraded-only",
        action="store_true",
        default=False,
        help="Only show pipelines that degraded.",
    )
    parser.set_defaults(func=run_compare)


def run_compare(args: argparse.Namespace) -> None:
    previous_path = Path(args.previous)
    current_path = Path(args.current)

    if not previous_path.exists():
        print(f"[compare] ERROR: previous snapshot not found: {previous_path}", file=sys.stderr)
        sys.exit(1)

    if not current_path.exists():
        print(f"[compare] ERROR: current snapshot not found: {current_path}", file=sys.stderr)
        sys.exit(1)

    previous = load_snapshot(previous_path)
    current = load_snapshot(current_path)

    if previous is None:
        print(f"[compare] ERROR: could not load previous snapshot: {previous_path}", file=sys.stderr)
        sys.exit(1)

    if current is None:
        print(f"[compare] ERROR: could not load current snapshot: {current_path}", file=sys.stderr)
        sys.exit(1)

    report = build_compare_report(previous, current)

    if args.degraded_only:
        filtered = [ln for ln in report.lines if "🔴" in ln or "violation" in ln or "comparison" in ln or "Pipelines:" in ln]
        print("\n".join(filtered))
    else:
        print(format_compare_report(report))

    if report.degraded > 0:
        sys.exit(2)
