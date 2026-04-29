"""CLI subcommand: replay — replay historical pipeline evaluations."""

from __future__ import annotations

import argparse
from typing import Optional

from pipewatch.replay import ReplayOptions, ReplayResult, replay_history


def add_replay_subparser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    """Register the 'replay' subcommand."""
    parser = subparsers.add_parser(
        "replay",
        help="Replay historical pipeline evaluation records.",
    )
    parser.add_argument(
        "--pipeline",
        default=None,
        help="Filter by pipeline name.",
    )
    parser.add_argument(
        "--last",
        type=int,
        default=None,
        metavar="N",
        help="Show only the last N records.",
    )
    parser.add_argument(
        "--violations-only",
        action="store_true",
        default=False,
        help="Show only records with threshold violations.",
    )
    parser.add_argument(
        "--history-dir",
        default=None,
        help="Path to history directory (default: ~/.pipewatch/history).",
    )
    parser.set_defaults(func=run_replay)


def run_replay(args: argparse.Namespace) -> None:
    """Entry point for the replay subcommand."""
    options = ReplayOptions(
        pipeline_name=args.pipeline,
        last_n=args.last,
        only_violations=args.violations_only,
    )
    history_dir: Optional[str] = getattr(args, "history_dir", None)
    result: ReplayResult = replay_history(options, history_dir=history_dir)

    if result.records_shown == 0:
        print("No records matched the given filters.")
    else:
        print(
            f"\n--- Replayed {result.records_shown} of {result.records_total} record(s) ---"
        )
