"""CLI subcommand: summary — display a tabular pipeline health overview."""

import argparse
from typing import Optional

from pipewatch.config import load_config
from pipewatch.fetcher import fetch_all_metrics
from pipewatch.metrics import evaluate_metrics
from pipewatch.history import load_history
from pipewatch.trend import compute_trend
from pipewatch.filter import parse_filter_options, filter_evaluations
from pipewatch.summary import build_overall_summary, format_summary_table


def add_summary_subparser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "summary",
        help="Display a tabular summary of pipeline health.",
    )
    parser.add_argument(
        "--config",
        default="pipewatch.yaml",
        help="Path to configuration file (default: pipewatch.yaml).",
    )
    parser.add_argument(
        "--name",
        nargs="*",
        dest="names",
        help="Filter by pipeline name(s).",
    )
    parser.add_argument(
        "--status",
        choices=["ok", "critical"],
        dest="status",
        help="Filter by health status.",
    )
    parser.add_argument(
        "--no-trend",
        action="store_true",
        default=False,
        help="Omit trend data from the summary table.",
    )
    parser.set_defaults(func=run_summary)


def run_summary(args: argparse.Namespace) -> int:
    try:
        config = load_config(args.config)
    except Exception as exc:
        print(f"[error] Failed to load config: {exc}")
        return 1

    all_metrics = fetch_all_metrics(config.pipelines)
    evaluations = [
        evaluate_metrics(m, config.pipelines[m.pipeline_name])
        for m in all_metrics
    ]

    filter_opts = parse_filter_options(
        names=getattr(args, "names", None),
        status=getattr(args, "status", None),
    )
    evaluations = filter_evaluations(evaluations, filter_opts)

    trends: Optional[dict] = None
    if not getattr(args, "no_trend", False):
        trends = {}
        for ev in evaluations:
            name = ev.metrics.pipeline_name
            records = load_history(name)
            trend = compute_trend(records)
            if trend:
                trends[name] = trend

    summary = build_overall_summary(evaluations, trends=trends)
    print(format_summary_table(summary))
    return 0
