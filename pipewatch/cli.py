"""CLI entry point for pipewatch."""

import argparse
import sys
from pathlib import Path

from pipewatch.config import load_config
from pipewatch.fetcher import fetch_all_metrics
from pipewatch.metrics import evaluate_metrics
from pipewatch.reporter import print_report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipewatch",
        description="Monitor and visualize ETL pipeline health.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("pipewatch.yaml"),
        help="Path to the YAML configuration file (default: pipewatch.yaml)",
    )
    parser.add_argument(
        "--pipeline",
        metavar="NAME",
        help="Only check the named pipeline instead of all configured pipelines.",
    )
    parser.add_argument(
        "--exit-code",
        action="store_true",
        default=False,
        help="Exit with code 1 if any pipeline is unhealthy.",
    )
    return parser


def run(argv: list[str] | None = None) -> int:
    """Main entry point; returns an exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        app_config = load_config(args.config)
    except FileNotFoundError:
        print(f"[error] Config file not found: {args.config}", file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001
        print(f"[error] Failed to load config: {exc}", file=sys.stderr)
        return 2

    pipelines = app_config.pipelines
    if args.pipeline:
        pipelines = [p for p in pipelines if p.name == args.pipeline]
        if not pipelines:
            print(f"[error] No pipeline named '{args.pipeline}' found.", file=sys.stderr)
            return 2

    all_metrics = fetch_all_metrics(pipelines)
    evaluations = [
        evaluate_metrics(m, cfg)
        for m, cfg in zip(all_metrics, pipelines)
    ]

    print_report(evaluations)

    if args.exit_code and any(not ev.healthy for ev in evaluations):
        return 1
    return 0


def main() -> None:  # pragma: no cover
    sys.exit(run())


if __name__ == "__main__":  # pragma: no cover
    main()
