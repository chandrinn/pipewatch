"""Fetcher module for collecting pipeline metrics from configured sources."""

import random
import time
from datetime import datetime, timedelta
from typing import Optional

from pipewatch.config import PipelineConfig
from pipewatch.metrics import PipelineMetrics


def _simulate_metrics(config: PipelineConfig, seed: Optional[int] = None) -> PipelineMetrics:
    """Generate simulated metrics for a pipeline (used when source is 'mock')."""
    rng = random.Random(seed)
    now = datetime.utcnow()
    duration_seconds = rng.uniform(30, config.thresholds.max_duration_seconds * 1.2)
    rows_processed = rng.randint(
        int(config.thresholds.min_rows * 0.8),
        int(config.thresholds.min_rows * 2),
    )
    error_count = rng.choices([0, rng.randint(1, 5)], weights=[0.85, 0.15])[0]
    started_at = now - timedelta(seconds=duration_seconds)
    return PipelineMetrics(
        pipeline_name=config.name,
        rows_processed=rows_processed,
        duration_seconds=duration_seconds,
        error_count=error_count,
        started_at=started_at,
        finished_at=now,
    )


def fetch_metrics(config: PipelineConfig) -> PipelineMetrics:
    """Fetch metrics for a single pipeline based on its source configuration."""
    source = config.source.lower()

    if source == "mock":
        seed = int(time.time() / 60)  # changes every minute
        return _simulate_metrics(config, seed=seed)

    raise NotImplementedError(
        f"Source '{config.source}' is not supported yet. "
        "Supported sources: mock"
    )


def fetch_all_metrics(configs: list[PipelineConfig]) -> list[PipelineMetrics]:
    """Fetch metrics for all configured pipelines."""
    results = []
    for config in configs:
        try:
            metrics = fetch_metrics(config)
            results.append(metrics)
        except Exception as exc:  # noqa: BLE001
            print(f"[warn] Could not fetch metrics for '{config.name}': {exc}")
    return results
