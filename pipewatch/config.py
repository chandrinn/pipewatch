"""Configuration loader for pipewatch alerting thresholds and pipeline settings."""

import os
import yaml
from dataclasses import dataclass, field
from typing import Dict, Optional


DEFAULT_CONFIG_PATH = os.path.expanduser("~/.pipewatch/config.yaml")


@dataclass
class ThresholdConfig:
    max_duration_seconds: float = 300.0
    max_error_rate: float = 0.05
    min_throughput_rps: float = 0.0
    max_lag_seconds: float = 60.0


@dataclass
class PipelineConfig:
    name: str
    source: str
    thresholds: ThresholdConfig = field(default_factory=ThresholdConfig)
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class AppConfig:
    pipelines: Dict[str, PipelineConfig] = field(default_factory=dict)
    refresh_interval_seconds: int = 10
    log_level: str = "INFO"


def load_config(path: Optional[str] = None) -> AppConfig:
    """Load configuration from a YAML file.

    Args:
        path: Path to the config file. Defaults to ~/.pipewatch/config.yaml.

    Returns:
        Populated AppConfig instance.

    Raises:
        FileNotFoundError: If the config file does not exist.
        ValueError: If the config file is malformed.
    """
    config_path = path or DEFAULT_CONFIG_PATH

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r") as fh:
        raw = yaml.safe_load(fh)

    if not isinstance(raw, dict):
        raise ValueError("Config file must contain a YAML mapping at the top level.")

    pipelines: Dict[str, PipelineConfig] = {}
    for name, pipeline_raw in raw.get("pipelines", {}).items():
        if "source" not in pipeline_raw:
            raise ValueError(f"Pipeline '{name}' is missing required field 'source'.")
        threshold_raw = pipeline_raw.get("thresholds", {})
        thresholds = ThresholdConfig(**{k: v for k, v in threshold_raw.items() if k in ThresholdConfig.__dataclass_fields__})
        pipelines[name] = PipelineConfig(
            name=name,
            source=pipeline_raw["source"],
            thresholds=thresholds,
            tags=pipeline_raw.get("tags", {}),
        )

    return AppConfig(
        pipelines=pipelines,
        refresh_interval_seconds=raw.get("refresh_interval_seconds", 10),
        log_level=raw.get("log_level", "INFO"),
    )
