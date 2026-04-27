"""Pipeline metrics collection and evaluation."""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from pipewatch.config import PipelineConfig


@dataclass
class PipelineMetrics:
    """Holds runtime metrics for a single pipeline run."""
    pipeline_name: str
    row_count: int
    duration_seconds: float
    error_count: int
    timestamp: datetime = field(default_factory=datetime.utcnow)
    source: Optional[str] = None

    @property
    def rows_per_second(self) -> float:
        if self.duration_seconds <= 0:
            return 0.0
        return self.row_count / self.duration_seconds


@dataclass
class MetricEvaluation:
    """Result of evaluating metrics against configured thresholds."""
    pipeline_name: str
    status: str  # "ok", "warning", "critical"
    violations: list[str] = field(default_factory=list)

    @property
    def is_healthy(self) -> bool:
        return self.status == "ok"


def evaluate_metrics(
    metrics: PipelineMetrics,
    config: PipelineConfig,
) -> MetricEvaluation:
    """Evaluate pipeline metrics against threshold configuration."""
    violations: list[str] = []
    thresholds = config.thresholds

    if thresholds.max_duration_seconds is not None:
        if metrics.duration_seconds > thresholds.max_duration_seconds:
            violations.append(
                f"duration {metrics.duration_seconds:.1f}s exceeds max "
                f"{thresholds.max_duration_seconds}s"
            )

    if thresholds.min_row_count is not None:
        if metrics.row_count < thresholds.min_row_count:
            violations.append(
                f"row count {metrics.row_count} below min {thresholds.min_row_count}"
            )

    if thresholds.max_error_count is not None:
        if metrics.error_count > thresholds.max_error_count:
            violations.append(
                f"error count {metrics.error_count} exceeds max "
                f"{thresholds.max_error_count}"
            )

    if violations:
        status = "critical"
    else:
        status = "ok"

    return MetricEvaluation(
        pipeline_name=metrics.pipeline_name,
        status=status,
        violations=violations,
    )
