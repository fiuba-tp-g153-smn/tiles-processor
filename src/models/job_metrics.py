"""Per-job performance metrics model.

One ``JobMetrics`` instance is one persisted row in the metrics database: the
full record of a single work unit that reached a terminal outcome (success,
failure, requeue, DLQ or skip), with timing breakdown.
"""

from dataclasses import dataclass, field
from enum import Enum


class JobOutcome(str, Enum):
    """Terminal disposition of a processed work unit.

    Inherits ``str`` so the value serializes directly to the SQLite TEXT column
    and compares cleanly against query parameters.
    """

    SUCCESS = "success"  # processed and uploaded
    ERROR = "error"  # attempt failed (retried, or terminal — e.g. missing source file)
    DLQ = "dlq"  # attempt failed and max retries exceeded
    REQUEUED = "requeued"  # transient error, copy put back on the queue
    SKIPPED = "skipped"  # nothing to do (e.g. forecast not yet available)


@dataclass(frozen=True, slots=True)
class JobMetrics:  # pylint: disable=too-many-instance-attributes
    """Immutable record of one finished job.

    Attributes mirror the ``job_metrics`` table columns one-to-one so the
    repository can persist without a translation layer.
    """

    work_unit_id: str
    image_id: str
    data_source_id: str
    processor_id: str
    band_id: str
    # Coarse grouping key for aggregation (equal to data_source_id).
    job_type: str
    # Human-friendly label, e.g. "Radar RMA12 DBZH · Horizontal Reflectivity".
    product_label: str
    # Scene/forecast time parsed from image_id (best-effort, may be "").
    image_timestamp: str
    outcome: str
    worker_host: str
    started_at: str  # ISO8601 UTC
    finished_at: str  # ISO8601 UTC
    retry_count: int = 0
    error_message: str | None = None
    download_s: float | None = None
    process_s: float | None = None
    total_s: float | None = None
    # Per-stage durations in seconds (e.g. {"georef": .., "tiling": ..}).
    stage_timings: dict[str, float] = field(default_factory=dict)
