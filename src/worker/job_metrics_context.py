"""Per-message accumulator for job performance metrics.

The worker creates one ``JobMetricsContext`` per consumed message. The handler
fills in timings as the pipeline runs; the worker stamps the terminal outcome.
``build()`` then produces an immutable :class:`JobMetrics` row to persist.

Splitting collection (here) from the single write (worker, in a ``finally``)
keeps the database single-writer-class and guarantees exactly one row per job,
including partial timings when a job fails mid-pipeline.
"""

import socket
from datetime import datetime, UTC
from time import perf_counter

from models.job_metrics import JobMetrics, JobOutcome
from models.work_unit import WorkUnit
from services.job_descriptor import describe_job


class JobMetricsContext:
    """Mutable, single-job timing/outcome accumulator."""

    def __init__(self, work_unit: WorkUnit):
        self._work_unit = work_unit
        self._started_perf = perf_counter()
        self._started_at = datetime.now(UTC).isoformat()
        self._worker_host = socket.gethostname()

        self._outcome: JobOutcome | None = None
        self._error_message: str | None = None
        self._download_s: float | None = None
        self._process_s: float | None = None
        self._stage_timings: dict[str, float] = {}

    def set_download_seconds(self, seconds: float) -> None:
        """Record how long the download (network) phase took."""
        self._download_s = seconds

    def set_process_seconds(self, seconds: float) -> None:
        """Record how long the processing phase took (subprocess or inline)."""
        self._process_s = seconds

    def set_stage_timings(self, timings: dict[str, float]) -> None:
        """Record per-stage durations surfaced from the processor."""
        self._stage_timings = dict(timings)

    def mark_outcome(
        self, outcome: JobOutcome, error_message: str | None = None
    ) -> None:
        """Stamp the terminal disposition of this job."""
        self._outcome = outcome
        self._error_message = error_message

    @property
    def has_outcome(self) -> bool:
        """True once a terminal outcome has been stamped."""
        return self._outcome is not None

    def build(self) -> JobMetrics:
        """Materialize the immutable metrics row.

        Must only be called after :meth:`mark_outcome`.
        """
        if self._outcome is None:
            raise ValueError("Cannot build JobMetrics without an outcome")

        wu = self._work_unit
        description = describe_job(wu.data_source_id, wu.image_id, wu.band_id)
        total_s = perf_counter() - self._started_perf

        return JobMetrics(
            work_unit_id=wu.work_unit_id,
            image_id=wu.image_id,
            data_source_id=wu.data_source_id,
            processor_id=wu.processor_id,
            band_id=wu.band_id,
            job_type=description.job_type,
            product_label=description.product_label,
            image_timestamp=description.image_timestamp,
            outcome=self._outcome.value,
            worker_host=self._worker_host,
            started_at=self._started_at,
            finished_at=datetime.now(UTC).isoformat(),
            retry_count=wu.retry_count,
            error_message=self._error_message,
            download_s=self._download_s,
            process_s=self._process_s,
            total_s=total_s,
            stage_timings=self._stage_timings,
        )
