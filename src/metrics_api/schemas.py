"""Pydantic models for the metrics API.

These power the OpenAPI/Swagger schema (typed responses + examples) and the
import request validation. They mirror the JSON the repository already returns
1:1, so attaching them as ``response_model`` does not change any response body.
"""

from typing import Any

from pydantic import BaseModel, Field

# Shared, realistic example fragments reused across model examples. Typed as
# dict[str, Any] so they satisfy Pydantic's JsonDict config type for mypy.
_STAGES_EXAMPLE: dict[str, Any] = {"georef": 3.2, "tiling": 19.3, "upload": 4.1}
_JOB_EXAMPLE: dict[str, Any] = {
    "id": 1287,
    "work_unit_id": "wu-9f2a",
    "image_id": "OR_ABI-L1b-RadF-M6C13_G19_s20260041200.nc",
    "data_source_id": "goes19_abi_band_13",
    "processor_id": "goes_band_13",
    "band_id": "band_13",
    "job_type": "goes19_abi_band_13",
    "product_label": "GOES ABI band_13 · Cloud Tops",
    "image_timestamp": "20260041200",
    "outcome": "success",
    "worker_host": "worker1",
    "started_at": "2026-06-04T12:00:00+00:00",
    "finished_at": "2026-06-04T12:00:44+00:00",
    "retry_count": 0,
    "error_message": None,
    "download_s": 1.84,
    "process_s": 42.1,
    "total_s": 44.31,
    "stage_timings": _STAGES_EXAMPLE,
}


class StatBlock(BaseModel):
    """avg/min/max/p95 over successful jobs (``null`` when there are no samples)."""

    avg: float | None = None
    min: float | None = None
    max: float | None = None
    p95: float | None = None


class OutcomeCounts(BaseModel):
    """Job counts per terminal outcome, plus the grand total."""

    success: int = 0
    error: int = 0
    dlq: int = 0
    requeued: int = 0
    skipped: int = 0
    total: int = 0


class JobTypeSummary(BaseModel):
    """Aggregated statistics for one ``job_type`` (``GET /api/summary``)."""

    job_type: str
    product_label: str | None = None
    counts: OutcomeCounts
    error_rate: float
    last_finished: str
    total_s: StatBlock
    download_s: StatBlock
    process_s: StatBlock
    stages: dict[str, float] = Field(default_factory=dict)

    model_config = {
        "json_schema_extra": {
            "example": {
                "job_type": "goes19_abi_band_13",
                "product_label": "GOES ABI band_13 · Cloud Tops",
                "counts": {
                    "success": 40,
                    "error": 1,
                    "dlq": 0,
                    "requeued": 0,
                    "skipped": 0,
                    "total": 41,
                },
                "error_rate": 0.024,
                "last_finished": "2026-06-04T12:00:44+00:00",
                "total_s": {"avg": 44.3, "min": 38.0, "max": 121.0, "p95": 110.0},
                "download_s": {"avg": 1.8, "min": 1.2, "max": 3.0, "p95": 2.7},
                "process_s": {"avg": 42.5, "min": 36.0, "max": 118.0, "p95": 107.0},
                "stages": _STAGES_EXAMPLE,
            }
        }
    }


class JobRecord(BaseModel):
    """One finished job (a ``job_metrics`` row) — ``/api/jobs`` and exports."""

    id: int | None = None
    work_unit_id: str | None = None
    image_id: str
    data_source_id: str
    processor_id: str | None = None
    band_id: str | None = None
    job_type: str
    product_label: str | None = None
    image_timestamp: str | None = None
    outcome: str
    worker_host: str | None = None
    started_at: str
    finished_at: str
    retry_count: int = 0
    error_message: str | None = None
    download_s: float | None = None
    process_s: float | None = None
    total_s: float | None = None
    stage_timings: dict[str, float] = Field(default_factory=dict)

    model_config = {"json_schema_extra": {"example": _JOB_EXAMPLE}}


class ThroughputBucket(BaseModel):
    """Finished-job count for one (time bucket, job_type) (``/api/throughput``)."""

    bucket: str
    job_type: str
    count: int

    model_config = {
        "json_schema_extra": {
            "example": {
                "bucket": "2026-06-04T12",
                "job_type": "goes19_abi_band_13",
                "count": 6,
            }
        }
    }


class TimingSeriesPoint(BaseModel):
    """Per (bucket, job_type) timing over successful jobs (``/api/timeseries``)."""

    bucket: str
    job_type: str
    count: int
    avg_total_s: float | None = None
    p95_total_s: float | None = None
    stages: dict[str, float] = Field(default_factory=dict)

    model_config = {
        "json_schema_extra": {
            "example": {
                "bucket": "2026-06-04T12",
                "job_type": "goes19_abi_band_13",
                "count": 6,
                "avg_total_s": 44.3,
                "p95_total_s": 110.0,
                "stages": _STAGES_EXAMPLE,
            }
        }
    }


class QueueDepths(BaseModel):
    """RabbitMQ queue depths; ``null`` when the broker is unreachable."""

    work: int | None = None
    light: int | None = None
    dlq: int | None = None


class InProgressJob(BaseModel):
    """A job currently queued/processing (progress tracker)."""

    image_id: str
    band_id: str
    status: str
    created_at: str
    updated_at: str


class LiveStatus(BaseModel):
    """Real-time queue depths + in-progress jobs (``GET /api/live``)."""

    queues: QueueDepths
    in_progress: list[InProgressJob]

    model_config = {
        "json_schema_extra": {
            "example": {
                "queues": {"work": 3, "light": 5, "dlq": 0},
                "in_progress": [
                    {
                        "image_id": "RMA12_DBZH_20260114T170328Z",
                        "band_id": "radar",
                        "status": "PROCESSING",
                        "created_at": "2026-06-04T12:01:00+00:00",
                        "updated_at": "2026-06-04T12:01:05+00:00",
                    }
                ],
            }
        }
    }


class HealthStatus(BaseModel):
    """Liveness probe payload (``GET /health``)."""

    status: str

    model_config = {"json_schema_extra": {"example": {"status": "ok"}}}


class RootStatus(BaseModel):
    """Service identity + liveness (``GET /``)."""

    status: str
    service: str

    model_config = {
        "json_schema_extra": {
            "example": {"status": "ok", "service": "tiles-processor-metrics"}
        }
    }


class MetricsExport(BaseModel):
    """A portable dump of metrics — the body of both export and import.

    ``version`` is the source database's Alembic schema revision; an import
    refuses a payload whose version does not match the target database.
    """

    version: str | None = Field(
        None, description="Alembic schema revision of the source database"
    )
    exported_at: str = Field(description="ISO8601 UTC timestamp of the export")
    window_hours: int | None = Field(
        None, description="Lookback window of the export (null = all time)"
    )
    count: int = Field(description="Number of job records in this export")
    jobs: list[JobRecord]

    model_config = {
        "json_schema_extra": {
            "example": {
                "version": "metrics_0001",
                "exported_at": "2026-06-05T10:00:00+00:00",
                "window_hours": 24,
                "count": 1,
                "jobs": [_JOB_EXAMPLE],
            }
        }
    }


class ImportResult(BaseModel):
    """Outcome of an import (``POST /api/import``)."""

    version: str | None = None
    inserted: int
    skipped: int

    model_config = {
        "json_schema_extra": {
            "example": {"version": "metrics_0001", "inserted": 120, "skipped": 4}
        }
    }
