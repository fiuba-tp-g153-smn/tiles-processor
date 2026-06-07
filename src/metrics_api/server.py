"""FastAPI server for the status metrics API.

Exposes read-only JSON endpoints backed by the metrics database; the UI itself
lives in the separate ``visualizer`` Angular app, which consumes these endpoints
cross-origin (hence CORS). The API opens the shared ``metrics.db`` (and, for the
live view, ``progress_tracker.db`` and RabbitMQ) that the workers write.
"""

import asyncio
import logging
import secrets
from collections import Counter
from collections.abc import Callable
from contextlib import asynccontextmanager
from datetime import datetime, UTC, timedelta
from pathlib import Path

import orjson
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query, Response, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.security import APIKeyHeader

from clients.metrics_repository import MetricsRepository
from clients.progress_tracker import ProgressTracker
from clients.rabbitmq_client import RabbitMQClient
from config import Config
from metrics_api.queue_monitor import QueueDepthMonitor
from metrics_api.schemas import (
    HealthStatus,
    ImportResult,
    JobRecord,
    JobTypeSummary,
    LiveStatus,
    MetricsExport,
    RootStatus,
    ThroughputBucket,
    TimingSeriesPoint,
)
from db.migrate import ensure_migrations

logger = logging.getLogger(__name__)

# Write endpoints authenticate with this header (matches the stack convention).
_API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)

# Origins allowed to call the metrics API cross-origin (the visualizer). The
# read endpoints have no credentials, so "*" is safe; /api/import is an
# unauthenticated write (matches the otherwise-open posture — restrict network
# exposure or add an API key if reachable beyond the backoffice).
_CORS_ORIGINS = ["*"]

# Swagger tag groups (rendered at /docs).
_OPENAPI_TAGS = [
    {"name": "metrics", "description": "Read-only performance-metric aggregations."},
    {
        "name": "backup",
        "description": "Full export and idempotent import of job metrics.",
    },
    {"name": "meta", "description": "Service metadata and health checks."},
]


class PollSummaryMiddleware:  # pylint: disable=too-few-public-methods
    """Coalesce a burst of near-simultaneous requests into one log line.

    The dashboard polls several endpoints at once; rather than emitting one access
    log per request (Uvicorn's, disabled here), accumulate the paths and, after a
    short quiet window, emit a single consolidated line. Debounced: each request
    cancels and reschedules the timer, so a whole burst becomes one line. The
    ``/health`` probe is shown like any other request (it arrives alone, so it gets
    its own line); only the bare root ``/`` ping is ignored. All state is touched
    only on the event-loop thread (via ``call_later``), so no locking is needed.
    """

    _IGNORE = frozenset({"/"})

    def __init__(self, app, *, window_s: float = 1.0):
        self.app = app
        self._window_s = window_s
        self._counts: Counter[str] = Counter()
        self._flush_handle: asyncio.TimerHandle | None = None

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        await self.app(scope, receive, send)
        self._record(scope["path"])

    def _record(self, path: str) -> None:
        """Count a served request and (re)arm the debounced flush."""
        if path in self._IGNORE:
            return
        self._counts[path] += 1
        loop = asyncio.get_running_loop()
        if self._flush_handle is not None:
            self._flush_handle.cancel()
        self._flush_handle = loop.call_later(self._window_s, self._emit)

    def _emit(self) -> None:
        """Log one consolidated line for the burst and reset the counters."""
        self._flush_handle = None
        if not self._counts:
            return
        total = sum(self._counts.values())
        paths = ", ".join(self._counts)  # insertion (arrival) order
        self._counts.clear()
        logger.info("served %d request(s): %s", total, paths)


def _since_from_hours(hours: int | None) -> str | None:
    """Convert a lookback window in hours to an ISO8601 cutoff (None = all time)."""
    if not hours or hours <= 0:
        return None
    return (datetime.now(UTC) - timedelta(hours=hours)).isoformat()


def _read_in_progress(tracker: ProgressTracker | None) -> list[dict]:
    """Read the workers' in-progress jobs (best-effort, never raises)."""
    if tracker is None:
        return []
    try:
        return tracker.list_in_progress()
    except Exception:  # pylint: disable=broad-exception-caught
        logger.warning("Could not read in-progress jobs", exc_info=True)
        return []


def _make_rabbitmq_connector(config: Config) -> Callable[[], RabbitMQClient]:
    """Build a fast-fail factory that connects a fresh RabbitMQ client.

    Uses a single quick attempt (not ``create_rabbitmq_client``, which retries
    for ~45s) so the metrics API never blocks /api/live when the broker is down.
    """

    def connect() -> RabbitMQClient:
        client = RabbitMQClient(
            host=config.RABBITMQ_HOST,
            port=config.RABBITMQ_PORT,
            username=config.RABBITMQ_USER,
            password=config.RABBITMQ_PASSWORD,
            queue_name=config.RABBITMQ_QUEUE,
            dlq_name=config.RABBITMQ_DLQ,
            dlx_name=config.RABBITMQ_DLX,
        )
        client.connect(max_retries=1, retry_delay=0)
        return client

    return connect


def create_app(config: Config) -> FastAPI:  # pylint: disable=too-many-locals
    """Build the FastAPI app wired to the metrics repository."""
    repo = MetricsRepository(Path(config.METRICS_DB_PATH))

    # Built once and reused by the live view (every /api/live), rather than
    # reconstructed per request. Degrades to None if the file can't be opened.
    try:
        progress_tracker: ProgressTracker | None = ProgressTracker(
            Path(config.TMP_DIR) / "progress_tracker.db"
        )
    except Exception:  # pylint: disable=broad-exception-caught
        logger.warning("Could not open progress tracker", exc_info=True)
        progress_tracker = None

    # One persistent, reused RabbitMQ connection for queue-depth probes (instead
    # of opening/closing one per /api/live poll). Connects lazily on first use.
    queue_monitor = QueueDepthMonitor(
        _make_rabbitmq_connector(config), config.RABBITMQ_QUEUE, config.RABBITMQ_DLQ
    )

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        yield
        queue_monitor.close()

    app = FastAPI(
        title="tiles-processor metrics API",
        description=(
            "Performance metrics for the tiles-processor pipeline (read-only "
            "aggregations that back the visualizer dashboard) plus a full "
            "export/import for backup and copy-between-environments."
        ),
        version="1.0.0",
        openapi_tags=_OPENAPI_TAGS,
        lifespan=lifespan,
    )

    # The visualizer browser app calls this API from a different origin.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_CORS_ORIGINS,
        allow_methods=["GET", "POST"],
        allow_headers=["*"],
    )

    # Compress large JSON bodies (the all-time export gzips ~10x); only kicks in
    # above the size threshold so small responses are untouched.
    app.add_middleware(GZipMiddleware, minimum_size=1024)

    # Uvicorn's per-request access log is disabled (run_metrics_api); instead emit
    # one consolidated line per dashboard poll (the ~6 simultaneous requests).
    app.add_middleware(PollSummaryMiddleware)

    def require_api_key(provided: str | None = Security(_API_KEY_HEADER)) -> None:
        """Authenticate a write request via the ``X-API-Key`` header.

        Fail-closed: if no key is configured the endpoint is disabled (503);
        otherwise a missing/incorrect key is rejected (401, constant-time compare).
        """
        expected = config.METRICS_API_KEY
        if not expected:
            raise HTTPException(
                status_code=503,
                detail="Writes are disabled: METRICS_API_KEY is not configured",
            )
        if not provided or not secrets.compare_digest(provided, expected):
            raise HTTPException(status_code=401, detail="Invalid or missing API key")

    _hours = Query(
        None,
        ge=0,
        description="Lookback window in hours; 0 or absent = all time, 24 = 24h, 168 = 7d.",
        examples=[24],
    )

    @app.get(
        "/",
        response_model=RootStatus,
        tags=["meta"],
        summary="Service status",
    )
    def root() -> dict:
        return {"status": "ok", "service": "tiles-processor-metrics"}

    @app.get(
        "/health",
        response_model=HealthStatus,
        tags=["meta"],
        summary="Liveness probe",
    )
    def health() -> dict:
        return {"status": "ok"}

    @app.get(
        "/api/summary",
        response_model=list[JobTypeSummary],
        tags=["metrics"],
        summary="Aggregated statistics per job type",
        description=(
            "Counts by outcome, error rate, and avg/min/max/p95 timings per "
            "job type (timings over successful jobs)."
        ),
    )
    def api_summary(hours: int | None = _hours) -> list[dict]:
        return repo.summary(_since_from_hours(hours))

    @app.get(
        "/api/jobs",
        response_model=list[JobRecord],
        tags=["metrics"],
        summary="Recent finished jobs (paginated)",
        description=(
            "Finished jobs newest-first, with limit/offset paging, optional "
            "type/outcome filters and an optional `hours` lookback window."
        ),
    )
    def api_jobs(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        limit: int = Query(
            50,
            ge=0,
            le=1000,
            description="Max rows to return; 0 = sin límite (todas las del rango).",
            examples=[50],
        ),
        offset: int = Query(
            0, ge=0, description="Rows to skip (paging).", examples=[0]
        ),
        job_type: str | None = Query(
            None,
            alias="type",
            description="Filter by job type.",
            examples=["goes19_abi_band_13"],
        ),
        outcome: str | None = Query(
            None, description="Filter by outcome.", examples=["dlq"]
        ),
        hours: int | None = _hours,
        since: str | None = Query(
            None,
            description="ISO8601 lower bound (finished_at >= since); overrides `hours`.",
            examples=["2026-06-01T00:00:00+00:00"],
        ),
        before: str | None = Query(
            None,
            description="ISO8601 upper bound (finished_at < before); for windowed loads.",
            examples=["2026-06-08T00:00:00+00:00"],
        ),
    ) -> list[dict]:
        return repo.recent_jobs(
            limit=limit,
            offset=offset,
            job_type=job_type,
            outcome=outcome,
            since=since or _since_from_hours(hours),
            before=before,
        )

    @app.get(
        "/api/throughput",
        response_model=list[ThroughputBucket],
        tags=["metrics"],
        summary="Finished-job counts per time bucket",
        description="Count of finished jobs per (time bucket, job_type).",
    )
    def api_throughput(
        bucket: str = Query(
            "hour", description="Bucket width: hour, day or 10min.", examples=["hour"]
        ),
        hours: int | None = _hours,
    ) -> list[dict]:
        return repo.throughput(bucket=bucket, since=_since_from_hours(hours))

    @app.get(
        "/api/timeseries",
        response_model=list[TimingSeriesPoint],
        tags=["metrics"],
        summary="Per-bucket timing series (successful jobs)",
        description="Avg/p95 total seconds and per-stage averages per (bucket, job_type).",
    )
    def api_timeseries(
        bucket: str = Query(
            "hour", description="Bucket width: hour, day or 10min.", examples=["hour"]
        ),
        hours: int | None = _hours,
    ) -> list[dict]:
        return repo.timing_series(bucket=bucket, since=_since_from_hours(hours))

    @app.get(
        "/api/live",
        response_model=LiveStatus,
        tags=["metrics"],
        summary="Live queue depths and in-progress jobs",
        description=(
            "Real-time RabbitMQ queue depths (null when the broker is unreachable) "
            "and the jobs currently queued/processing."
        ),
    )
    def api_live() -> dict:
        return {
            "queues": queue_monitor.depths(),
            "in_progress": _read_in_progress(progress_tracker),
        }

    @app.get(
        "/api/export",
        responses={200: {"model": MetricsExport, "description": "The metrics dump."}},
        tags=["backup"],
        summary="Export all metrics (optionally windowed)",
        description=(
            "Full dump of job records (optionally limited to the last `hours`), "
            "tagged with the database's Alembic schema `version`. Feed the result "
            "straight to POST /api/import."
        ),
    )
    def api_export(hours: int | None = _hours) -> Response:
        # Serialized straight to bytes with orjson (no response_model) so a large
        # all-time export skips building one Pydantic model per row and a second
        # encode pass; the payload is already JSON-native. Schema documented above;
        # GZipMiddleware compresses the body on the wire.
        jobs = repo.export_jobs(_since_from_hours(hours))
        payload = {
            "version": repo.schema_version(),
            "exported_at": datetime.now(UTC).isoformat(),
            "window_hours": hours,
            "count": len(jobs),
            "jobs": jobs,
        }
        return Response(orjson.dumps(payload), media_type="application/json")

    @app.post(
        "/api/import",
        response_model=ImportResult,
        tags=["backup"],
        summary="Import metrics (idempotent)",
        description=(
            "Insert exported job records, skipping duplicates (keyed on "
            "work_unit_id + image_id + finished_at + outcome) so re-importing the "
            "same export is a no-op. Rejects a payload whose `version` does not "
            "match this database's schema revision. **Requires** the `X-API-Key` header."
        ),
        dependencies=[Depends(require_api_key)],
        responses={
            401: {
                "description": "Missing or invalid X-API-Key header.",
                "content": {
                    "application/json": {
                        "example": {"detail": "Invalid or missing API key"}
                    }
                },
            },
            409: {
                "description": "Schema version mismatch — payload from a different schema.",
                "content": {
                    "application/json": {
                        "example": {
                            "detail": "Export version 'metrics_0001' does not "
                            "match database schema 'metrics_0002'"
                        }
                    }
                },
            },
            422: {"description": "Malformed payload (validation error)."},
            503: {
                "description": "Writes disabled — METRICS_API_KEY is not configured.",
                "content": {
                    "application/json": {
                        "example": {
                            "detail": "Writes are disabled: METRICS_API_KEY is not configured"
                        }
                    }
                },
            },
        },
    )
    def api_import(payload: MetricsExport) -> dict:
        current = repo.schema_version()
        if payload.version != current:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Export version {payload.version!r} does not match database "
                    f"schema {current!r}"
                ),
            )
        result = repo.import_jobs([job.model_dump() for job in payload.jobs])
        return {"version": current, **result}

    return app


def run_metrics_api(config: Config) -> None:
    """Run the metrics API web server (blocking)."""
    # Apply DB migrations before opening the (Alembic-owned) databases. Kept out
    # of create_app so the app stays test-constructible without migrations.
    ensure_migrations(config)

    app = create_app(config)
    logger.info("Starting metrics API on port %d", config.METRICS_API_PORT)
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=config.METRICS_API_PORT,
        log_level=config.LOG_LEVEL.lower(),
        # Per-request access logs are replaced by PollSummaryMiddleware's
        # consolidated per-poll line (the /health probe is still shown there).
        access_log=False,
    )
