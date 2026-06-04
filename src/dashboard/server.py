"""FastAPI server for the backoffice performance dashboard.

Serves a single raw-HTML page plus JSON endpoints backed by the metrics
database. The dashboard is read-only: it opens the shared ``metrics.db`` (and,
for the live view, ``progress_tracker.db`` and RabbitMQ) that the workers write.
"""

import logging
from datetime import datetime, UTC, timedelta
from pathlib import Path

import uvicorn
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse

from clients.metrics_repository import MetricsRepository
from config import Config

logger = logging.getLogger(__name__)

_TEMPLATES = Path(__file__).parent / "templates"


def _since_from_hours(hours: int | None) -> str | None:
    """Convert a lookback window in hours to an ISO8601 cutoff (None = all time)."""
    if not hours or hours <= 0:
        return None
    return (datetime.now(UTC) - timedelta(hours=hours)).isoformat()


def _in_progress(config: Config) -> list[dict]:
    """Read the workers' in-progress jobs (best-effort, never raises)."""
    # Imported lazily so the module stays importable without the heavy clients.
    from clients.progress_tracker import (  # pylint: disable=import-outside-toplevel
        ProgressTracker,
    )

    try:
        tracker = ProgressTracker(Path(config.TMP_DIR) / "progress_tracker.db")
        return tracker.list_in_progress()
    except Exception:  # pylint: disable=broad-exception-caught
        logger.warning("Could not read in-progress jobs", exc_info=True)
        return []


def _queue_depths(config: Config) -> dict:
    """Probe RabbitMQ queue depths via a short-lived passive declare.

    Degrades to ``None`` counts when RabbitMQ is unreachable so the dashboard
    shows "n/a" rather than erroring.
    """
    from clients.rabbitmq_client import (  # pylint: disable=import-outside-toplevel
        RabbitMQClient,
    )

    client = RabbitMQClient(
        host=config.RABBITMQ_HOST,
        port=config.RABBITMQ_PORT,
        username=config.RABBITMQ_USER,
        password=config.RABBITMQ_PASSWORD,
        queue_name=config.RABBITMQ_QUEUE,
        dlq_name=config.RABBITMQ_DLQ,
        dlx_name=config.RABBITMQ_DLX,
    )
    try:
        client.connect(max_retries=1, retry_delay=0)
        return {
            "work": client.get_queue_size(config.RABBITMQ_QUEUE),
            "dlq": client.get_queue_size(config.RABBITMQ_DLQ),
        }
    except Exception:  # pylint: disable=broad-exception-caught
        logger.warning("Queue depth probe failed", exc_info=True)
        return {"work": None, "dlq": None}
    finally:
        try:
            client.close()
        except Exception:  # pylint: disable=broad-exception-caught
            pass


def create_app(config: Config) -> FastAPI:
    """Build the FastAPI app wired to the metrics repository."""
    repo = MetricsRepository(Path(config.METRICS_DB_PATH))
    index_html = (_TEMPLATES / "index.html").read_text(encoding="utf-8")

    app = FastAPI(title="tiles-processor metrics", docs_url=None, redoc_url=None)

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        return index_html

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.get("/api/summary")
    def api_summary(hours: int | None = Query(None, ge=0)) -> list[dict]:
        return repo.summary(_since_from_hours(hours))

    @app.get("/api/jobs")
    def api_jobs(
        limit: int = Query(100, ge=1, le=1000),
        job_type: str | None = Query(None, alias="type"),
        outcome: str | None = Query(None),
    ) -> list[dict]:
        return repo.recent_jobs(limit=limit, job_type=job_type, outcome=outcome)

    @app.get("/api/throughput")
    def api_throughput(
        bucket: str = Query("hour"),
        hours: int | None = Query(None, ge=0),
    ) -> list[dict]:
        return repo.throughput(bucket=bucket, since=_since_from_hours(hours))

    @app.get("/api/live")
    def api_live() -> dict:
        return {
            "queues": _queue_depths(config),
            "in_progress": _in_progress(config),
        }

    return app


def run_dashboard(config: Config) -> None:
    """Run the dashboard web server (blocking)."""
    app = create_app(config)
    logger.info("Starting dashboard on port %d", config.DASHBOARD_PORT)
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=config.DASHBOARD_PORT,
        log_level=config.LOG_LEVEL.lower(),
    )
