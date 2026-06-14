"""Worker implementation for processing work units from RabbitMQ."""

import asyncio
import shutil
from asyncio import AbstractEventLoop, new_event_loop, set_event_loop
from logging import getLogger
from signal import signal, SIGINT, SIGTERM
from pathlib import Path
from typing import Optional

from clients.message_queue_client import MessageQueueClient
from data_sources.ecmwf_producer_source import (
    ForecastNotAvailableError,
    TransientDownloadError,
)
from clients.metrics_repository import MetricsRepository
from clients.progress_tracker import ProgressTracker
from config import Config
from db.migrate import ensure_migrations
from exceptions import UnprocessableInputError
from factories import (
    create_data_source_registry,
    create_rabbitmq_client,
    create_s3_client,
)
from worker.ecmwf_grib_downloader import EcmwfGribDownloader
from worker.inline_processor import InlineProcessor
from worker.job_metrics_context import JobMetricsContext
from models.ecmwf_config import ECMWF_MSLP_CONFIG, ECMWF_TP_CONFIG
from models.job_metrics import JobOutcome
from models.work_unit import WorkUnit
from worker.work_handler import WorkHandler
from health_server import HealthCheckServer

# NOTE: Heavy processors (GoesProcessor, RadarProcessor) are NOT imported here.
# Processing runs in a subprocess to isolate memory from pyproj/rioxarray/GDAL.
# This keeps the main worker process lightweight when idle.

logger = getLogger(__name__)


class Worker:  # pylint: disable=too-few-public-methods
    """
    Worker that consumes work units from RabbitMQ and processes them.

    The worker:
    1. Connects to RabbitMQ and starts consuming from the work queue
    2. Processes each work unit (download + process in single atomic operation)
    3. On success, acknowledges the message
    4. On failure, either retries or sends to dead letter queue

    Error Handling:
        - Transient errors: Retry up to max_retries times
        - Permanent errors: Send to dead letter queue
        - Handler exceptions are caught and logged
    """

    # When every queue is empty (or all concurrency slots are busy), wait this
    # long per drain iteration before polling again — bounds pickup latency
    # while servicing RabbitMQ heartbeats. Negligible for second-to-minute jobs.
    _IDLE_POLL_S = 1.0

    def __init__(
        self,
        config: Config,
        mq_client: MessageQueueClient,
        handler: WorkHandler,
        metrics_repository: Optional[MetricsRepository] = None,
    ):
        self._config = config
        self._mq_client = mq_client
        self._handler = handler
        self._metrics_repository = metrics_repository
        self._running = True
        self._loop: Optional[AbstractEventLoop] = None
        self._health_server: Optional[HealthCheckServer] = None

    def _check_readiness(self) -> tuple[bool, str]:
        """Check if external dependencies are available."""
        # Check RabbitMQ connection
        if not self._mq_client.is_connected:
            return False, "RabbitMQ not connected"
        return True, "Dependencies healthy"

    def start(self) -> None:
        """
        Start the worker.

        This is a blocking call that runs until the worker is stopped.
        Uses a single asyncio event loop for all async operations.
        """
        logger.info("Worker starting...")

        # Create a single event loop for the worker's lifetime
        self._loop = new_event_loop()
        set_event_loop(self._loop)

        # Start health check server
        self._health_server = HealthCheckServer(
            port=self._config.HEALTH_PORT, check_readiness=self._check_readiness
        )
        self._health_server.start()

        # Set up signal handlers for graceful shutdown
        signal(SIGINT, self._signal_handler)
        signal(SIGTERM, self._signal_handler)

        try:
            # Run the bounded-concurrency async drain loop on the worker's loop.
            self._loop.run_until_complete(self._drain())
        except KeyboardInterrupt:
            logger.info("Worker interrupted by user")
        finally:
            self._shutdown()

    def _consume_tiers(self) -> tuple[list[str], list[str]]:
        """Return (strict_queues, round_robin_queues) for this worker type.

        Normal workers drain the normal queue with strict priority, then
        round-robin the two light queues. Light workers have no normal queue —
        they only round-robin the light queues.
        """
        light = [
            self._config.RABBITMQ_RADAR_LIGHT_QUEUE,
            self._config.RABBITMQ_WRF_LIGHT_QUEUE,
        ]
        if self._config.WORKER_TYPE == "light":
            return [], light
        return [self._config.RABBITMQ_QUEUE], light

    def _signal_handler(self, signum, _frame):
        """Handle shutdown signals gracefully."""
        logger.info("Received signal %d, initiating graceful shutdown...", signum)
        self._running = False
        self._handler.abort()
        self._mq_client.stop_consuming()

    def _shutdown(self) -> None:
        """Clean shutdown of the worker."""
        logger.info("Worker shutting down...")
        try:
            self._mq_client.close()
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning("Error closing RabbitMQ connection: %s", e)

        # Stop health server
        if self._health_server:
            self._health_server.stop()

        # Close the event loop
        if self._loop and not self._loop.is_closed():
            self._loop.close()

        logger.info("Worker stopped")

    async def _drain(self) -> None:
        """Bounded-concurrency async consume loop.

        Pulls up to WORKER_CONCURRENCY work units and runs each as a concurrent
        task, so one unit's I/O-bound upload tail overlaps another unit's
        CPU-bound compute. Each iteration services RabbitMQ heartbeats without
        blocking on new messages. Strict-priority then round-robin tiers come
        from the MQ client's poll_one.
        """
        assert self._loop is not None
        strict, round_robin = self._consume_tiers()
        concurrency = self._config.WORKER_CONCURRENCY
        logger.info(
            "Worker draining: strict=%s round-robin=%s concurrency=%d",
            strict,
            round_robin,
            concurrency,
        )
        inflight: set[asyncio.Task] = set()
        while self._running:
            while len(inflight) < concurrency:
                message = self._mq_client.poll_one(strict, round_robin)
                if message is None:
                    break
                inflight.add(
                    self._loop.create_task(self._process_message_async(*message))
                )

            # Keep heartbeats/acks flowing without blocking on new messages.
            self._mq_client.service_events(0.0)

            if inflight:
                done, inflight = await asyncio.wait(
                    inflight,
                    timeout=self._IDLE_POLL_S,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                self._log_task_errors(done)
            else:
                await asyncio.sleep(self._IDLE_POLL_S)

        await self._drain_inflight(inflight)

    @staticmethod
    def _log_task_errors(tasks: "set[asyncio.Task]") -> None:
        """Surface any unexpected task crash (per-unit handling is internal)."""
        for task in tasks:
            exc = task.exception()
            if exc is not None:
                logger.error("Work unit task crashed: %s", exc, exc_info=exc)

    async def _drain_inflight(self, inflight: "set[asyncio.Task]") -> None:
        """Await in-flight units on shutdown; abort() kills their subprocesses."""
        if not inflight:
            return
        logger.info("Awaiting %d in-flight unit(s) before shutdown", len(inflight))
        for result in await asyncio.gather(*inflight, return_exceptions=True):
            if isinstance(result, Exception):
                logger.error("In-flight unit ended with error: %s", result)

    async def _process_message_async(
        self, work_unit: WorkUnit, delivery_tag: int, source_queue: str
    ) -> None:
        """
        Process a single work unit end-to-end, then ack / retry / DLQ.

        Runs as a concurrent task in the drain loop. Acks on success or skip,
        republishes + acks on retry/DLQ, and on shutdown leaves the message for
        redelivery (nack-requeue).

        Args:
            work_unit: The work unit to process
            delivery_tag: Message delivery tag for ack/nack
            source_queue: Queue this message came from. Requeues and retries go
                back here so a light unit stolen by a normal worker returns to
                the light queue, not the worker's primary (normal) queue.
        """
        logger.info("Processing: %s", work_unit)
        collector = JobMetricsContext(work_unit, worker_host=self._config.WORKER_ID)
        try:
            await self._handler.handle(work_unit, collector)
            logger.info("Successfully processed %s", work_unit.image_id)
            collector.mark_outcome(JobOutcome.SUCCESS)
            self._mq_client.ack(delivery_tag)

        except ForecastNotAvailableError as e:
            logger.warning(
                "Forecast not yet available, skipping %s: %s", work_unit.image_id, e
            )
            self._handler.release_progress(work_unit)
            collector.mark_outcome(JobOutcome.SKIPPED, str(e))
            self._mq_client.ack(delivery_tag)  # producer re-enqueues next cycle

        except UnprocessableInputError as e:
            # Deterministic bad input (e.g. radar sweeps with incompatible range
            # geometry). Ack and record SKIPPED — no retry, no DLQ. Unlike the
            # forecast case we do NOT release_progress: re-discovering it would
            # only re-skip it; the JOB_TTL reclaims the (short-lived) unit.
            logger.warning("Skipping unprocessable %s: %s", work_unit.image_id, e)
            collector.mark_outcome(JobOutcome.SKIPPED, str(e))
            self._mq_client.ack(delivery_tag)

        except TransientDownloadError as e:
            logger.warning(
                "Transient download error, requeuing %s: %s", work_unit.image_id, e
            )
            # Keep image_id marked in-progress so the producer's next cron tick
            # does not re-discover the GRIB and enqueue a duplicate WorkUnit
            # (which races with the requeued copy on the same work_dir).
            # If all workers crash before the requeued copy is processed,
            # ProgressTracker's TTL (JOB_TTL_MINUTES) eventually releases it.
            self._mq_client.publish(work_unit, queue_name=source_queue)
            collector.mark_outcome(JobOutcome.REQUEUED, str(e))
            self._mq_client.ack(delivery_tag)  # original acked; copy is requeued

        except Exception as e:  # pylint: disable=broad-exception-caught
            self._handle_processing_error(
                work_unit, delivery_tag, source_queue, collector, e
            )

        finally:
            self._record_metrics(collector)

    def _handle_processing_error(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        work_unit: WorkUnit,
        delivery_tag: int,
        source_queue: str,
        collector: JobMetricsContext,
        error: Exception,
    ) -> None:
        """Ack+retry, ack+DLQ, or (on shutdown) nack-requeue a failed unit."""
        if not self._running:
            logger.info("Shutdown interrupted processing of %s", work_unit.image_id)
            # Leave outcome unset and redeliver: not a terminal failure.
            self._mq_client.nack(delivery_tag, requeue=True)
            return

        logger.exception("Error processing %s: %s", work_unit, error)
        if work_unit.can_retry:
            retry_unit = work_unit.create_retry()
            logger.info(
                "Retrying %s (attempt %d/%d)",
                work_unit,
                retry_unit.retry_count,
                retry_unit.max_retries,
            )
            self._mq_client.publish(retry_unit, queue_name=source_queue)
            collector.mark_outcome(JobOutcome.ERROR, str(error))
        else:
            logger.error("Max retries exceeded for %s, sending to DLQ", work_unit)
            self._mq_client.publish_to_dlq(work_unit, str(error))
            collector.mark_outcome(JobOutcome.DLQ, str(error))
        self._mq_client.ack(delivery_tag)

    def _record_metrics(self, collector: JobMetricsContext) -> None:
        """Persist one metrics row. Never lets a metrics failure break the worker."""
        if self._metrics_repository is None or not collector.has_outcome:
            return
        try:
            self._metrics_repository.record(collector.build())
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception("Failed to record job metrics")


def _purge_stale_work_dirs(tmp_dir: Path) -> None:
    """Remove residual per-image working directories from prior crashes.

    The progress tracker DB and any non-directory entries are preserved.
    """
    if not tmp_dir.exists():
        return
    # Only directories are purged below, so the SQLite files (and their WAL
    # sidecars) are preserved regardless; listed for clarity.
    keep = {
        "progress_tracker.db",
        "progress_tracker.db-wal",
        "progress_tracker.db-shm",
        "metrics.db",
        "metrics.db-wal",
        "metrics.db-shm",
    }
    for entry in tmp_dir.iterdir():
        if entry.name in keep:
            continue
        try:
            if entry.is_dir():
                shutil.rmtree(entry, ignore_errors=True)
                logger.info("Purged stale work dir: %s", entry)
        except OSError as exc:
            logger.warning("Could not purge %s: %s", entry, exc)


def run_worker(config: Config) -> None:
    """
    Entry point to run a worker.

    Creates and starts a worker that processes work units from RabbitMQ.
    Heavy image processing runs in subprocesses to keep this process lightweight.

    Args:
        config: Application configuration
    """
    # Apply DB migrations before building any repository (Alembic owns the
    # schema). Serialized across processes by a file lock; a no-op once at head.
    ensure_migrations(config)

    data_source_registry = create_data_source_registry(config)
    mq_client = create_rabbitmq_client(config)

    # Stale work directories from prior crashes are not cleaned up by the
    # finally block in WorkHandler.handle, so wipe them at boot.
    _purge_stale_work_dirs(Path(config.TMP_DIR))

    # Configure S3 lifecycle policy for automatic tile expiration
    s3_client = create_s3_client(config)
    loop = new_event_loop()
    set_event_loop(loop)
    try:
        # Ensure bucket exists and configure lifecycle
        loop.run_until_complete(s3_client.ensure_bucket_exists())
        loop.run_until_complete(
            s3_client.configure_lifecycle_policy(config.TILE_RETENTION_DAYS)
        )
        logger.info("S3 per-prefix lifecycle configured for tile expiration")
    finally:
        loop.close()

    # Create progress tracker (SQLite-based)
    tracker_path = Path(config.TMP_DIR) / "progress_tracker.db"
    progress_tracker = ProgressTracker(tracker_path)

    # Create metrics repository (SQLite-based, shared with the metrics API)
    metrics_repository: Optional[MetricsRepository] = None
    if config.ENABLE_METRICS:
        metrics_repository = MetricsRepository(Path(config.METRICS_DB_PATH))

    # Build inline processors (run in main process, need MQ access).
    # GRIB inputs and ECMWF outputs each expire via their own per-prefix bucket
    # lifecycle rule (grib/models/ecmwf vs cog|tiles|geojson/models/ecmwf), so
    # operators can retain raw GRIB inputs independently of derived outputs.
    inline_processors: dict[str, InlineProcessor] = {}
    if config.ENABLE_ECMWF_PRECIPITATION:
        ecmwf_tp_s3 = create_s3_client(config)
        inline_processors[ECMWF_TP_CONFIG.inline_processor_id] = EcmwfGribDownloader(
            product_config=ECMWF_TP_CONFIG,
            s3_client=ecmwf_tp_s3,
            bounds=config.get_bounds(),
        )
    if config.ENABLE_ECMWF_MEAN_SEA_LEVEL_PRESSURE:
        ecmwf_mslp_s3 = create_s3_client(config)
        inline_processors[ECMWF_MSLP_CONFIG.inline_processor_id] = EcmwfGribDownloader(
            product_config=ECMWF_MSLP_CONFIG,
            s3_client=ecmwf_mslp_s3,
            bounds=config.get_bounds(),
        )

    # Create work handler with dependencies
    handler = WorkHandler(
        config=config,
        progress_tracker=progress_tracker,
        data_source_registry=data_source_registry,
        mq_client=mq_client,
        inline_processors=inline_processors,
    )

    # Create and start worker
    worker = Worker(config, mq_client, handler, metrics_repository)
    worker.start()
