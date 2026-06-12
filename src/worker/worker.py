"""Worker implementation for processing work units from RabbitMQ."""

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
            # Start consuming (blocking). Normal workers drain their normal
            # queue strict-first, then round-robin the two light queues; light
            # workers only round-robin the light queues.
            strict, round_robin = self._consume_tiers()
            self._mq_client.consume(
                callback=self._process_message,
                strict_queues=strict,
                round_robin_queues=round_robin,
            )
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

    def _process_message(
        self,
        work_unit: WorkUnit,
        client: MessageQueueClient,
        _delivery_tag: int,
        source_queue: str,
    ) -> bool:
        """
        Process a single work unit message.

        This is called by the RabbitMQ consumer for each message.
        Returns True to acknowledge the message, False to reject it.

        Args:
            work_unit: The work unit to process
            client: RabbitMQ client for publishing
            _delivery_tag: Message delivery tag for ack/nack
            source_queue: Queue this message came from. Requeues and retries go
                back here so a light unit stolen by a normal worker returns to
                the light queue, not the worker's primary (normal) queue.

        Returns:
            True if message should be acknowledged
        """
        logger.info("Processing: %s", work_unit)
        collector = JobMetricsContext(work_unit, worker_host=self._config.WORKER_ID)

        try:
            # Run the async handler in the shared event loop
            if self._loop is None:
                raise RuntimeError("Event loop is not initialized")

            self._loop.run_until_complete(self._handler.handle(work_unit, collector))

            logger.info("Successfully processed %s", work_unit.image_id)
            collector.mark_outcome(JobOutcome.SUCCESS)
            return True  # Acknowledge

        except ForecastNotAvailableError as e:
            logger.warning(
                "Forecast not yet available, skipping %s: %s", work_unit.image_id, e
            )
            self._handler.release_progress(work_unit)
            collector.mark_outcome(JobOutcome.SKIPPED, str(e))
            return (
                True  # Acknowledge without retry; producer will re-enqueue next cycle
            )

        except TransientDownloadError as e:
            logger.warning(
                "Transient download error, requeuing %s: %s", work_unit.image_id, e
            )
            # Keep image_id marked in-progress so the producer's next cron tick
            # does not re-discover the GRIB and enqueue a duplicate WorkUnit
            # (which races with the requeued copy on the same work_dir).
            # If all workers crash before the requeued copy is processed,
            # ProgressTracker's TTL (JOB_TTL_MINUTES) eventually releases it.
            client.publish(work_unit, queue_name=source_queue)
            collector.mark_outcome(JobOutcome.REQUEUED, str(e))
            return True  # Acknowledge original; copy is back in the queue

        except Exception as e:  # pylint: disable=broad-exception-caught
            if not self._running:
                logger.info("Shutdown interrupted processing of %s", work_unit.image_id)
                # Leave outcome unset: the message is requeued, not terminal.
                return False  # Don't ack - RabbitMQ will requeue on disconnect

            logger.exception("Error processing %s: %s", work_unit, e)

            # Check if we can retry
            if work_unit.can_retry:
                retry_unit = work_unit.create_retry()
                logger.info(
                    "Retrying %s (attempt %d/%d)",
                    work_unit,
                    retry_unit.retry_count,
                    retry_unit.max_retries,
                )
                client.publish(retry_unit, queue_name=source_queue)
                collector.mark_outcome(JobOutcome.ERROR, str(e))
            else:
                # Max retries exceeded, send to DLQ
                logger.error("Max retries exceeded for %s, sending to DLQ", work_unit)
                client.publish_to_dlq(work_unit, str(e))
                collector.mark_outcome(JobOutcome.DLQ, str(e))

            return True  # Acknowledge (we've handled it via retry or DLQ)

        finally:
            self._record_metrics(collector)

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
        logger.info(
            "S3 lifecycle configured: tiles will expire after %d days",
            config.TILE_RETENTION_DAYS,
        )
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
    # GRIB uploads use SEAWEEDFS_ECMWF_GRIB_TTL — independent from the output
    # TTL (SEAWEEDFS_ECMWF_TTL) so operators can keep raw GRIB inputs around
    # longer than the derived COG/tile/GeoJSON outputs (or vice versa).
    inline_processors: dict[str, InlineProcessor] = {}
    if config.ENABLE_ECMWF_PRECIPITATION:
        ecmwf_tp_s3 = create_s3_client(config, with_ttl=config.SEAWEEDFS_ECMWF_GRIB_TTL)
        inline_processors[ECMWF_TP_CONFIG.inline_processor_id] = EcmwfGribDownloader(
            product_config=ECMWF_TP_CONFIG,
            s3_client=ecmwf_tp_s3,
            bounds=config.get_bounds(),
        )
    if config.ENABLE_ECMWF_MEAN_SEA_LEVEL_PRESSURE:
        ecmwf_mslp_s3 = create_s3_client(
            config, with_ttl=config.SEAWEEDFS_ECMWF_GRIB_TTL
        )
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
