"""Worker implementation for processing work units from RabbitMQ."""

from asyncio import AbstractEventLoop, new_event_loop, set_event_loop
from logging import getLogger
from signal import signal, SIGINT, SIGTERM
from pathlib import Path
from typing import Optional

import processors  # noqa: F401
from clients.rabbitmq_client import RabbitMQClient
from clients.progress_tracker import ProgressTracker
from config import Config
from data_sources import DataSourceRegistry, Goes19DataSource
from models.band_config import BAND_CONFIGS
from models.work_unit import WorkUnit
from worker.work_handler import WorkHandler

logger = getLogger(__name__)

# Healthcheck file path
HEALTH_FILE = Path("/app/data/tmp/healthy")


class Worker:
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
        rabbitmq_client: RabbitMQClient,
        progress_tracker: ProgressTracker,
    ):
        self._config = config
        self._rabbitmq = rabbitmq_client
        self._progress_tracker = progress_tracker
        self._running = True
        self._loop: Optional[AbstractEventLoop] = None

        # Initialize the unified work handler
        self._handler = WorkHandler(config, progress_tracker)

    def _update_heartbeat(self) -> None:
        """Update the heartbeat file for health checks."""
        try:
            HEALTH_FILE.parent.mkdir(parents=True, exist_ok=True)
            HEALTH_FILE.touch()
        except Exception as e:
            logger.warning(f"Failed to update heartbeat file: {e}")

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

        # Update heartbeat on startup
        self._update_heartbeat()

        # Set up signal handlers for graceful shutdown
        signal(SIGINT, self._signal_handler)
        signal(SIGTERM, self._signal_handler)

        try:
            # Start consuming (blocking)
            self._rabbitmq.consume(
                callback=self._process_message,
                prefetch_count=1,
            )
        except KeyboardInterrupt:
            logger.info("Worker interrupted by user")
        finally:
            self._shutdown()

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, initiating shutdown...")
        self._running = False
        # The consume loop will exit on the next iteration

    def _shutdown(self) -> None:
        """Clean shutdown of the worker."""
        logger.info("Worker shutting down...")
        try:
            self._rabbitmq.close()
        except Exception as e:
            logger.warning(f"Error closing RabbitMQ connection: {e}")

        # Close the event loop
        if self._loop and not self._loop.is_closed():
            self._loop.close()

        logger.info("Worker stopped")

    def _process_message(
        self, work_unit: WorkUnit, client: RabbitMQClient, delivery_tag: int
    ) -> bool:
        """
        Process a single work unit message.

        This is called by the RabbitMQ consumer for each message.
        Returns True to acknowledge the message, False to reject it.

        Args:
            work_unit: The work unit to process
            client: RabbitMQ client for publishing
            delivery_tag: Message delivery tag for ack/nack

        Returns:
            True if message should be acknowledged
        """
        logger.info(f"Processing: {work_unit}")

        try:
            # Run the async handler in the shared event loop
            self._loop.run_until_complete(self._handler.handle(work_unit))

            # Update heartbeat after successful processing
            self._update_heartbeat()

            logger.info(f"Successfully processed {work_unit.image_id}")
            return True  # Acknowledge

        except Exception as e:
            logger.exception(f"Error processing {work_unit}: {e}")

            # Check if we can retry
            if work_unit.can_retry:
                retry_unit = work_unit.create_retry()
                logger.info(
                    f"Retrying {work_unit} (attempt {retry_unit.retry_count}/{retry_unit.max_retries})"
                )
                client.publish(retry_unit)
            else:
                # Max retries exceeded, send to DLQ
                logger.error(f"Max retries exceeded for {work_unit}, sending to DLQ")
                client.publish_to_dlq(work_unit, str(e))

            return True  # Acknowledge (we've handled it via retry or DLQ)


def run_worker(config: Config) -> None:
    """
    Entry point to run a worker.

    Creates and starts a worker that processes work units from RabbitMQ.

    Args:
        config: Application configuration
    """
    # Register GOES19 data sources for each band
    for band_id, band_config in BAND_CONFIGS.items():
        data_source = Goes19DataSource(band_config)
        DataSourceRegistry.register(data_source)

    # Create RabbitMQ client
    rabbitmq = RabbitMQClient(
        host=config.RABBITMQ_HOST,
        port=config.RABBITMQ_PORT,
        username=config.RABBITMQ_USER,
        password=config.RABBITMQ_PASSWORD,
    )

    # Connect with retry
    rabbitmq.connect(max_retries=10, retry_delay=5.0)

    # Create progress tracker
    tracker_file = Path(config.TMP_DIR) / "progress_tracker.json"
    progress_tracker = ProgressTracker(tracker_file)

    # Create and start worker
    worker = Worker(config, rabbitmq, progress_tracker)
    worker.start()
