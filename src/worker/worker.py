"""Worker implementation for processing work units from RabbitMQ."""

import asyncio
import logging
import signal
import sys
from pathlib import Path
from typing import Dict, Type

from clients.rabbitmq_client import RabbitMQClient
from config import Config
from models.stage import Stage
from models.work_unit import WorkUnit
from worker.stage_handlers.base_handler import BaseStageHandler
from worker.stage_handlers.download_handler import DownloadHandler
from worker.stage_handlers.georeference_handler import GeoreferenceHandler
from worker.stage_handlers.brightness_handler import BrightnessTemperatureHandler
from worker.stage_handlers.geotiff_handler import GeoTIFFHandler
from worker.stage_handlers.tiles_upload_handler import TilesUploadHandler
from worker.stage_handlers.cleanup_handler import CleanupHandler

logger = logging.getLogger(__name__)

# Healthcheck file path
HEALTH_FILE = Path("/app/data/tmp/healthy")


class Worker:
    """
    Worker that consumes work units from RabbitMQ and processes them.

    The worker:
    1. Connects to RabbitMQ and starts consuming from the work queue
    2. Dispatches each work unit to the appropriate stage handler
    3. On success, publishes the next stage work unit and acknowledges
    4. On failure, either retries or sends to dead letter queue

    Stage Handlers:
        - DOWNLOAD: DownloadHandler
        - GEOREFERENCE: GeoreferenceHandler
        - BRIGHTNESS_TEMPERATURE: BrightnessTemperatureHandler
        - GEOTIFF: GeoTIFFHandler
        - TILES_AND_UPLOAD: TilesUploadHandler
        - CLEANUP: CleanupHandler

    Error Handling:
        - Transient errors: Retry up to max_retries times
        - Permanent errors: Send to dead letter queue
        - Handler exceptions are caught and logged
    """

    # Map stages to handler classes
    HANDLER_MAP: Dict[Stage, Type[BaseStageHandler]] = {
        Stage.DOWNLOAD: DownloadHandler,
        Stage.GEOREFERENCE: GeoreferenceHandler,
        Stage.BRIGHTNESS_TEMPERATURE: BrightnessTemperatureHandler,
        Stage.GEOTIFF: GeoTIFFHandler,
        Stage.TILES_AND_UPLOAD: TilesUploadHandler,
        Stage.CLEANUP: CleanupHandler,
    }

    def __init__(self, config: Config, rabbitmq_client: RabbitMQClient):
        self._config = config
        self._rabbitmq = rabbitmq_client
        self._handlers: Dict[Stage, BaseStageHandler] = {}
        self._running = True

        # Initialize handlers
        for stage, handler_class in self.HANDLER_MAP.items():
            self._handlers[stage] = handler_class(config)

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
        Uses an asyncio event loop to handle async stage handlers.
        """
        logger.info("Worker starting...")

        # Update heartbeat on startup
        self._update_heartbeat()

        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

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
            # Run the async handler in the event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                updated_work_unit = loop.run_until_complete(
                    self._handle_work_unit(work_unit)
                )
            finally:
                loop.close()

            # Success - publish next stage if not terminal
            if not updated_work_unit.is_terminal:
                next_work_unit = updated_work_unit.create_next_stage()
                if next_work_unit:
                    client.publish(next_work_unit)
                    logger.info(f"Published next stage: {next_work_unit}")
            else:
                logger.info(f"Completed terminal stage for {work_unit.image_id}")

            # Update heartbeat after successful processing
            self._update_heartbeat()

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

    async def _handle_work_unit(self, work_unit: WorkUnit) -> WorkUnit:
        """
        Dispatch work unit to the appropriate stage handler.

        Args:
            work_unit: The work unit to process

        Returns:
            Updated work unit with stage outputs populated

        Raises:
            Exception: If the handler fails
        """
        handler = self._handlers.get(work_unit.stage)
        if handler is None:
            raise ValueError(f"No handler for stage: {work_unit.stage}")

        logger.info(f"Dispatching to {handler.__class__.__name__}")
        return await handler.handle(work_unit)


def run_worker(config: Config) -> None:
    """
    Entry point to run a worker.

    Creates and starts a worker that processes work units from RabbitMQ.

    Args:
        config: Application configuration
    """
    # Create RabbitMQ client
    rabbitmq = RabbitMQClient(
        host=config.RABBITMQ_HOST,
        port=config.RABBITMQ_PORT,
        username=config.RABBITMQ_USER,
        password=config.RABBITMQ_PASSWORD,
    )

    # Connect with retry
    rabbitmq.connect(max_retries=10, retry_delay=5.0)

    # Create and start worker
    worker = Worker(config, rabbitmq)
    worker.start()
