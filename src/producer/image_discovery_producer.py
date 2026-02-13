"""Producer that discovers new images and publishes work units."""

from asyncio import Event, get_running_loop, run
from logging import getLogger
from signal import SIGINT, SIGTERM
from datetime import datetime, UTC, timedelta
from pathlib import Path
from typing import Set

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from clients.message_queue_client import MessageQueueClient
from clients.progress_tracker import ProgressTracker
from config import Config
from data_sources import DataSource, DataSourceRegistry, DiscoveryConfig
from factories import (
    create_data_source_registry,
    create_minio_client,
    create_rabbitmq_client,
)
from models.work_unit import WorkUnit
from health_server import HealthCheckServer

logger = getLogger(__name__)


class ImageDiscoveryProducer:  # pylint: disable=too-few-public-methods
    """
    Producer that discovers new images and publishes work units.

    This producer:
    1. Runs on a schedule using APScheduler
    2. Iterates over registered data sources
    3. Discovers new images from each data source
    4. Checks MinIO for existing tiles (to avoid reprocessing)
    5. Checks in-progress tracker (to avoid duplicate work units)
    6. Creates work units for new images
    7. Publishes work units to RabbitMQ
    8. Marks images as in-progress in SQLite before publishing
    """

    # Default schedule: every 5 minutes
    DEFAULT_CRON = "*/5 * * * *"

    def __init__(
        self,
        config: Config,
        mq_client: MessageQueueClient,
        progress_tracker: ProgressTracker,
        data_source_registry: DataSourceRegistry,
    ):
        self._config = config
        self._mq_client = mq_client
        self._progress_tracker = progress_tracker
        self._data_source_registry = data_source_registry
        self._minio_client = create_minio_client(config)

    async def discover_and_publish(self) -> int:
        """
        Discover new images and publish work units.

        Returns:
            Number of work units published
        """
        current_time = datetime.now(UTC)
        bounds = self._config.get_bounds()
        total_published = 0

        # Process each registered data source
        for data_source in self._data_source_registry.get_all():
            if not self._is_source_enabled(data_source):
                logger.info("Skipping %s (disabled in config)", data_source.source_id)
                continue

            logger.info("Discovering new images for %s...", data_source.source_id)

            try:
                count = await self._discover_source(current_time, data_source, bounds)
                total_published += count
                logger.info(
                    "Published %d work units for %s",
                    count,
                    data_source.source_id,
                )
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.exception(
                    "Error discovering images for %s: %s",
                    data_source.source_id,
                    e,
                )

        logger.info("Total work units published: %d", total_published)
        return total_published

    def _is_source_enabled(self, data_source: DataSource) -> bool:
        """Check if a data source is enabled in the config."""
        source_id = data_source.source_id

        # Check for GOES19 band sources
        if source_id == "goes19_band_13":
            return self._config.ENABLE_BAND_13
        if source_id == "goes19_band_9":
            return self._config.ENABLE_BAND_9
        if source_id == "goes19_band_2":
            return self._config.ENABLE_BAND_2
        if source_id == "goes19_glm_fed":
            return self._config.ENABLE_GLM_FED
        if source_id == "radar_nexrad":
            return self._config.ENABLE_RADAR

        # Default: enabled if registered
        return True

    async def _discover_source(
        self,
        current_time: datetime,
        data_source: DataSource,
        bounds: dict,
    ) -> int:
        """Discover and publish work units for a single data source."""
        # Get band_id from source_id (e.g., "goes19_band_13" -> "band_13")
        band_id = data_source.source_id.replace("goes19_", "")

        # Get existing tilesets in MinIO
        output_prefix = f"{band_id}/tiles"
        existing_tilesets = await self._get_existing_tilesets(output_prefix)
        logger.info(
            "Found %d existing tilesets for %s",
            len(existing_tilesets),
            data_source.source_id,
        )

        # Get in-progress images from SQLite
        in_progress = self._progress_tracker.get_in_progress_images(band_id)
        logger.info(
            "Found %d images in progress for %s",
            len(in_progress),
            data_source.source_id,
        )

        # Create discovery config
        discovery_config = DiscoveryConfig(
            current_time=current_time,
            existing_tilesets=existing_tilesets,
            in_progress_images=in_progress,
            bounds=bounds,
        )

        # Discover new images
        new_images = await data_source.discover_images(discovery_config)

        if not new_images:
            logger.info("No new images found for %s", data_source.source_id)
            return 0

        logger.info(
            "Found %d new images for %s",
            len(new_images),
            data_source.source_id,
        )

        # Publish work units for new images
        published = 0
        for image_info in new_images:
            # Mark as in-progress in SQLite BEFORE publishing to queue
            self._progress_tracker.mark_in_progress(image_info.image_id, band_id)

            work_unit = WorkUnit.create(
                image_id=image_info.image_id,
                source_uri=image_info.source_uri,
                data_source_id=image_info.data_source_id,
                processor_id=image_info.processor_id,
                output_prefix=image_info.output_prefix,
                bounds=bounds,
                band_id=band_id,
            )
            self._mq_client.publish(work_unit)
            published += 1
            logger.debug("Published work unit for %s", image_info.image_id)

        return published

    async def _get_existing_tilesets(self, s3_prefix: str) -> Set[str]:
        """Get set of existing tileset names (base filenames) in MinIO."""
        try:
            prefixes = await self._minio_client.list_prefixes(
                f"{s3_prefix}/", delimiter="/"
            )
            tilesets = set()
            for prefix in prefixes:
                tileset_name = prefix.rstrip("/").split("/")[-1]
                # Remove _tiles suffix to get the base name
                if tileset_name.endswith("_tiles"):
                    base_name = tileset_name[:-6]  # Remove "_tiles"
                    tilesets.add(base_name)
            return tilesets
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning("Error listing MinIO tilesets: %s", e)
            return set()


def run_producer(config: Config) -> None:
    """
    Entry point to run the producer with APScheduler.

    Runs continuously, discovering new images on a schedule.

    Args:
        config: Application configuration
    """
    data_source_registry = create_data_source_registry()

    logger.info("Producer starting with APScheduler...")

    # Create progress tracker (SQLite-based)
    tracker_path = Path(config.TMP_DIR) / "progress_tracker.db"
    progress_tracker = ProgressTracker(
        tracker_path, ttl=timedelta(minutes=config.JOB_TTL_MINUTES)
    )

    mq_client = create_rabbitmq_client(config)

    # Start health check server
    def check_readiness() -> tuple[bool, str]:
        if not mq_client.is_connected:
            return False, "RabbitMQ not connected"
        return True, "Dependencies healthy"

    health_server = HealthCheckServer(
        port=config.HEALTH_PORT, check_readiness=check_readiness
    )
    health_server.start()

    # Create producer with dependencies
    producer = ImageDiscoveryProducer(
        config=config,
        mq_client=mq_client,
        progress_tracker=progress_tracker,
        data_source_registry=data_source_registry,
    )

    # Create scheduler
    scheduler = AsyncIOScheduler(timezone=config.TIMEZONE)

    async def job_wrapper():
        """Wrapper to run async discover_and_publish in the scheduler."""
        try:
            await producer.discover_and_publish()
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.exception("Error in discovery job: %s", e)

    # Add job with cron trigger (every 5 minutes by default)
    cron_schedule = ImageDiscoveryProducer.DEFAULT_CRON
    scheduler.add_job(
        job_wrapper,
        CronTrigger.from_crontab(cron_schedule),
        id="image_discovery",
        name="Discover new images",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=60,
    )

    # Run the scheduler
    async def run_scheduler():
        # Set up signal handlers within the event loop for async safety
        stop_event = Event()
        loop = get_running_loop()
        loop.add_signal_handler(SIGINT, stop_event.set)
        loop.add_signal_handler(SIGTERM, stop_event.set)

        # Run initial discovery BEFORE starting scheduler to prevent
        # race condition where the cron job fires during the initial run,
        # causing duplicate work units (both runs see empty in-progress set)
        logger.info("Running initial image discovery...")
        await job_wrapper()

        scheduler.start()
        logger.info("Producer scheduler started (schedule: %s)", cron_schedule)

        # Wait for stop signal
        await stop_event.wait()

        scheduler.shutdown()
        mq_client.close()
        health_server.stop()
        logger.info("Producer stopped")

    # Run the async scheduler
    run(run_scheduler())
