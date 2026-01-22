"""Producer that discovers new images and publishes work units."""

from asyncio import Event, run
from logging import getLogger
from signal import signal, SIGINT, SIGTERM
from datetime import datetime, UTC, timedelta
from pathlib import Path
from typing import Set

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from clients.rabbitmq_client import RabbitMQClient
from clients.message_queue_client import MessageQueueClient
from clients.s3_client import S3Client
from clients.progress_tracker import ProgressTracker
from config import Config
from data_sources import (
    DataSourceRegistry,
    DataSource,
    DiscoveryConfig,
    Goes19DataSource,
    RadarDataSource,
)
from models.band_config import BAND_CONFIGS
from models.work_unit import WorkUnit

logger = getLogger(__name__)


class ImageDiscoveryProducer:
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

        # S3 client for MinIO (to check existing tiles)
        self._minio_client = S3Client.create_with_credentials(
            bucket_name=config.S3_TILES_DATA_BUCKET_NAME,
            endpoint=config.S3_TILES_DATA_ENDPOINT,
            access_key=config.S3_TILES_DATA_RW_ACCESS_KEY,
            secret_key=config.S3_TILES_DATA_RW_SECRET_KEY,
            secure=config.S3_TILES_DATA_SECURE,
        )

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
                logger.info(f"Skipping {data_source.source_id} (disabled in config)")
                continue

            logger.info(f"Discovering new images for {data_source.source_id}...")

            try:
                count = await self._discover_source(current_time, data_source, bounds)
                total_published += count
                logger.info(f"Published {count} work units for {data_source.source_id}")
            except Exception as e:
                logger.exception(
                    f"Error discovering images for {data_source.source_id}: {e}"
                )

        logger.info(f"Total work units published: {total_published}")
        return total_published

    def _is_source_enabled(self, data_source: DataSource) -> bool:
        """Check if a data source is enabled in the config."""
        source_id = data_source.source_id

        # Check for GOES19 band sources
        if source_id == "goes19_band_13":
            return self._config.ENABLE_BAND_13
        elif source_id == "goes19_band_9":
            return self._config.ENABLE_BAND_9
        elif source_id == "radar_nexrad":
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
            f"Found {len(existing_tilesets)} existing tilesets for {data_source.source_id}"
        )

        # Get in-progress images from SQLite
        in_progress = self._progress_tracker.get_in_progress_images(band_id)
        logger.info(
            f"Found {len(in_progress)} images in progress for {data_source.source_id}"
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
            logger.info(f"No new images found for {data_source.source_id}")
            return 0

        logger.info(f"Found {len(new_images)} new images for {data_source.source_id}")

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
            logger.debug(f"Published work unit for {image_info.image_id}")

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
        except Exception as e:
            logger.warning(f"Error listing MinIO tilesets: {e}")
            return set()


def _create_data_source_registry() -> DataSourceRegistry:
    """Create and populate the data source registry."""
    registry = DataSourceRegistry()

    # Register GOES19 data sources for each band
    for band_id, band_config in BAND_CONFIGS.items():
        data_source = Goes19DataSource(band_config)
        registry.register(data_source)

    # Register Radar data source (placeholder)
    registry.register(RadarDataSource())

    return registry


def run_producer(config: Config) -> None:
    """
    Entry point to run the producer with APScheduler.

    Runs continuously, discovering new images on a schedule.

    Args:
        config: Application configuration
    """
    # Create data source registry
    data_source_registry = _create_data_source_registry()

    logger.info("Producer starting with APScheduler...")

    # Create progress tracker (SQLite-based)
    tracker_path = Path(config.TMP_DIR) / "progress_tracker.db"
    progress_tracker = ProgressTracker(
        tracker_path, ttl=timedelta(minutes=config.JOB_TTL_MINUTES)
    )

    # Create Message Queue client
    mq_client = RabbitMQClient(
        host=config.RABBITMQ_HOST,
        port=config.RABBITMQ_PORT,
        username=config.RABBITMQ_USER,
        password=config.RABBITMQ_PASSWORD,
        queue_name=config.RABBITMQ_QUEUE,
        dlq_name=config.RABBITMQ_DLQ,
        dlx_name=config.RABBITMQ_DLX,
    )
    mq_client.connect(max_retries=10, retry_delay=5.0)

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
        except Exception as e:
            logger.exception(f"Error in discovery job: {e}")

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

    # Set up signal handlers for graceful shutdown
    stop_event = Event()

    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        stop_event.set()

    signal(SIGINT, signal_handler)
    signal(SIGTERM, signal_handler)

    # Run the scheduler
    async def run_scheduler():
        scheduler.start()
        logger.info(f"Producer scheduler started (schedule: {cron_schedule})")

        # Run initial discovery immediately
        logger.info("Running initial image discovery...")
        await job_wrapper()

        # Wait for stop signal
        await stop_event.wait()

        scheduler.shutdown()
        mq_client.close()
        logger.info("Producer stopped")

    # Run the async scheduler
    run(run_scheduler())
