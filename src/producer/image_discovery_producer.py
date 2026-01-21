"""Producer that discovers new satellite images and publishes work units."""

import asyncio
import logging
import signal
from datetime import datetime, UTC, timedelta
from pathlib import Path
from typing import List, Set

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from clients.rabbitmq_client import RabbitMQClient
from clients.s3_client import S3Client
from clients.progress_tracker import ProgressTracker
from config import Config
from constants import constants
from models.band_config import BAND_CONFIGS, BandConfig
from models.work_unit import WorkUnit

logger = logging.getLogger(__name__)


class ImageDiscoveryProducer:
    """
    Producer that discovers new satellite images and publishes DOWNLOAD work units.

    This producer:
    1. Runs on a schedule using APScheduler
    2. Queries NOAA's S3 bucket for recent satellite images
    3. Checks MinIO for existing tiles (to avoid reprocessing)
    4. Checks in-progress tracker (to avoid duplicate work units)
    5. Creates DOWNLOAD work units for new images
    6. Publishes work units to RabbitMQ

    Discovery Strategy:
        - Looks for images from the last 4 hours (24 images at 10-min intervals)
        - Checks both enabled bands (band_13, band_9) based on config
        - Skips images that already have tiles in MinIO
        - Skips images that are currently being processed
    """

    # Number of images to process per band (4 hours at 10-min intervals)
    TARGET_IMAGES = 26

    # How many hours back to search for images
    MAX_HOURS_BACK = 5

    # Default schedule: every 5 minutes
    DEFAULT_CRON = "*/5 * * * *"

    def __init__(
        self,
        config: Config,
        rabbitmq_client: RabbitMQClient,
        progress_tracker: ProgressTracker,
    ):
        self._config = config
        self._rabbitmq = rabbitmq_client
        self._progress_tracker = progress_tracker

        # S3 client for NOAA public bucket (unsigned access)
        self._noaa_client = S3Client(
            constants.GOES19_BUCKET_NAME, max_concurrent_downloads=6
        )

        # S3 client for MinIO (to check existing tiles)
        self._minio_client = S3Client.create_with_credentials(
            bucket_name=config.S3_TILES_DATA_BUCKET_NAME,
            endpoint=config.S3_TILES_DATA_ENDPOINT,
            access_key=config.S3_TILES_DATA_RW_ACCESS_KEY,
            secret_key=config.S3_TILES_DATA_RW_SECRET_KEY,
            secure=config.S3_TILES_DATA_SECURE,
        )

        self._l1b_products_path = "ABI-L1b-RadF"

    async def discover_and_publish(self) -> int:
        """
        Discover new images and publish work units.

        Returns:
            Number of work units published
        """
        current_time = datetime.now(UTC)
        bounds = self._config.get_bounds()
        total_published = 0

        # Process each enabled band
        for band_id, band_config in BAND_CONFIGS.items():
            if not self._is_band_enabled(band_id):
                logger.info(f"Skipping {band_id} (disabled in config)")
                continue

            logger.info(f"Discovering new images for {band_id}...")

            try:
                count = await self._discover_band(current_time, band_config, bounds)
                total_published += count
                logger.info(f"Published {count} work units for {band_id}")
            except Exception as e:
                logger.exception(f"Error discovering images for {band_id}: {e}")

        logger.info(f"Total work units published: {total_published}")
        return total_published

    def _is_band_enabled(self, band_id: str) -> bool:
        """Check if a band is enabled in the config."""
        if band_id == "band_13":
            return self._config.ENABLE_BAND_13
        elif band_id == "band_9":
            return self._config.ENABLE_BAND_9
        return False

    async def _discover_band(
        self,
        current_time: datetime,
        band_config: BandConfig,
        bounds: dict,
    ) -> int:
        """Discover and publish work units for a single band."""
        # Get existing tilesets in MinIO
        existing_tilesets = await self._get_existing_tilesets(band_config.s3_prefix)
        logger.info(
            f"Found {len(existing_tilesets)} existing tilesets for {band_config.band_id}"
        )

        # Get in-progress images
        in_progress = self._progress_tracker.get_in_progress_images(band_config.band_id)
        logger.info(
            f"Found {len(in_progress)} images in progress for {band_config.band_id}"
        )

        # Find new images in NOAA S3
        new_images = await self._find_new_images(
            current_time,
            band_config.file_pattern,
            existing_tilesets,
            in_progress,
        )

        if not new_images:
            logger.info(f"No new images found for {band_config.band_id}")
            return 0

        logger.info(f"Found {len(new_images)} new images for {band_config.band_id}")

        # Publish work units for new images
        published = 0
        for s3_key in new_images:
            # Extract image_id
            image_id = s3_key.split("/")[-1]

            # Mark as in-progress before publishing
            self._progress_tracker.mark_in_progress(image_id, band_config.band_id)

            work_unit = WorkUnit.create_download_work_unit(
                source_s3_uri=s3_key,
                band_id=band_config.band_id,
                bounds=bounds,
            )
            self._rabbitmq.publish(work_unit)
            published += 1
            logger.debug(f"Published work unit for {image_id}")

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

    async def _find_new_images(
        self,
        current_time: datetime,
        file_pattern: str,
        existing_tilesets: Set[str],
        in_progress: Set[str],
    ) -> List[str]:
        """
        Find images in NOAA S3 that need processing.

        Strategy: Strict Latest N
        1. Collect all available images from the last MAX_HOURS_BACK.
        2. Sort by timestamp (descending) to get the absolute latest images.
        3. Take the top TARGET_IMAGES.
        4. Filter out any that are already processed or in progress.
        """
        all_candidates = []
        hours_back = 0

        # 1. Collect all candidates from the lookback window
        while hours_back <= self.MAX_HOURS_BACK:
            search_time = current_time - timedelta(hours=hours_back)
            directory_path = self._build_directory_path(search_time)

            try:
                files = await self._noaa_client._get_folder_file_paths(
                    directory_path, file_pattern=file_pattern
                )
                all_candidates.extend(files)
            except Exception as e:
                logger.warning(f"Error listing NOAA S3 for {directory_path}: {e}")

            hours_back += 1

        # 2. Sort by timestamp (descending)
        # Assuming filename format allows alphanumeric sorting, or extraction is needed.
        # Files are typically: OR_ABI-L1b-RadF-M6C13_G19_s20260211000204...
        # The 's' timestamp is correct for sorting.
        all_candidates.sort(reverse=True)

        # 3. Take top N (Strict Window)
        target_candidates = all_candidates[: self.TARGET_IMAGES]

        # 4. Filter
        new_images = []
        for s3_key in target_candidates:
            filename = s3_key.split("/")[-1]
            stem = Path(filename).stem

            # Skip if tiles already exist
            if stem in existing_tilesets:
                continue

            # Skip if already in progress
            if filename in in_progress:
                continue

            new_images.append(s3_key)

        return new_images

    def _build_directory_path(self, time: datetime) -> str:
        """Build the S3 directory path for a given time."""
        return f"{self._l1b_products_path}/{time.strftime('%Y/%j/%H')}"


def run_producer(config: Config) -> None:
    """
    Entry point to run the producer with APScheduler.

    Runs continuously, discovering new images on a schedule.

    Args:
        config: Application configuration
    """
    logger.info("Producer starting with APScheduler...")

    # Create progress tracker
    tracker_file = Path(config.TMP_DIR) / "progress_tracker.json"
    progress_tracker = ProgressTracker(tracker_file)

    # Create RabbitMQ client
    rabbitmq = RabbitMQClient(
        host=config.RABBITMQ_HOST,
        port=config.RABBITMQ_PORT,
        username=config.RABBITMQ_USER,
        password=config.RABBITMQ_PASSWORD,
    )
    rabbitmq.connect(max_retries=10, retry_delay=5.0)

    # Create producer
    producer = ImageDiscoveryProducer(config, rabbitmq, progress_tracker)

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
        name="Discover new satellite images",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=60,
    )

    # Set up signal handlers for graceful shutdown
    stop_event = asyncio.Event()

    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        stop_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

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
        rabbitmq.close()
        logger.info("Producer stopped")

    # Run the async scheduler
    asyncio.run(run_scheduler())
