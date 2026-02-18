"""ECMWF model data source implementation for total precipitation forecasts."""

import asyncio
import json
import logging
from datetime import datetime, timedelta, UTC
from pathlib import Path
from typing import Optional

from ecmwf.opendata import Client

from clients.s3_client import S3Client
from data_sources.base import DataSource, ImageInfo, DiscoveryConfig
from models.ecmwf_config import EcmwfProductConfig

logger = logging.getLogger(__name__)

# ECMWF forecast configuration constants
PUBLICATION_DELAY_HOURS = 7  # Minimum delay before forecast is available
MAX_LOOKBACK_HOURS = 48  # Maximum time to search backwards for forecasts
FORECAST_HOURS = 144  # Total forecast length (6 days)
PERIOD_HOURS = 6  # Length of each precipitation period


class EcmwfDataSource(DataSource):
    """
    Data source for ECMWF total precipitation forecasts.

    This data source implements sequential processing logic:
    - Only processes forecast T_i if T_{i-1} is already completed
    - Downloads GRIB files from ECMWF API during discovery
    - Caches GRIB files in MinIO for worker processing
    - Generates 24 work units per forecast (one per 6-hour period)

    The ECMWF IFS model publishes forecasts at 00:00 UTC and 12:00 UTC,
    but they are not available until ~7 hours after the base time due to
    processing delays.
    """

    def __init__(self, product_config: EcmwfProductConfig, minio_client: S3Client):
        """
        Initialize ECMWF data source.

        Args:
            product_config: ECMWF product configuration
            minio_client: S3 client for MinIO storage
        """
        self._product_config = product_config
        self._minio_client = minio_client
        self._ecmwf_client = Client(source="aws")

    @property
    def source_id(self) -> str:
        """Unique identifier for this data source."""
        return f"ecmwf_{self._product_config.product_id}"

    @property
    def processor_id(self) -> str:
        """The processor ID to use for images from this source."""
        return f"ecmwf_{self._product_config.product_id}"

    async def discover_images(self, config: DiscoveryConfig) -> list[ImageInfo]:
        """
        Discover new ECMWF forecasts that need processing.

        Sequential processing enforcement:
        - Finds the next forecast T_i where T_{i-1} is completed but T_i is not
        - If T_i is not available on ECMWF API, returns empty (waits for next cycle)
        - Downloads GRIB from ECMWF, uploads to MinIO, generates 24 ImageInfo
        - Ignores all candidates older than the most recent processed forecast

        Args:
            config: Discovery configuration with current time, existing tilesets, etc.

        Returns:
            List of 24 ImageInfo for 6-hour periods, or empty list if nothing to process
        """
        current_time = config.current_time

        # Generate candidate forecast base times in descending order (newest to oldest)
        candidates = self._generate_candidate_timestamps(current_time)

        # Filter out candidates older than the latest processed forecast
        latest_processed = self._get_latest_processed_forecast(config.existing_tilesets)
        if latest_processed is not None:
            candidates = [c for c in candidates if c >= latest_processed]

        logger.info(
            "[%s] Searching for next forecast to process (checking %d candidates)",
            self.source_id,
            len(candidates),
        )

        for candidate_time in candidates:
            # Check if current candidate is completed
            current_completed = self._is_forecast_completed(
                candidate_time, config.existing_tilesets
            )

            if current_completed:
                # Both T_{i-1} and T_i are completed, continue to older forecasts
                logger.debug(
                    "[%s] Forecast %s already completed",
                    self.source_id,
                    candidate_time.strftime("%Y-%m-%dT%H:%M"),
                )
                continue

            # Found the next forecast to process: T_{i-1} done, T_i not done
            logger.info(
                "[%s] Found next forecast to process: %s",
                self.source_id,
                candidate_time.strftime("%Y-%m-%dT%H:%M UTC"),
            )

            # Try to download from ECMWF API
            grib_path = await self._download_and_cache_grib(candidate_time)

            if grib_path is None:
                # GRIB not available yet, wait for next discovery cycle
                logger.warning(
                    "[%s] Forecast %s not available on ECMWF API yet, will retry later",
                    self.source_id,
                    candidate_time.strftime("%Y-%m-%dT%H:%M UTC"),
                )
                continue

            # Generate 24 ImageInfo for 6-hour periods
            image_infos = self._generate_period_image_infos(
                candidate_time, grib_path, config
            )

            logger.info(
                "[%s] Generated %d work units for forecast %s",
                self.source_id,
                len(image_infos),
                candidate_time.strftime("%Y-%m-%dT%H:%M UTC"),
            )

            return image_infos

        logger.info(
            "[%s] No new forecasts to process at this time",
            self.source_id,
        )
        return []

    def _generate_candidate_timestamps(self, current_time: datetime) -> list[datetime]:
        """
        Generate candidate forecast base times in descending order.

        ECMWF publishes forecasts at 00:00 and 12:00 UTC, available after
        PUBLICATION_DELAY_HOURS. Searches back up to MAX_LOOKBACK_HOURS.

        Args:
            current_time: Current time (UTC)

        Returns:
            List of candidate forecast base times (newest to oldest)
        """
        candidates = []

        # Calculate the most recent potential forecast time accounting for delay
        latest_possible = current_time - timedelta(hours=PUBLICATION_DELAY_HOURS)

        # Round down to nearest 12-hour boundary (00:00 or 12:00)
        if latest_possible.hour >= 12:
            latest_forecast = latest_possible.replace(
                hour=12, minute=0, second=0, microsecond=0
            )
        else:
            latest_forecast = latest_possible.replace(
                hour=0, minute=0, second=0, microsecond=0
            )

        # Generate candidates going backwards
        current_candidate = latest_forecast
        cutoff_time = current_time - timedelta(hours=MAX_LOOKBACK_HOURS)

        while current_candidate >= cutoff_time:
            candidates.append(current_candidate)
            # Go back 12 hours for next candidate
            current_candidate -= timedelta(hours=12)

        return candidates

    def _get_latest_processed_forecast(self, existing_tilesets: set[str]) -> Optional[datetime]:
        """
        Find the most recent forecast that has been processed.

        Args:
            existing_tilesets: Set of processed image IDs

        Returns:
            The most recent forecast base time that was processed, or None if none found
        """
        latest_time = None
        prefix = "ecmwf_tp_"

        for tileset_id in existing_tilesets:
            if not tileset_id.startswith(prefix):
                continue

            try:
                # Extract timestamp from format: ecmwf_tp_20260217T0000_h000-006
                # Remove prefix and split by underscore
                parts = tileset_id[len(prefix):].split('_')
                if len(parts) < 2:
                    continue

                timestamp_str = parts[0]  # e.g., "20260217T0000"
                forecast_time = datetime.strptime(timestamp_str, "%Y%m%dT%H%M").replace(tzinfo=UTC)

                if latest_time is None or forecast_time > latest_time:
                    latest_time = forecast_time
            except (ValueError, IndexError):
                # Invalid format, skip
                continue

        return latest_time

    def _is_forecast_completed(
        self, forecast_time: datetime, existing_tilesets: set[str]
    ) -> bool:
        """
        Check if any period from a forecast has been processed.

        Args:
            forecast_time: The forecast base time
            existing_tilesets: Set of processed image IDs

        Returns:
            True if at least one period from this forecast exists
        """
        # Check if the first period (h000-006) exists as a proxy for completion
        base_time_str = forecast_time.strftime("%Y%m%dT%H%M")
        first_period_id = f"ecmwf_tp_{base_time_str}_h000-006"

        return first_period_id in existing_tilesets

    async def _download_and_cache_grib(
        self, forecast_time: datetime
    ) -> Optional[str]:
        """
        Download GRIB from ECMWF API and upload to MinIO cache.

        Args:
            forecast_time: The forecast base time (00:00 or 12:00 UTC)

        Returns:
            MinIO path to cached GRIB, or None if download failed
        """
        base_time_str = forecast_time.strftime("%Y%m%dT%H%MZ")
        minio_key = f"models/ecmwf/total_precipitation/tmp/{base_time_str}.grib"

        # Check if already cached in MinIO
        try:
            exists = await self._minio_client.exists(minio_key)
            if exists:
                logger.info(
                    "[%s] GRIB already cached in MinIO: %s",
                    self.source_id,
                    minio_key,
                )
                return minio_key
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning(
                "[%s] Error checking MinIO cache: %s", self.source_id, e
            )

        # Download from ECMWF API
        logger.info(
            "[%s] Downloading GRIB from ECMWF API for %s",
            self.source_id,
            base_time_str,
        )

        try:
            # Run ECMWF client in thread pool (it's synchronous)
            loop = asyncio.get_event_loop()
            local_grib_path = await loop.run_in_executor(
                None,
                self._retrieve_ecmwf_grib,
                forecast_time,
            )

            # Upload to MinIO
            logger.info(
                "[%s] Uploading GRIB to MinIO: %s",
                self.source_id,
                minio_key,
            )
            await self._minio_client.upload_file(local_grib_path, minio_key)

            # Cleanup local file
            Path(local_grib_path).unlink(missing_ok=True)

            logger.info(
                "[%s] Successfully cached GRIB in MinIO: %s",
                self.source_id,
                minio_key,
            )
            return minio_key

        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error(
                "[%s] Failed to download/cache GRIB for %s: %s",
                self.source_id,
                base_time_str,
                e,
            )
            return None

    def _retrieve_ecmwf_grib(self, forecast_time: datetime) -> str:
        """
        Retrieve GRIB from ECMWF API (synchronous operation).

        Args:
            forecast_time: The forecast base time

        Returns:
            Path to downloaded GRIB file

        Raises:
            Exception: If retrieval fails
        """
        date = forecast_time.strftime("%Y-%m-%d")
        time = forecast_time.hour  # 0 or 12
        steps = list(range(3, 145, 3))

        # Create temporary file path
        temp_path = f"/tmp/ecmwf_{forecast_time.strftime('%Y%m%d')}_{time:02d}.grib"

        # Retrieve from ECMWF
        self._ecmwf_client.retrieve(
            date=date,
            time=time,
            step=steps,
            type="fc",
            param=[self._product_config.parameter],
            target=temp_path,
        )

        return temp_path

    def _generate_period_image_infos(
        self,
        forecast_time: datetime,
        minio_grib_path: str,
        config: DiscoveryConfig,
    ) -> list[ImageInfo]:
        """
        Generate ImageInfo for each 6-hour period in the forecast.

        Args:
            forecast_time: The forecast base time
            minio_grib_path: MinIO path to cached GRIB file
            config: Discovery configuration

        Returns:
            List of ImageInfo for 24 periods (0-6h, 6-12h, ..., 138-144h)
        """
        image_infos = []
        base_time_str = forecast_time.strftime("%Y%m%dT%H%M")

        # Generate 24 periods of 6 hours each
        for period_idx in range(FORECAST_HOURS // PERIOD_HOURS):
            hour_start = period_idx * PERIOD_HOURS
            hour_end = (period_idx + 1) * PERIOD_HOURS

            image_id = f"ecmwf_tp_{base_time_str}_h{hour_start:03d}-{hour_end:03d}"

            # Skip if already processed or in progress
            if image_id in config.existing_tilesets:
                continue
            if image_id in config.in_progress_images:
                continue

            # Create source_uri with metadata for worker
            source_uri = json.dumps(
                {
                    "minio_path": minio_grib_path,
                    "base_time": forecast_time.isoformat(),
                    "hour_start": hour_start,
                    "hour_end": hour_end,
                }
            )

            image_infos.append(
                ImageInfo(
                    image_id=image_id,
                    source_uri=source_uri,
                    data_source_id=self.source_id,
                    processor_id=self.processor_id,
                    output_prefix=self._product_config.s3_prefix,
                )
            )

        return image_infos

    async def download(self, source_uri: str, dest_path: Path) -> Path:
        """
        Download GRIB file from MinIO cache to local path.

        Args:
            source_uri: JSON string with MinIO path and metadata
            dest_path: Local path to save the downloaded file

        Returns:
            Path to the downloaded file
        """
        # Parse source_uri JSON
        metadata = json.loads(source_uri)
        minio_path = metadata["minio_path"]

        # Ensure destination directory exists
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        # Download from MinIO
        await self._minio_client.download_to_file(minio_path, dest_path)

        logger.info(
            "[%s] Downloaded GRIB from MinIO %s to %s",
            self.source_id,
            minio_path,
            dest_path,
        )

        return dest_path
