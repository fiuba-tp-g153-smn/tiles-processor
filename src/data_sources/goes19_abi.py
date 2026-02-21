"""GOES-19 ABI (Advanced Baseline Imager) satellite data source implementation."""

import logging
import re
from pathlib import Path

from data_sources.base import ImageInfo, DiscoveryConfig
from data_sources.goes19_base import Goes19BaseDataSource
from models.band_config import BandConfig

logger = logging.getLogger(__name__)


class Goes19AbiDataSource(Goes19BaseDataSource):
    """
    Data source for GOES-19 ABI satellite imagery from NOAA's public S3 bucket.

    This data source:
    - Discovers images from the ABI-L1b-RadF product path
    - Downloads NetCDF files from the unsigned public bucket
    - Supports different bands (band_13, band_9, band_2, etc.)
    """

    # Discovery parameters
    TARGET_IMAGES = 26  # 4+ hours at 10-min intervals

    def __init__(self, band_config: BandConfig):
        """
        Initialize GOES-19 ABI data source for a specific band.

        Args:
            band_config: Band configuration (determines file pattern, output prefix, etc.)
        """
        super().__init__(
            band_config=band_config,
            product_path="ABI-L1b-RadF",
            max_concurrent_downloads=6,
        )

    @property
    def source_id(self) -> str:
        """Unique identifier for this data source."""
        return f"goes19_abi_{self._band_config.band_id}"

    @property
    def processor_id(self) -> str:
        """The processor ID to use for images from this source."""
        return f"goes_{self._band_config.band_id}"

    async def discover_images(self, config: DiscoveryConfig) -> list[ImageInfo]:
        """
        Discover new GOES-19 images that need processing.

        Uses a "Strict Latest N" strategy:
        1. Collect all available images from the last MAX_HOURS_BACK hours
        2. Sort by timestamp (descending) to get the absolute latest images
        3. Take the top TARGET_IMAGES
        4. Filter out any that are already processed or in progress
        """
        all_candidates = await self._collect_candidates_from_hourly_paths(
            config.current_time, self._band_config.file_pattern
        )

        # Sort by timestamp (descending) - filenames are sortable
        all_candidates.sort(reverse=True)

        # Take top N (Strict Window)
        target_candidates = all_candidates[: self.TARGET_IMAGES]

        # Filter out already processed or in-progress images
        new_images = []
        for s3_key in target_candidates:
            filename = s3_key.split("/")[-1]

            # Extract timestamp (s20260521320209 -> 20260521320209)
            match = re.search(r's(\d{14})_', filename)
            if not match:
                continue
            timestamp = match.group(1)

            # Skip if tiles already exist
            if timestamp in config.existing_tilesets:
                continue

            # Skip if already in progress
            if timestamp in config.in_progress_images:
                continue

            new_images.append(
                ImageInfo(
                    image_id=timestamp,
                    source_uri=s3_key,
                    data_source_id=self.source_id,
                    processor_id=self.processor_id,
                    output_prefix=self._band_config.s3_prefix,
                )
            )

        logger.info(
            "[%s] Found %d new images (from %d candidates)",
            self.source_id,
            len(new_images),
            len(target_candidates),
        )
        return new_images

    async def download(self, source_uri: str, dest_path: Path) -> Path:
        """
        Download an image from NOAA's S3 bucket.

        Args:
            source_uri: S3 key of the file to download
            dest_path: Local path to save the downloaded file

        Returns:
            Path to the downloaded file.
        """
        # Handle full URI format (s3://bucket/key) or just key
        if source_uri.startswith("s3://"):
            parts = source_uri.replace("s3://", "").split("/", 1)
            s3_key = parts[1] if len(parts) > 1 else parts[0]
        else:
            s3_key = source_uri

        dest_path.parent.mkdir(parents=True, exist_ok=True)

        await self._s3_client.download_to_file(s3_key, dest_path)
        logger.info("[%s] Downloaded to %s", self.source_id, dest_path)

        return dest_path
