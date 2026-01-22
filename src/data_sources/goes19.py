"""GOES-19 satellite data source implementation."""

import logging
from datetime import timedelta
from pathlib import Path
from typing import Set, List

from clients.s3_client import S3Client
from data_sources.base import DataSource, ImageInfo, DiscoveryConfig
from models.band_config import BandConfig

logger = logging.getLogger(__name__)


# NOAA public bucket for GOES-19 data
GOES19_BUCKET_NAME = "noaa-goes19"


class Goes19DataSource(DataSource):
    """
    Data source for GOES-19 satellite imagery from NOAA's public S3 bucket.

    This data source:
    - Discovers images from the ABI-L1b-RadF product path
    - Downloads NetCDF files from the unsigned public bucket
    - Supports different bands (band_13, band_9, etc.)
    """

    # Discovery parameters
    TARGET_IMAGES = 26  # 4+ hours at 10-min intervals
    MAX_HOURS_BACK = 5

    def __init__(self, band_config: BandConfig):
        """
        Initialize GOES-19 data source for a specific band.

        Args:
            band_config: Band configuration (determines file pattern, output prefix, etc.)
        """
        self._band_config = band_config
        self._s3_client = S3Client(GOES19_BUCKET_NAME, max_concurrent_downloads=6)
        self._l1b_products_path = "ABI-L1b-RadF"

    @property
    def source_id(self) -> str:
        """Unique identifier for this data source."""
        return f"goes19_{self._band_config.band_id}"

    @property
    def processor_id(self) -> str:
        """The processor ID to use for images from this source."""
        return f"goes_{self._band_config.band_id}"

    @property
    def band_config(self) -> BandConfig:
        """Get the band configuration."""
        return self._band_config

    async def discover_images(self, config: DiscoveryConfig) -> list[ImageInfo]:
        """
        Discover new GOES-19 images that need processing.

        Uses a "Strict Latest N" strategy:
        1. Collect all available images from the last MAX_HOURS_BACK hours
        2. Sort by timestamp (descending) to get the absolute latest images
        3. Take the top TARGET_IMAGES
        4. Filter out any that are already processed or in progress
        """
        all_candidates = await self._collect_candidates(config.current_time)

        # Sort by timestamp (descending) - filenames are sortable
        all_candidates.sort(reverse=True)

        # Take top N (Strict Window)
        target_candidates = all_candidates[: self.TARGET_IMAGES]

        # Filter out already processed or in-progress images
        new_images = []
        for s3_key in target_candidates:
            filename = s3_key.split("/")[-1]
            stem = Path(filename).stem

            # Skip if tiles already exist
            if stem in config.existing_tilesets:
                continue

            # Skip if already in progress
            if filename in config.in_progress_images:
                continue

            new_images.append(
                ImageInfo(
                    image_id=filename,
                    source_uri=s3_key,
                    data_source_id=self.source_id,
                    processor_id=self.processor_id,
                    output_prefix=self._band_config.s3_prefix,
                )
            )

        logger.info(
            f"[{self.source_id}] Found {len(new_images)} new images "
            f"(from {len(target_candidates)} candidates)"
        )
        return new_images

    async def _collect_candidates(self, current_time) -> List[str]:
        """Collect all candidate files from the lookback window."""
        all_candidates = []
        hours_back = 0

        while hours_back <= self.MAX_HOURS_BACK:
            search_time = current_time - timedelta(hours=hours_back)
            directory_path = self._build_directory_path(search_time)

            try:
                files = await self._s3_client._get_folder_file_paths(
                    directory_path, file_pattern=self._band_config.file_pattern
                )
                all_candidates.extend(files)
            except Exception as e:
                logger.warning(f"Error listing NOAA S3 for {directory_path}: {e}")

            hours_back += 1

        return all_candidates

    def _build_directory_path(self, time) -> str:
        """Build the S3 directory path for a given time."""
        return f"{self._l1b_products_path}/{time.strftime('%Y/%j/%H')}"

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

        content = await self._s3_client.download_single_file(s3_key)

        if content is None:
            raise RuntimeError(f"Failed to download {s3_key}")

        # Ensure parent directory exists
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        # Write to local file
        dest_path.write_bytes(content)
        logger.info(f"[{self.source_id}] Downloaded to {dest_path}")

        return dest_path
