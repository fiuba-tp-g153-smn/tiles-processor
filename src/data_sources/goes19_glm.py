"""GOES-19 GLM (Geostationary Lightning Mapper) data source implementation."""

import asyncio
import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from data_sources.base import ImageInfo, DiscoveryConfig
from data_sources.goes19_base import Goes19BaseDataSource
from models.band_config import BandConfig

logger = logging.getLogger(__name__)


class Goes19GlmDataSource(Goes19BaseDataSource):
    """
    Data source for GOES-19 GLM lightning data from NOAA's public S3 bucket.

    GLM-L2-LCFA files contain events, groups, and flashes at ~20 second intervals.
    For a 10-minute FED product, we need to aggregate ~30 files.

    Discovery Strategy:
    - Instead of discovering individual files, we discover "time windows"
    - Each window = 10 minutes of data (e.g., 12:00-12:10)
    - Window ID = start timestamp (e.g., "GLM_FED_s20260212120000")
    - We'll download all L2-LCFA files within that window during processing
    """

    TARGET_WINDOWS = 26  # 4+ hours of 10-min windows
    WINDOW_DURATION_MINUTES = 10
    MAX_CONCURRENT_DOWNLOADS = 10  # Parallel downloads per window

    def __init__(self, band_config: BandConfig):
        """
        Initialize GLM data source.

        Args:
            band_config: Band configuration for GLM product
        """
        super().__init__(
            band_config=band_config,
            product_path="GLM-L2-LCFA",
            max_concurrent_downloads=self.MAX_CONCURRENT_DOWNLOADS,
        )

    @property
    def source_id(self) -> str:
        """Unique identifier for this data source."""
        return "goes19_glm_fed"

    @property
    def processor_id(self) -> str:
        """The processor ID to use for images from this source."""
        return "glm_fed"

    async def discover_images(self, config: DiscoveryConfig) -> list[ImageInfo]:
        """
        Discover new GLM time windows that need processing.

        Returns ImageInfo for each 10-minute window, where:
        - image_id = synthetic ID like "GLM_FED_s20260212120000"
        - source_uri = JSON with window_start and list of file S3 keys
        """
        # Collect all available L2-LCFA files from lookback window
        all_files = await self._collect_candidates_from_hourly_paths(
            config.current_time, "OR_GLM-L2-LCFA"
        )

        # Group files into 10-minute windows
        windows = self._group_into_windows(all_files)

        # Sort by window start time (descending)
        windows.sort(key=lambda x: x[0], reverse=True)

        # Filter out incomplete windows (must have full duration elapsed)
        complete_windows = [
            (window_start, window_files)
            for window_start, window_files in windows
            if window_start + timedelta(minutes=self.WINDOW_DURATION_MINUTES)
            <= config.current_time
        ]

        # Cap at TARGET_WINDOWS after filtering so we get up to N complete windows
        target_windows = complete_windows[: self.TARGET_WINDOWS]

        logger.debug(
            "[%s] Filtered %d/%d windows (excluded %d incomplete windows)",
            self.source_id,
            len(target_windows),
            len(windows),
            len(windows) - len(complete_windows),
        )

        # Filter already processed
        new_images = []
        for window_start, window_files in target_windows:
            window_id = self._create_window_id(window_start)

            # Skip if already processed or in progress
            if (
                window_id in config.existing_tilesets
                or window_id in config.in_progress_images
            ):
                continue

            # Store window metadata + file list in source_uri (JSON-encoded)
            source_uri = json.dumps(
                {"window_start": window_start.isoformat(), "files": window_files}
            )

            new_images.append(
                ImageInfo(
                    image_id=window_id,
                    source_uri=source_uri,
                    data_source_id=self.source_id,
                    processor_id=self.processor_id,
                    output_prefix=self._band_config.s3_prefix,
                )
            )

        logger.info(
            "[%s] Found %d new windows (from %d total windows)",
            self.source_id,
            len(new_images),
            len(target_windows),
        )
        return new_images

    def _group_into_windows(  # pylint: disable=too-many-locals
        self, files: list[str]
    ) -> list[tuple[datetime, list[str]]]:
        """
        Group L2-LCFA files into 10-minute windows based on timestamps.

        GLM filenames follow pattern:
        OR_GLM-L2-LCFA_G19_s20260212120000_e20260212120200_c20260212120228.nc
                             ^start         ^end

        Args:
            files: List of S3 keys for GLM-L2-LCFA files

        Returns:
            List of (window_start_time, [file1, file2, ...]) tuples
        """
        windows = defaultdict(list)

        for file_key in files:
            filename = file_key.split("/")[-1]

            # Extract start time from filename (position after "_s")
            if "_s" not in filename:
                logger.warning("Skipping malformed GLM filename: %s", filename)
                continue

            try:
                # Extract timestamp: s20260212120000 → 2026-02-12 12:00:00
                start_str = filename.split("_s")[1].split("_")[0]
                year = int(start_str[0:4])
                day_of_year = int(start_str[4:7])
                hour = int(start_str[7:9])
                minute = int(start_str[9:11])
                second = int(start_str[11:13])

                # Convert day-of-year to datetime (UTC, matching NOAA filenames)
                file_time = datetime(year, 1, 1, tzinfo=timezone.utc) + timedelta(
                    days=day_of_year - 1, hours=hour, minutes=minute, seconds=second
                )

                # Round down to nearest 10-minute boundary
                window_minute = (
                    file_time.minute // self.WINDOW_DURATION_MINUTES
                ) * self.WINDOW_DURATION_MINUTES
                window_start = file_time.replace(
                    minute=window_minute, second=0, microsecond=0
                )

                windows[window_start].append(file_key)

            except (ValueError, IndexError) as e:
                logger.warning("Failed to parse GLM filename %s: %s", filename, e)
                continue

        # Convert to sorted list of tuples
        return list(windows.items())

    def _create_window_id(self, window_start: datetime) -> str:
        """Create a unique ID for a time window (timestamp only)."""
        # Format: 20260521120000 (YYYYMMDDHHMMSS)
        return window_start.strftime("%Y%m%d%H%M%S")

    async def download(self, source_uri: str, dest_path: Path) -> Path:
        """
        Download all L2-LCFA files for a time window in parallel.

        Args:
            source_uri: JSON string with {"window_start": ..., "files": [...]}
            dest_path: Directory to save all files (treated as directory, not single file)

        Returns:
            Path to the directory containing downloaded files
        """
        window_data = json.loads(source_uri)
        files = window_data["files"]
        window_start = window_data["window_start"]

        # Ensure dest_path is a directory
        dest_path.mkdir(parents=True, exist_ok=True)

        logger.info(
            "[%s] Downloading %d GLM files for window %s (max concurrency: %d)",
            self.source_id,
            len(files),
            window_start,
            self.MAX_CONCURRENT_DOWNLOADS,
        )

        # Download files in parallel with semaphore-based concurrency control
        semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_DOWNLOADS)

        async def download_one(s3_key: str) -> None:
            """Download a single file with semaphore control."""
            async with semaphore:
                filename = s3_key.split("/")[-1]
                file_dest = dest_path / filename
                await self._s3_client.download_to_file(s3_key, file_dest)

        # Create tasks for all files and run them concurrently
        tasks = [download_one(s3_key) for s3_key in files]
        await asyncio.gather(*tasks)

        logger.info(
            "[%s] Downloaded %d files to %s", self.source_id, len(files), dest_path
        )
        return dest_path
