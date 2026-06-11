"""Base class for GOES-19 data sources (ABI and GLM)."""

import logging
from abc import abstractmethod
from datetime import datetime, timedelta
from pathlib import Path

from data_sources.base import DataSource
from data_sources.goes19_repository import Goes19FileRepository
from models.band_config import BandConfig

logger = logging.getLogger(__name__)


class Goes19BaseDataSource(DataSource):
    """
    Abstract base class for GOES-19 data sources.

    Provides shared functionality for discovering and downloading GOES-19
    data via an injected repository (NOAA's public S3 bucket, a private
    mirror, or a local folder with the same layout). Both ABI (imaging) and
    GLM (lightning) data sources inherit from this class.

    The GOES-19 data is organized in hourly directories with the pattern:
    {product_path}/YYYY/JJJ/HH/ where JJJ is the day of year.
    """

    # Discovery parameters
    MAX_HOURS_BACK = 5  # How far back to search for files

    def __init__(
        self,
        band_config: BandConfig,
        product_path: str,
        repository: Goes19FileRepository,
    ):
        """
        Initialize GOES-19 data source.

        Args:
            band_config: Band configuration (determines file pattern, output prefix, etc.)
            product_path: Path prefix for the product (e.g., "ABI-L1b-RadF", "GLM-L2-LCFA")
            repository: Storage backend the hourly directories are read from
        """
        self._band_config = band_config
        self._product_path = product_path
        self._repository = repository

    @property
    def band_config(self) -> BandConfig:
        """Get the band configuration."""
        return self._band_config

    async def _collect_candidates_from_hourly_paths(
        self, current_time: datetime, file_pattern: str
    ) -> list[str]:
        """
        Collect all candidate files from the lookback window.

        Iterates through hourly directories backwards from current_time,
        listing all files that match the given pattern.

        Args:
            current_time: The reference time to start searching from
            file_pattern: Pattern to match files (e.g., "C13_G19", "OR_GLM-L2-LCFA")

        Returns:
            List of source URIs for all matching files found
        """
        all_candidates = []
        hours_back = 0

        while hours_back <= self.MAX_HOURS_BACK:
            search_time = current_time - timedelta(hours=hours_back)
            directory_path = self._build_directory_path(search_time)

            try:
                files = await self._repository.list_files(
                    directory_path, file_pattern=file_pattern
                )
                all_candidates.extend(files)
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.warning(
                    "Error listing files for %s in %s: %s",
                    self.source_id,
                    directory_path,
                    e,
                )

            hours_back += 1

        return all_candidates

    def _build_directory_path(self, time: datetime) -> str:
        """
        Build the hourly directory path for a given time.

        GOES-19 data is organized by year, day-of-year, and hour:
        {product_path}/YYYY/JJJ/HH/

        Args:
            time: The datetime to build a path for

        Returns:
            Directory path string (S3 key prefix or path under the local input dir)
        """
        return f"{self._product_path}/{time.strftime('%Y/%j/%H')}"

    @abstractmethod
    async def download(self, source_uri: str, dest_path) -> Path:
        """
        Download data from the configured backend.

        Must be implemented by subclasses to handle their specific download patterns.
        """
