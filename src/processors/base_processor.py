"""Base processor class definition."""

import shutil
from abc import ABC, abstractmethod
from pathlib import Path
import logging

from models.work_unit import WorkUnit
from config import Config

logger = logging.getLogger(__name__)


class ShutdownRequested(Exception):
    """Raised when graceful shutdown is requested between processing steps."""


class ImageProcessor(ABC):
    """
    Abstract base class for image processors.

    This class defines the interface for different types of image processors
    (e.g., Band 13, Band 9, Radar) that implement the full processing pipeline
    from downloaded file to S3 upload.
    """

    def __init__(self, config: Config):
        self.config = config
        self._base_dir = Path(config.TMP_DIR)
        self._shutdown_requested = False

    def request_shutdown(self) -> None:
        """Signal that the processor should stop at the next checkpoint."""
        self._shutdown_requested = True

    def _check_shutdown(self) -> None:
        """Raise ShutdownRequested if a graceful shutdown was requested."""
        if self._shutdown_requested:
            raise ShutdownRequested("Graceful shutdown requested")

    @abstractmethod
    async def process(self, downloaded_file_path: str, work_unit: WorkUnit) -> None:
        """
        Process the downloaded image and upload the result to S3.

        Args:
            downloaded_file_path: Path to the downloaded NetCDF file
            work_unit: The work unit containing metadata and configuration

        Raises:
            Exception: If any part of the processing fails
        """

    def _ensure_dir(self, directory: Path) -> Path:
        """Ensure directory exists and return it."""
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    def _get_band_dir(self, work_unit: WorkUnit) -> Path:
        """Get base directory for this work unit's band data."""
        return self._base_dir / work_unit.band_id

    def _cleanup_file(self, file_path: Path) -> None:
        """Safe cleanup of a single file."""
        try:
            if file_path.exists():
                file_path.unlink()
        except OSError as e:
            logger.warning("Failed to cleanup file %s: %s", file_path, e)

    def _cleanup_directory(self, dir_path: Path) -> None:
        """Safe cleanup of a directory."""
        try:
            if dir_path.exists():
                shutil.rmtree(dir_path)
        except OSError as e:
            logger.warning("Failed to cleanup directory %s: %s", dir_path, e)
