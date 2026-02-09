"""Base processor class definition."""

from abc import ABC, abstractmethod
from typing import Optional
from pathlib import Path
import logging

from models.work_unit import WorkUnit
from config import Config

logger = logging.getLogger(__name__)


class ImageProcessor(ABC):
    """
    Abstract base class for image processors.

    This class defines the interface for different types of image processors
    (e.g., Band 13, Band 9, Radar) that implement the full processing pipeline
    from downloaded file to S3 upload.
    """

    def __init__(self, config: Config):
        self.config = config
        # Use TMP_DIR for consistency with stage handlers
        self._base_dir = Path(config.TMP_DIR)

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
        pass

    def _ensure_dir(self, directory: Path) -> Path:
        """Ensure directory exists and return it."""
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    def _get_band_dir(self, work_unit: WorkUnit) -> Path:
        """Get base directory for this work unit's product data."""
        return self._base_dir / work_unit.product_id

    def _cleanup_file(self, file_path: Path) -> None:
        """Safe cleanup of a single file."""
        try:
            if file_path.exists():
                file_path.unlink()
        except Exception as e:
            logger.warning(f"Failed to cleanup file {file_path}: {e}")

    def _cleanup_directory(self, dir_path: Path) -> None:
        """Safe cleanup of a directory."""
        import shutil

        try:
            if dir_path.exists():
                shutil.rmtree(dir_path)
        except Exception as e:
            logger.warning(f"Failed to cleanup directory {dir_path}: {e}")
