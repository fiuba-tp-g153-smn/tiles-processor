"""Base class for stage handlers."""

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional

from config import Config
from models.work_unit import WorkUnit

logger = logging.getLogger(__name__)


class BaseStageHandler(ABC):
    """
    Abstract base class for processing stage handlers.

    Each stage handler is responsible for:
    1. Processing a specific stage of the pipeline
    2. Updating the work unit paths with output locations
    3. Returning the updated work unit

    Stage handlers should be stateless - all state is in the WorkUnit.
    """

    def __init__(self, config: Config):
        self._config = config
        self._base_dir = Path.cwd() / config.TMP_DIR

    @abstractmethod
    async def handle(self, work_unit: WorkUnit) -> WorkUnit:
        """
        Process the work unit for this stage.

        Args:
            work_unit: The work unit to process

        Returns:
            Updated work unit with paths populated for this stage's output

        Raises:
            Exception: If processing fails
        """
        pass

    def _get_band_dir(self, work_unit: WorkUnit) -> Path:
        """Get the base directory for this band's temporary files."""
        return self._base_dir / work_unit.band_id

    def _ensure_dir(self, path: Path) -> Path:
        """Ensure a directory exists and return it."""
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _cleanup_file(self, file_path: Optional[str]) -> None:
        """Safely delete a file if it exists."""
        if file_path:
            path = Path(file_path)
            if path.exists():
                try:
                    path.unlink()
                    logger.debug(f"Deleted: {file_path}")
                except Exception as e:
                    logger.warning(f"Failed to delete {file_path}: {e}")
