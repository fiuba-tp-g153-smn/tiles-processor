"""Base processor class definition."""

import json
import shutil
from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import Path
from time import perf_counter
from typing import Iterator
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
        self._stage_timings: dict[str, float] = {}
        self._metrics_sink: Path | None = None

    def request_shutdown(self) -> None:
        """Signal that the processor should stop at the next checkpoint."""
        self._shutdown_requested = True

    def _check_shutdown(self) -> None:
        """Raise ShutdownRequested if a graceful shutdown was requested."""
        if self._shutdown_requested:
            raise ShutdownRequested("Graceful shutdown requested")

    def bind_metrics_sink(self, path: Path) -> None:
        """Set the file where per-stage timings are flushed after processing."""
        self._metrics_sink = path

    @contextmanager
    def _time_stage(self, name: str) -> Iterator[None]:
        """Accumulate the wall-clock duration of a pipeline stage.

        Durations for repeated stage names sum together (e.g. a processor that
        loops over forecast steps), so the flushed value is the total per stage.
        """
        start = perf_counter()
        try:
            yield
        finally:
            elapsed = perf_counter() - start
            self._stage_timings[name] = self._stage_timings.get(name, 0.0) + elapsed

    def flush_metrics(self) -> None:
        """Write accumulated stage timings to the bound sink as JSON.

        Called in a ``finally`` by the subprocess entry point so partial timings
        survive a mid-pipeline failure. No-op when nothing was recorded.
        """
        if self._metrics_sink is None or not self._stage_timings:
            return
        try:
            self._metrics_sink.write_text(json.dumps(self._stage_timings))
        except OSError as exc:
            logger.warning(
                "Failed to write metrics sink %s: %s", self._metrics_sink, exc
            )

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
