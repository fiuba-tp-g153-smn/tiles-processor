"""Unified work handler for processing work units (download + process)."""

from logging import getLogger
from pathlib import Path
from time import perf_counter

from clients.progress_tracker import ProgressTracker
from config import Config
from data_sources import DataSourceRegistry
from models.work_unit import WorkUnit
from processors import ProcessorRegistry, ImageProcessor

logger = getLogger(__name__)


class WorkHandler:
    """
    Unified handler for processing work units.

    This handler performs the complete processing flow:
    1. Download image from data source
    2. Process image with the appropriate processor
    3. Cleanup temporary files
    4. Update progress tracker

    The download and process are combined into a single atomic unit of work.
    """

    def __init__(
        self,
        config: Config,
        progress_tracker: ProgressTracker,
        data_source_registry: DataSourceRegistry,
        processor_registry: ProcessorRegistry,
    ):
        self._config = config
        self._progress_tracker = progress_tracker
        self._data_source_registry = data_source_registry
        self._processor_registry = processor_registry
        self._base_dir = Path(config.TMP_DIR)

        # Cache for processor instances (lazy instantiation)
        self._processor_cache: dict[str, ImageProcessor] = {}

    def _get_processor(self, processor_id: str) -> ImageProcessor:
        """Get or create a processor instance."""
        if processor_id not in self._processor_cache:
            processor_class = self._processor_registry.get(processor_id)
            self._processor_cache[processor_id] = processor_class(self._config)
        return self._processor_cache[processor_id]

    async def handle(self, work_unit: WorkUnit) -> None:
        """
        Handle a work unit by downloading and processing the image.

        Args:
            work_unit: The work unit to process

        Raises:
            Exception: If download or processing fails
        """
        total_start = perf_counter()
        logger.info(f"[HANDLER] Starting processing for {work_unit}")

        # Get data source and processor
        data_source = self._data_source_registry.get(work_unit.data_source_id)
        processor = self._get_processor(work_unit.processor_id)

        # Setup directories
        raw_dir = self._ensure_dir(self._base_dir / work_unit.band_id / "raw")
        local_path = raw_dir / work_unit.image_id

        try:
            # Step 1: Download
            download_start = perf_counter()
            logger.info(f"[HANDLER] Downloading {work_unit.image_id}")
            await data_source.download(work_unit.source_uri, local_path)
            download_time = perf_counter() - download_start

            # Step 2: Process
            process_start = perf_counter()
            logger.info(f"[HANDLER] Processing {work_unit.image_id}")
            await processor.process(str(local_path), work_unit)
            process_time = perf_counter() - process_start

            # Step 3: Mark as completed in SQLite
            self._progress_tracker.mark_completed(work_unit.image_id, work_unit.band_id)

            # Log timing summary
            total_time = perf_counter() - total_start
            logger.info(
                f"[HANDLER] Completed {work_unit.image_id} | "
                f"download: {download_time:.2f}s, "
                f"process: {process_time:.2f}s, "
                f"total: {total_time:.2f}s"
            )

        finally:
            # Cleanup downloaded file
            self._cleanup_file(local_path)

    def _ensure_dir(self, directory: Path) -> Path:
        """Ensure directory exists and return it."""
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    def _cleanup_file(self, file_path: Path) -> None:
        """Safe cleanup of a single file."""
        try:
            if file_path.exists():
                file_path.unlink()
                logger.debug(f"Cleaned up: {file_path}")
        except Exception as e:
            logger.warning(f"Failed to cleanup file {file_path}: {e}")
