"""Process stage handler - executes image processing pipeline."""

import logging
from typing import Optional
from pathlib import Path

from config import Config
from models.work_unit import WorkUnit
from worker.stage_handlers.base_handler import BaseStageHandler
from processors.base_processor import ImageProcessor
from processors.band13_processor import Band13Processor
from processors.band9_processor import Band9Processor

logger = logging.getLogger(__name__)


class ProcessHandler(BaseStageHandler):
    """
    Handler for the PROCESS stage.

    Dispatches the work unit to the appropriate ImageProcessor based on the
    processor_type.
    """

    def __init__(self, config: Config, progress_tracker):
        super().__init__(config)
        self._progress_tracker = progress_tracker
        self._processors: dict[str, ImageProcessor] = {
            "band_13": Band13Processor(config),
            "band_9": Band9Processor(config),
        }

    async def handle(self, work_unit: WorkUnit) -> WorkUnit:
        """
        Process the image using the appropriate processor.

        Returns work unit for terminal check.
        """
        logger.info(f"[PROCESS] Starting for {work_unit.image_id}")

        if not work_unit.paths.downloaded_file:
            raise ValueError("downloaded_file path is required for PROCESS stage")

        processor_type = work_unit.processor_type
        if processor_type not in self._processors:
            raise ValueError(f"No processor found for type: {processor_type}")

        processor = self._processors[processor_type]

        await processor.process(work_unit.paths.downloaded_file, work_unit)

        logger.info(f"[PROCESS] Completed for {work_unit.image_id}")

        # 1. Mark as completed in SQLite
        self._progress_tracker.mark_completed(work_unit.image_id, work_unit.band_id)

        # 2. Cleanup downloaded NetCDF file
        self._cleanup_file(work_unit.paths.downloaded_file)

        # Return the work unit so the worker can check is_terminal
        return work_unit
