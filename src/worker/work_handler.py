"""Unified work handler for processing work units (download + process)."""

import subprocess
import sys
from collections import deque
from logging import getLogger
from pathlib import Path
from threading import Thread
from time import perf_counter

from clients.progress_tracker import ProgressTracker
from config import Config
from data_sources import DataSourceRegistry
from models.work_unit import WorkUnit

logger = getLogger(__name__)


class WorkHandler:
    """
    Unified handler for processing work units.

    This handler performs the complete processing flow:
    1. Download image from data source (in main process - lightweight)
    2. Process image in subprocess (heavy libraries isolated)
    3. Cleanup temporary files
    4. Update progress tracker

    Memory Optimization:
        The heavy processing (pyproj, rioxarray, GDAL) runs in a subprocess.
        When the subprocess exits, all memory from those libraries is reclaimed.
        This keeps the main worker process lightweight when idle.
    """

    def __init__(
        self,
        config: Config,
        progress_tracker: ProgressTracker,
        data_source_registry: DataSourceRegistry,
    ):
        self._config = config
        self._progress_tracker = progress_tracker
        self._data_source_registry = data_source_registry
        self._base_dir = Path(config.TMP_DIR)

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

        # Get data source for download
        data_source = self._data_source_registry.get(work_unit.data_source_id)

        # Setup directories
        raw_dir = self._ensure_dir(self._base_dir / work_unit.band_id / "raw")
        local_path = raw_dir / work_unit.image_id

        try:
            # Step 1: Download (lightweight, stays in main process)
            download_start = perf_counter()
            logger.info(f"[HANDLER] Downloading {work_unit.image_id}")
            await data_source.download(work_unit.source_uri, local_path)
            download_time = perf_counter() - download_start

            # Step 2: Process in subprocess (heavy libraries isolated)
            process_start = perf_counter()
            logger.info(
                f"[HANDLER] Processing {work_unit.image_id} in subprocess "
                f"(processor: {work_unit.processor_id})"
            )
            self._run_processing_subprocess(work_unit, str(local_path))
            process_time = perf_counter() - process_start

            # Step 3: Mark as completed in SQLite
            self._progress_tracker.mark_completed(work_unit.image_id, work_unit.band_id)

            # Log timing summary
            total_time = perf_counter() - total_start
            logger.info(
                f"[HANDLER] Completed end2end processing and upload | "
                f"download: {download_time:.2f}s, "
                f"process: {process_time:.2f}s, "
                f"total: {total_time:.2f}s | "
                f"image_id: {work_unit.image_id}"
            )

        finally:
            # Cleanup downloaded file
            self._cleanup_file(local_path)

    def _run_processing_subprocess(self, work_unit: WorkUnit, file_path: str) -> None:
        """
        Run image processing in a subprocess for memory isolation.

        When the subprocess exits, all memory from heavy libraries
        (pyproj, rioxarray, GDAL) is reclaimed by the OS.

        Subprocess logs are streamed to parent's logger in real-time.

        Args:
            work_unit: The work unit to process
            file_path: Path to the downloaded file

        Raises:
            RuntimeError: If subprocess fails (includes error details from stderr)
        """
        work_unit_json = work_unit.to_json()

        # Start subprocess with pipes for real-time streaming
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "worker.subprocess_processor",
                work_unit_json,
                file_path,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,  # Line-buffered
        )

        # Keep last N lines of stderr for error reporting
        stderr_buffer: deque[str] = deque(maxlen=50)

        def stream_stdout(pipe):
            """Stream stdout line by line to logger."""
            try:
                for line in iter(pipe.readline, ""):
                    line = line.rstrip()
                    if line:
                        logger.info(line)
            finally:
                pipe.close()

        def stream_stderr(pipe, buffer: deque):
            """Stream stderr line by line to logger and buffer for errors."""
            try:
                for line in iter(pipe.readline, ""):
                    line = line.rstrip()
                    if line:
                        buffer.append(line)
                        logger.error(f"[SUBPROCESS] {line}")
            finally:
                pipe.close()

        # Start streaming threads
        stdout_thread = Thread(
            target=stream_stdout,
            args=(process.stdout,),
            daemon=True,
        )
        stderr_thread = Thread(
            target=stream_stderr,
            args=(process.stderr, stderr_buffer),
            daemon=True,
        )

        stdout_thread.start()
        stderr_thread.start()

        # Wait for process to complete (with timeout)
        try:
            return_code = process.wait(timeout=1800)  # 30 minute timeout
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            # Wait for threads to capture any remaining output
            stdout_thread.join(timeout=2)
            stderr_thread.join(timeout=2)
            raise RuntimeError(
                f"Processing subprocess timed out after 30 minutes "
                f"for {work_unit.image_id}"
            )

        # Wait for streaming threads to finish capturing all output
        stdout_thread.join(timeout=5)
        stderr_thread.join(timeout=5)

        # Check for errors
        if return_code != 0:
            # Build detailed error message from stderr buffer
            error_details = (
                "\n".join(stderr_buffer)
                if stderr_buffer
                else "No error details captured"
            )
            raise RuntimeError(
                f"Processing subprocess failed for {work_unit.image_id} "
                f"(exit code {return_code}):\n{error_details}"
            )

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
