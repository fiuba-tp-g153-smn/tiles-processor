"""Unified work handler for processing work units (download + process)."""

import asyncio
import json
import os
import shutil
import signal as _signal
import sys
import time
from asyncio.subprocess import Process
from collections import deque
from logging import getLogger
from pathlib import Path
from threading import Thread
from time import perf_counter
from uuid import uuid4

from clients.message_queue_client import MessageQueueClient
from clients.progress_tracker import ProgressTracker
from config import Config
from data_sources import DataSourceRegistry
from exceptions import UnprocessableInputError
from models.work_unit import WorkUnit
from worker.exit_codes import EXIT_SKIP_CODE, SKIP_REASON_PREFIX
from worker.inline_processor import InlineProcessor
from worker.job_metrics_context import JobMetricsContext

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
        mq_client: MessageQueueClient | None = None,
        inline_processors: dict[str, InlineProcessor] | None = None,
    ):
        self._config = config
        self._progress_tracker = progress_tracker
        self._data_source_registry = data_source_registry
        self._mq_client = mq_client
        self._inline_processors: dict[str, InlineProcessor] = inline_processors or {}
        self._base_dir = Path(config.TMP_DIR)
        # Live processing subprocesses (one per in-flight unit under
        # WORKER_CONCURRENCY). abort() signals all of them on shutdown.
        self._processes: set[Process] = set()

    async def handle(
        self, work_unit: WorkUnit, collector: JobMetricsContext | None = None
    ) -> None:
        """
        Handle a work unit by downloading and processing the image.

        Args:
            work_unit: The work unit to process
            collector: Optional per-job metrics accumulator. When provided, the
                download/process timings are recorded into it; the caller stamps
                the outcome and persists the row.

        Raises:
            Exception: If download or processing fails
        """
        total_start = perf_counter()
        logger.info("[HANDLER] Starting processing for %s", work_unit)

        # Transition the producer's IN_PROGRESS row to PROCESSING so the tracker
        # TTL can reclaim it if this worker crashes / dead-letters / stalls
        # mid-job (otherwise the image would stay queued forever and never be
        # rediscovered).
        self._progress_tracker.mark_processing(work_unit.image_id, work_unit.band_id)

        # Get data source for download
        data_source = self._data_source_registry.get(work_unit.data_source_id)

        # Setup a per-attempt-unique work directory. Keying on a fresh token
        # (not just band_id/image_id) means two concurrent copies of the same
        # unit — a redelivery, or a producer re-discovery racing an in-flight
        # one under WORKER_CONCURRENCY>1 — never share a scratch dir and so
        # can't rmtree each other's raw file mid-flight.
        image_stem = Path(work_unit.image_id).stem
        attempt = uuid4().hex[:8]
        work_dir = self._ensure_dir(
            self._base_dir / work_unit.band_id / f"{image_stem}-{attempt}"
        )
        raw_dir = self._ensure_dir(work_dir / "raw")
        local_path = raw_dir / work_unit.image_id

        # Per-stage timings are written here by the subprocess. It is a SIBLING
        # of work_dir so neither the processor's nor this handler's rmtree of
        # work_dir removes it before we read it back.
        metrics_sink = work_dir.parent / f"{image_stem}-{attempt}.metrics.json"

        try:
            # Step 1: Download (lightweight, stays in main process)
            download_start = perf_counter()
            logger.info("[HANDLER] Downloading %s", work_unit.image_id)
            local_path = await data_source.download(work_unit.source_uri, local_path)
            download_time = perf_counter() - download_start
            if collector is not None:
                collector.set_download_seconds(download_time)

            # Step 2: Process — inline (no subprocess) or in subprocess
            process_start = perf_counter()
            if work_unit.processor_id in self._inline_processors:
                logger.info(
                    "[HANDLER] Processing %s inline (processor: %s)",
                    work_unit.image_id,
                    work_unit.processor_id,
                )
                assert self._mq_client is not None
                await self._inline_processors[work_unit.processor_id].process(
                    str(local_path), work_unit, self._mq_client, collector
                )
            else:
                logger.info(
                    "[HANDLER] Processing %s in subprocess (processor: %s)",
                    work_unit.image_id,
                    work_unit.processor_id,
                )
                await self._run_processing_subprocess(
                    work_unit, str(local_path), metrics_sink
                )
                if collector is not None:
                    collector.set_stage_timings(self._read_stage_timings(metrics_sink))
            process_time = perf_counter() - process_start
            if collector is not None:
                collector.set_process_seconds(process_time)

            # Step 3: Mark as completed in SQLite
            self._progress_tracker.mark_completed(work_unit.image_id, work_unit.band_id)

            # Log timing summary
            total_time = perf_counter() - total_start
            logger.info(
                "[HANDLER] Completed end2end processing and upload | "
                "download: %.2fs, process: %.2fs, total: %.2fs | "
                "image_id: %s",
                download_time,
                process_time,
                total_time,
                work_unit.image_id,
            )

        finally:
            # Cleanup entire per-image work directory (raw + any subprocess leftovers)
            self._cleanup_directory(work_dir)
            self._cleanup_file(metrics_sink)

    @staticmethod
    def _read_stage_timings(sink_path: Path) -> dict[str, float]:
        """Read per-stage timings written by the subprocess (best-effort)."""
        try:
            if sink_path.exists():
                return json.loads(sink_path.read_text())
        except (OSError, ValueError) as exc:
            logger.warning("Could not read stage timings %s: %s", sink_path, exc)
        return {}

    @staticmethod
    def _cleanup_file(file_path: Path) -> None:
        """Safe removal of a single file."""
        try:
            file_path.unlink(missing_ok=True)
        except OSError as exc:
            logger.warning("Failed to cleanup file %s: %s", file_path, exc)

    def release_progress(self, work_unit: WorkUnit) -> None:
        """Remove a work unit from the progress tracker so it can be rediscovered."""
        self._progress_tracker.mark_completed(work_unit.image_id, work_unit.band_id)

    def abort(self) -> None:
        """SIGTERM every live subprocess group for graceful shutdown.

        Called from the worker's signal handler (a synchronous context), so it
        signals the process groups directly rather than awaiting. A background
        thread escalates to SIGKILL any group still alive after a grace period.
        """
        procs = [p for p in self._processes if p.returncode is None]
        if not procs:
            return

        logger.info(
            "[HANDLER] Terminating %d subprocess group(s) for graceful shutdown...",
            len(procs),
        )
        for proc in procs:
            self._signal_group(proc, _signal.SIGTERM)

        def _force_kill():
            time.sleep(8)
            for proc in procs:
                if proc.returncode is None:
                    logger.warning(
                        "[HANDLER] Subprocess %s did not exit after SIGTERM; SIGKILL",
                        proc.pid,
                    )
                    self._signal_group(proc, _signal.SIGKILL)

        Thread(target=_force_kill, daemon=True).start()

    @staticmethod
    def _signal_group(proc: Process, sig: int) -> None:
        """Send a signal to the subprocess's own process group, ignoring races."""
        try:
            os.killpg(os.getpgid(proc.pid), sig)
        except ProcessLookupError:
            pass  # Already exited

    async def _run_processing_subprocess(
        self, work_unit: WorkUnit, file_path: str, metrics_sink: Path
    ) -> None:
        """
        Run image processing in a subprocess for memory isolation.

        Awaitable so the worker's event loop stays free to overlap this unit's
        compute+upload tail with another unit's work. When the subprocess exits,
        all heavy-library memory (pyproj, rioxarray, GDAL) is reclaimed by the OS.
        Subprocess logs are streamed to the parent logger in real time.

        Args:
            work_unit: The work unit to process
            file_path: Path to the downloaded file
            metrics_sink: Path where the subprocess writes per-stage timings

        Raises:
            RuntimeError: If the subprocess fails or times out (includes error
                details from the tail of its stderr).
        """
        proc = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "worker.subprocess_processor",
            work_unit.to_json(),
            file_path,
            str(metrics_sink),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        self._processes.add(proc)

        stderr_buffer: deque[str] = deque(maxlen=50)
        readers = asyncio.gather(
            self._stream_stdout(proc.stdout),
            self._stream_stderr(proc.stderr, stderr_buffer),
            return_exceptions=True,
        )
        try:
            try:
                return_code = await asyncio.wait_for(proc.wait(), timeout=1800)
            except asyncio.TimeoutError as exc:
                await self._kill(proc)
                raise RuntimeError(
                    f"Processing subprocess timed out after 30 minutes "
                    f"for {work_unit.image_id}"
                ) from exc
            finally:
                await readers  # drain any remaining stdout/stderr to EOF

            if return_code == EXIT_SKIP_CODE:
                # Deterministic unprocessable input — re-raise across the process
                # boundary so the worker records SKIPPED (ack, no retry/DLQ).
                raise UnprocessableInputError(self._extract_skip_reason(stderr_buffer))

            if return_code != 0:
                error_details = (
                    "\n".join(stderr_buffer)
                    if stderr_buffer
                    else "No error details captured"
                )
                raise RuntimeError(
                    f"Processing subprocess failed for {work_unit.image_id} "
                    f"(exit code {return_code}):\n{error_details}"
                )
        finally:
            self._processes.discard(proc)

    @staticmethod
    def _extract_skip_reason(stderr_buffer: "deque[str]") -> str:
        """Pull the subprocess's marked skip reason from its stderr tail."""
        for line in reversed(stderr_buffer):
            if line.startswith(SKIP_REASON_PREFIX):
                return line[len(SKIP_REASON_PREFIX) :]
        return "unprocessable input"

    @staticmethod
    async def _stream_stdout(stream: asyncio.StreamReader | None) -> None:
        """Stream subprocess stdout to the parent logger, line by line."""
        if stream is None:
            return
        async for raw in stream:
            line = raw.decode(errors="replace").rstrip()
            if line:
                logger.info(line)

    @staticmethod
    async def _stream_stderr(
        stream: asyncio.StreamReader | None, buffer: "deque[str]"
    ) -> None:
        """Stream subprocess stderr to the logger and buffer the tail for errors."""
        if stream is None:
            return
        async for raw in stream:
            line = raw.decode(errors="replace").rstrip()
            if line:
                buffer.append(line)
                logger.error("[SUBPROCESS] %s", line)

    async def _kill(self, proc: Process) -> None:
        """SIGKILL the subprocess group and reap it (best-effort)."""
        if proc.returncode is not None:
            return
        self._signal_group(proc, _signal.SIGKILL)
        try:
            await asyncio.wait_for(proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            pass

    def _ensure_dir(self, directory: Path) -> Path:
        """Ensure directory exists and return it."""
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    def _cleanup_directory(self, dir_path: Path) -> None:
        """Safe cleanup of a directory tree."""
        try:
            if dir_path.exists():
                shutil.rmtree(dir_path)
                logger.debug("Cleaned up directory: %s", dir_path)
        except OSError as e:
            logger.warning("Failed to cleanup directory %s: %s", dir_path, e)
