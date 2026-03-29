"""
Subprocess processor for memory-isolated image processing.

This module runs in a separate process to isolate heavy library memory usage.
When the subprocess exits, all memory from pyproj, rioxarray, GDAL, etc. is reclaimed.

Usage:
    python -m worker.subprocess_processor <work_unit_json> <file_path>

The subprocess:
1. Creates a processor registry with all available processors
2. Selects the appropriate processor based on work_unit.processor_id
3. Loads heavy libraries (pyproj, rioxarray, GDAL stack)
4. Processes the downloaded file
5. Uploads results to S3
6. Exits, releasing all memory
"""

import asyncio
import logging
import signal
import sys
import traceback

from processors.base_processor import ShutdownRequested

EXIT_ERROR_CODE = 1
EXIT_SUCCESS_CODE = 0


def create_processor_registry():
    """
    Create and populate the processor registry with all available processors.

    This function imports processors here (not at module level) to ensure
    heavy libraries are only loaded when processing actually starts.
    """
    # pylint: disable=import-outside-toplevel
    from processors import (
        ProcessorRegistry,
        GoesProcessor,
        Band2Processor,
        GlmFedProcessor,
        RadarProcessor,
        EcmwfPeriodProcessor,
    )

    registry = ProcessorRegistry()

    # Register GOES processors (both bands use the same processor class)
    registry.register("goes_band_13", GoesProcessor)
    registry.register("goes_band_9", GoesProcessor)

    # Register Band 2 processor (downsampled visible imagery)
    registry.register("goes_band_2", Band2Processor)

    # Register GLM processor (lightning products)
    registry.register("glm_fed", GlmFedProcessor)

    # Register Radar processor
    registry.register("radar", RadarProcessor)

    # Register ECMWF period processor (subprocess for scientific processing)
    registry.register("ecmwf_period_processor", EcmwfPeriodProcessor)

    return registry


def run_processing(work_unit_json: str, file_path: str) -> None:
    """
    Run the heavy image processing in this subprocess.

    Args:
        work_unit_json: JSON string of the WorkUnit
        file_path: Path to the downloaded file to process
    """
    # Import heavy modules here - they'll be unloaded when process exits
    # pylint: disable=import-outside-toplevel
    from config import Config
    from models.work_unit import WorkUnit
    from logging_config import setup_logging

    # Initialize config and logging first
    config = Config()
    setup_logging(config)
    logger = logging.getLogger(__name__)

    # Parse work unit
    work_unit = WorkUnit.from_json(work_unit_json)
    logger.info("[SUBPROCESS] Starting processing for %s", work_unit.image_id)
    logger.info("[SUBPROCESS] Processor: %s", work_unit.processor_id)

    # Create processor registry and get the appropriate processor
    registry = create_processor_registry()

    try:
        processor_class = registry.get(work_unit.processor_id)
    except KeyError as e:
        logger.error("[SUBPROCESS] %s", e)
        raise

    # Instantiate and run the processor
    processor = processor_class(config)
    logger.info(
        "[SUBPROCESS] Using %s for %s",
        processor_class.__name__,
        work_unit.processor_id,
    )

    # Install signal handler so SIGTERM triggers graceful shutdown
    # between processing steps instead of killing the process immediately
    signal.signal(signal.SIGTERM, lambda _sig, _frame: processor.request_shutdown())
    signal.signal(signal.SIGINT, lambda _sig, _frame: processor.request_shutdown())

    # Run processing
    asyncio.run(processor.process(file_path, work_unit))

    logger.info("[SUBPROCESS] Completed processing %s", work_unit.image_id)


def main() -> int:
    """Entry point for subprocess execution."""
    if len(sys.argv) != 3:
        print(
            "Usage: python -m worker.subprocess_processor <work_unit_json> <file_path>",
            file=sys.stderr,
        )
        return EXIT_ERROR_CODE

    work_unit_json = sys.argv[1]
    file_path = sys.argv[2]

    try:
        run_processing(work_unit_json, file_path)
        return EXIT_SUCCESS_CODE

    except ShutdownRequested:
        logging.info("[SUBPROCESS] Shutdown requested, exiting gracefully")
        return EXIT_ERROR_CODE

    except Exception as e:  # pylint: disable=broad-exception-caught
        # Log the error (will go to stderr which parent captures)
        logging.error("[SUBPROCESS] Processing failed: %s", e)
        traceback.print_exc()
        return EXIT_ERROR_CODE


if __name__ == "__main__":
    sys.exit(main())
