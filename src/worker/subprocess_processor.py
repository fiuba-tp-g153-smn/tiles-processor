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
5. Uploads results to MinIO
6. Exits, releasing all memory
"""

import asyncio
import logging
import sys


def create_processor_registry():
    """
    Create and populate the processor registry with all available processors.

    This function imports processors here (not at module level) to ensure
    heavy libraries are only loaded when processing actually starts.
    """
    from processors import (
        ProcessorRegistry,
        GoesProcessor,
        RadarProcessor,
    )
    from processors.ecmwf_precipitation_processor import EcmwfPrecipitationProcessor

    registry = ProcessorRegistry()

    # Register GOES processors (both bands use the same processor class)
    registry.register("goes_band_13", GoesProcessor)
    registry.register("goes_band_9", GoesProcessor)

    # Register Radar processor
    registry.register("radar", RadarProcessor)

    # Register ECMWF processors
    registry.register("ecmwf_ecmwf_total_precipitation", EcmwfPrecipitationProcessor)

    # Add new processors here as they are implemented:
    # registry.register("new_processor_id", NewProcessorClass)

    return registry


def run_processing(work_unit_json: str, file_path: str) -> None:
    """
    Run the heavy image processing in this subprocess.

    Args:
        work_unit_json: JSON string of the WorkUnit
        file_path: Path to the downloaded file to process
    """
    # Import heavy modules here - they'll be unloaded when process exits
    from config import Config
    from models.work_unit import WorkUnit
    from logging_config import setup_logging

    # Initialize config and logging first
    config = Config()
    setup_logging(config)
    logger = logging.getLogger(__name__)

    # Parse work unit
    work_unit = WorkUnit.from_json(work_unit_json)
    logger.info(f"[SUBPROCESS] Starting processing for {work_unit.image_id}")
    logger.info(f"[SUBPROCESS] Processor: {work_unit.processor_id}")

    # Create processor registry and get the appropriate processor
    registry = create_processor_registry()

    try:
        processor_class = registry.get(work_unit.processor_id)
    except KeyError as e:
        logger.error(f"[SUBPROCESS] {e}")
        raise

    # Instantiate and run the processor
    processor = processor_class(config)
    logger.info(
        f"[SUBPROCESS] Using {processor_class.__name__} for {work_unit.processor_id}"
    )

    # Run processing
    asyncio.run(processor.process(file_path, work_unit))

    logger.info(f"[SUBPROCESS] Completed processing {work_unit.image_id}")


def main() -> int:
    """Entry point for subprocess execution."""
    if len(sys.argv) != 3:
        print(
            "Usage: python -m worker.subprocess_processor <work_unit_json> <file_path>",
            file=sys.stderr,
        )
        return 1

    work_unit_json = sys.argv[1]
    file_path = sys.argv[2]

    try:
        run_processing(work_unit_json, file_path)
        return 0
    except Exception as e:
        # Log the error (will go to stderr which parent captures)
        logging.error(f"[SUBPROCESS] Processing failed: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
