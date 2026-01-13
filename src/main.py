import asyncio
import logging
import sys

from config import config
from logging_config import setup_logging
from jobs.process_band_13_job import ProcessBand13Job
from jobs.process_band_9_job import ProcessBand9Job
from scheduler import start_scheduler

# Centralized job registry
# Keys match the keys used in config.py for scheduling
AVAILABLE_JOBS = {
    "process_band_13": ProcessBand13Job,
    "process_band_9": ProcessBand9Job,
}

async def main():
    # Setup logging first thing
    setup_logging(log_level=config.LOG_LEVEL)
    logger = logging.getLogger(__name__)

    if len(sys.argv) < 2:
        logger.error("Usage: python3 ./src/main.py <job_name|scheduler>")
        return

    job_name = sys.argv[1]

    # Start scheduler mode
    if job_name == "scheduler":
        logger.info("Starting scheduler mode")
        await start_scheduler(AVAILABLE_JOBS)
        return

    # Run a specific job immediately (one-off)
    job_class = AVAILABLE_JOBS.get(job_name)
    if not job_class:
        logger.error(f"Job '{job_name}' not found. Available jobs: {list(AVAILABLE_JOBS.keys())}")
        return

    logger.info(f"Running manual job: {job_name}")
    job_instance = job_class()
    try:
        await job_instance.run()
        logger.info(f"Manual job '{job_name}' completed successfully.")
    except Exception:
        logger.exception(f"Manual job '{job_name}' failed.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
