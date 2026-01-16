import asyncio
import logging
import sys
import signal

from config import Config
from logging_config import setup_logging
from jobs.process_band_13_job import ProcessBand13Job
from jobs.process_band_9_job import ProcessBand9Job
from scheduler import start_scheduler
from jobs.heartbeat_job import HeartbeatJob

# Centralized job registry
# Keys match the keys used in config.py for scheduling
AVAILABLE_JOBS = {
    "process_band_13": ProcessBand13Job,
    "process_band_9": ProcessBand9Job,
    "heartbeat": HeartbeatJob,
}

EXIT_ERROR_CODE = 1
EXIT_SUCCESS_CODE = 0


def get_enabled_jobs(config: Config, logger: logging.Logger) -> dict:
    """Filter available jobs based on configuration settings."""
    enabled_jobs = {}

    # Heartbeat is always enabled
    enabled_jobs["heartbeat"] = AVAILABLE_JOBS["heartbeat"]

    if config.ENABLE_BAND_13:
        enabled_jobs["process_band_13"] = AVAILABLE_JOBS["process_band_13"]
    else:
        logger.warning("Band 13 processing is DISABLED in config.")

    if config.ENABLE_BAND_9:
        enabled_jobs["process_band_9"] = AVAILABLE_JOBS["process_band_9"]
    else:
        logger.warning("Band 9 processing is DISABLED in config.")

    return enabled_jobs


def get_scheduler_jobs(config: Config, logger: logging.Logger) -> dict | None:
    logger.info("Starting scheduler mode for ALL jobs")
    target_jobs = get_enabled_jobs(config, logger)

    if not target_jobs:
        logger.error("No jobs are enabled. Exiting.")
        return None
    return target_jobs


def get_single_job(job_name: str, logger: logging.Logger) -> dict | None:
    job_class = AVAILABLE_JOBS.get(job_name)
    if not job_class:
        logger.error(
            f"Job '{job_name}' not found. Available jobs: {list(AVAILABLE_JOBS.keys())}"
        )
        return None

    logger.info(f"Starting scheduler mode for SINGLE job: {job_name}")
    target_jobs = {job_name: job_class}

    # Always run heartbeat for healthchecks, even in single-job mode
    if "heartbeat" in AVAILABLE_JOBS and job_name != "heartbeat":
        target_jobs["heartbeat"] = AVAILABLE_JOBS["heartbeat"]

    return target_jobs


def get_target_jobs(
    config: Config, job_name: str, logger: logging.Logger
) -> dict | None:
    """Determine which jobs to run based on the job name argument.

    Returns None if no valid jobs are found or enabled.
    """
    if job_name == "scheduler":
        return get_scheduler_jobs(config, logger)

    return get_single_job(job_name, logger)


def setup_signal_handlers(
    loop: asyncio.AbstractEventLoop, stop_event: asyncio.Event, logger: logging.Logger
) -> None:
    """Setup graceful shutdown signal handlers."""

    def handle_signal(sig):
        logger.info(f"Received exit signal {sig.name}...")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: handle_signal(s))


async def main() -> int:
    # Setup logging first thing
    config = Config()
    setup_logging(config)
    logger = logging.getLogger(__name__)

    config.log_config()

    if len(sys.argv) < 2:
        logger.error("Usage: python3 ./src/main.py <job_name|scheduler>")
        return EXIT_ERROR_CODE

    job_name = sys.argv[1]

    target_jobs = get_target_jobs(config, job_name, logger)
    if target_jobs is None:
        return EXIT_ERROR_CODE

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    setup_signal_handlers(loop, stop_event, logger)

    await start_scheduler(config, target_jobs, stop_event=stop_event)

    return EXIT_SUCCESS_CODE


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger = logging.getLogger(__name__)
        logger.info("Application stopped by user (KeyboardInterrupt).")
        sys.exit(EXIT_SUCCESS_CODE)
