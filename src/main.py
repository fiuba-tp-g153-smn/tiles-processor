import asyncio
import logging
import sys
import signal

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

    config.log_config()

    if len(sys.argv) < 2:
        logger.error("Usage: python3 ./src/main.py <job_name|scheduler>")
        return

    job_name = sys.argv[1]

    target_jobs = {}
    if job_name == "scheduler":
        logger.info("Starting scheduler mode for ALL jobs")
        target_jobs = AVAILABLE_JOBS
    else:
        job_class = AVAILABLE_JOBS.get(job_name)
        if not job_class:
            logger.error(f"Job '{job_name}' not found. Available jobs: {list(AVAILABLE_JOBS.keys())}")
            return
        
        logger.info(f"Starting scheduler mode for SINGLE job: {job_name}")
        target_jobs = {job_name: job_class}

    
    # Graceful shutdown setup
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def handle_signal(sig):
        logger.info(f"Received exit signal {sig.name}...")
        stop_event.set()
        # Optionally, we could cancel tasks here if stop_event.wait() wasn't enough,
        # but stop_event is what start_scheduler waits on.

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: handle_signal(s))

    await start_scheduler(target_jobs, stop_event=stop_event)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass # Handle any remaining interrupt that might bubble up if strictly synchronous code ran

