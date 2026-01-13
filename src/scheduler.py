import asyncio
import logging
from datetime import timezone
from typing import Dict, Type

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from config import config

logger = logging.getLogger(__name__)


async def _run_job(job_cls):
    """
    Generic runner for any job class that has a .run() coroutine.
    """
    try:
        job = job_cls()
        logger.info(f"Starting job: {job_cls.__name__}")
        await job.run()
        logger.info(f"Job finished: {job_cls.__name__}")
    except Exception:
        logger.exception("Job %s failed", job_cls.__name__)


async def start_scheduler(job_registry: Dict[str, Type], stop_event: asyncio.Event = None):
    """
    Start APScheduler with jobs defined in the registry.
    Schedules are pulled from config.py based on the job name.
    """
    scheduler = AsyncIOScheduler(timezone=config.TIMEZONE)
    schedules = config.get_job_schedules()

    for job_name, job_cls in job_registry.items():
        schedule_cron = schedules.get(job_name)
        if not schedule_cron:
            logger.warning(f"No schedule found for job '{job_name}' in config. Skipping.")
            continue

        # We need to capture job_cls correctly in the closure
        # A common way is to use a default argument or a factory, but here
        # we can define a wrapper inside the loop if we are careful,
        # or better: use a helper function to generate the callback.
        
        callback = _create_job_callback(job_cls)
        
        scheduler.add_job(
            callback,
            trigger=CronTrigger.from_crontab(schedule_cron, timezone=config.TIMEZONE),
            id=job_name,
            replace_existing=True
        )
        logger.info(f"Scheduled job '{job_name}' with cron '{schedule_cron}'")

    logger.info("Starting scheduler with %d jobs", len(scheduler.get_jobs()))
    scheduler.start()

    # Keep the scheduler running until cancelled
    if stop_event is None:
        stop_event = asyncio.Event()

    try:
        await stop_event.wait()
    except asyncio.CancelledError:
        logger.info("Scheduler cancelled, shutting down")
    finally:
        scheduler.shutdown(wait=False)


def _create_job_callback(job_cls):
    """
    Helper to create a coroutine function for a specific job class.
    This avoids closure binding issues in loops.
    """
    async def run_specific_job():
        await _run_job(job_cls)
    # Set a nice name for debugging
    run_specific_job.__name__ = f"run_{job_cls.__name__}"
    return run_specific_job
