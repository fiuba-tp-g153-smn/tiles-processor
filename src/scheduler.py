"""
APScheduler-based Job Scheduler with SQLite Persistence.

This module provides a simplified scheduler using APScheduler's built-in
features for job management, with SQLite persistence for surviving restarts.

Key APScheduler Features Used:
    - SQLAlchemyJobStore: Persists jobs to SQLite database
    - ProcessPoolExecutor: Runs jobs in separate processes for CPU isolation
    - max_instances=1: Prevents overlapping executions of the same job
    - coalesce=True: Merges missed runs into a single execution
    - misfire_grace_time: Allows delayed execution within a grace period
    - replace_existing=True: Updates job if it already exists

Persistence:
    Jobs are stored in a SQLite database. When the scheduler restarts:
    - Existing job schedules are restored from the database
    - Misfired jobs (missed during downtime) are handled per coalesce/grace settings
    - The database file should be on a mounted volume for container persistence

This design ensures:
    - Jobs don't overlap (no concurrent runs of the same job type)
    - Missed schedules are handled gracefully
    - Job state survives container/application restarts
"""

import asyncio
import logging
import os
from pathlib import Path
from typing import Dict, Type

from apscheduler.executors.pool import ProcessPoolExecutor
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from config import Config
from logging_config import setup_logging

logger = logging.getLogger(__name__)

# Grace time for misfired jobs (5 minutes)
MISFIRE_GRACE_TIME = 300


def _get_directory_size(path: Path) -> int:
    """Calculate the total size of a directory in bytes."""
    total_size = 0
    if not path.exists():
        return 0
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            # Skip symbolic links
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)
    return total_size


def check_tmp_dir_usage(config: Config, job_name: str) -> bool:
    """Check if temp directory usage is within limits. Returns True if OK, False if limit exceeded."""
    tmp_path = Path.cwd() / config.TMP_DIR
    current_size = _get_directory_size(tmp_path)

    if current_size > config.MAX_TMP_DIR_SIZE_BYTES:
        logger.error(
            "Job %s skipped: temp directory %s size (%.2f GB) exceeds limit (%.2f GB)",
            job_name,
            tmp_path,
            current_size / (1024**3),
            config.MAX_TMP_DIR_SIZE_BYTES / (1024**3),
        )
        return False
    return True


def run_job(job_name: str, job_cls: Type):
    """
    Execute a job by name. This is a module-level function for APScheduler serialization.

    APScheduler with SQLite persistence requires jobs to be serializable. This function
    is defined at module level so it can be referenced as 'scheduler:run_job' and
    serialized properly.

    Note: This function is synchronous to be executed in a ProcessPoolExecutor.
    The actual job logic is async, so we use asyncio.run() to execute it.

    Args:
        job_name: Name of the job to execute
        job_cls: The job class to instantiate and run (passed explicitly for process safety)
    """
    # Each worker process creates its own config from env vars and settings.json
    config = Config()

    # Initialize logging in the worker process
    setup_logging(config)

    if not check_tmp_dir_usage(config, job_name):
        return

    try:
        job = job_cls()
        logger.info("Starting job: %s", job_name)
        asyncio.run(job.run())
        logger.info("Job completed: %s", job_name)
    except Exception:
        logger.exception("Job %s failed with error", job_name)


def log_scheduled_jobs(scheduler: AsyncIOScheduler):
    """Log the next run time for all scheduled jobs for better observability."""
    for job in scheduler.get_jobs():
        if job.next_run_time:
            logger.info(
                "Job '%s' next run scheduled for: %s",
                job.id,
                job.next_run_time.isoformat(),
            )
        else:
            logger.info("Job '%s' is paused or has no next run time", job.id)


async def start_scheduler(
    config: Config, job_registry: Dict[str, Type], stop_event: asyncio.Event
):
    """
    Start APScheduler with jobs defined in the registry.

    Uses APScheduler best practices:
        - max_instances=1: Prevents job overlap (same effect as queue)
        - coalesce=True: Merges missed executions
        - misfire_grace_time: Handles delayed starts gracefully

    Args:
        config: Application configuration
        job_registry: Dict mapping job names to job classes
        stop_event: Event to signal graceful shutdown

    Example:
        job_registry = {
            "process_band_13": ProcessBand13Job,
            "process_band_9": ProcessBand9Job,
        }
        await start_scheduler(config, job_registry, stop_event)
    """
    # Configure SQLite job store for persistence
    db_path = Path(config.SCHEDULER_DB_PATH)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    jobstores = {"default": SQLAlchemyJobStore(url=f"sqlite:///{db_path}")}

    # allocate 1 process per CPU core
    executors = {"default": ProcessPoolExecutor(max_workers=os.cpu_count())}

    scheduler = AsyncIOScheduler(
        jobstores=jobstores, executors=executors, timezone=config.TIMEZONE
    )

    # Start scheduler in paused state to load jobs from the database
    # This allows scheduler.get_job() to correctly detect existing jobs
    scheduler.start(paused=True)

    schedules = config.get_job_schedules()

    logger.info("Using persistent job store at: %s", db_path)

    for job_name, job_cls in job_registry.items():
        schedule_cron = schedules.get(job_name)
        if not schedule_cron:
            logger.warning("No schedule found for job '%s'. Skipping.", job_name)
            continue

        # Check if job already exists in the store
        # If it doesn't exist, we schedule it to run immediately (in addition to the cron schedule)
        # to ensure the system starts processing right away on first deployment.
        existing_job = scheduler.get_job(job_name)

        if not existing_job:
            logger.info(
                "Job '%s' not found in store. Scheduling separate one-off job for immediate execution.",
                job_name,
            )
            # Schedule a separate one-off job for immediate execution
            # We use the same run_job function but with a different ID
            scheduler.add_job(
                run_job,
                args=[job_name, job_cls],
                id=f"{job_name}_startup",
                name=f"{job_name} (Startup)",
                misfire_grace_time=MISFIRE_GRACE_TIME,
                replace_existing=True,
            )

        # Add recurring job using module-level run_job function with job_name as argument
        # This allows APScheduler to serialize the job for SQLite persistence
        scheduler.add_job(
            run_job,  # Module-level function (serializable)
            trigger=CronTrigger.from_crontab(schedule_cron, timezone=config.TIMEZONE),
            args=[job_name, job_cls],  # Pass class explicitly for process safety
            id=job_name,
            name=job_name,
            max_instances=1,  # Prevent overlapping runs
            coalesce=True,  # Merge missed runs into single execution
            misfire_grace_time=MISFIRE_GRACE_TIME,  # Allow delayed execution
            replace_existing=True,  # Update if job already exists
        )
        logger.info("Scheduled job '%s' with cron '%s'", job_name, schedule_cron)

    logger.info("Resuming scheduler with %d jobs", len(scheduler.get_jobs()))
    scheduler.resume()

    log_scheduled_jobs(scheduler)

    try:
        await stop_event.wait()
    except asyncio.CancelledError:
        logger.info("Scheduler received cancellation signal")
        raise
    finally:
        logger.info("Shutting down scheduler...")
        scheduler.shutdown(wait=False)
        logger.info("Scheduler shutdown complete")
