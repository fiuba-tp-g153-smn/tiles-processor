"""
APScheduler-based Job Scheduler with SQLite Persistence.

This module provides a simplified scheduler using APScheduler's built-in
features for job management, with SQLite persistence for surviving restarts.

Key APScheduler Features Used:
    - SQLAlchemyJobStore: Persists jobs to SQLite database
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

from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from config import config

logger = logging.getLogger(__name__)

# Grace time for misfired jobs (5 minutes)
MISFIRE_GRACE_TIME = 300

# Global job registry - populated by start_scheduler, used by run_job
# This allows the module-level run_job function to look up job classes
_job_registry: Dict[str, Type] = {}


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


async def run_job(job_name: str):
    """
    Execute a job by name. This is a module-level function for APScheduler serialization.

    APScheduler with SQLite persistence requires jobs to be serializable. This function
    is defined at module level so it can be referenced as 'scheduler:run_job' and
    serialized properly. The job_name is passed as an argument and used to look up
    the actual job class from the global registry.

    Args:
        job_name: Name of the job to execute (must be in _job_registry)
    """
    # Check tmp directory size before execution
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
        return

    job_cls = _job_registry.get(job_name)
    if not job_cls:
        logger.error("Job %s not found in registry", job_name)
        return

    try:
        job = job_cls()
        logger.info("Starting job: %s", job_name)
        await job.run()
        logger.info("Job completed: %s", job_name)
    except Exception:
        logger.exception("Job %s failed with error", job_name)


def _create_job_runner(job_cls: Type, job_name: str):
    """
    Create an async job runner function for APScheduler (non-persistent mode).

    This is used when testing without persistence. For persistent job stores,
    the module-level run_job function is used instead.

    Args:
        job_cls: The job class to instantiate and run
        job_name: Human-readable job name for logging

    Returns:
        Async function that APScheduler can execute
    """
    async def _run_job():
        # Check tmp directory size before execution
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
            return

        try:
            job = job_cls()
            logger.info("Starting job: %s", job_name)
            await job.run()
            logger.info("Job completed: %s", job_name)
        except Exception:
            logger.exception("Job %s failed with error", job_name)

    # Set function name for APScheduler logging
    _run_job.__name__ = f"run_{job_name}"
    return _run_job


async def start_scheduler(job_registry: Dict[str, Type], stop_event: asyncio.Event):
    """
    Start APScheduler with jobs defined in the registry.

    Uses APScheduler best practices:
        - max_instances=1: Prevents job overlap (same effect as queue)
        - coalesce=True: Merges missed executions
        - misfire_grace_time: Handles delayed starts gracefully

    Args:
        job_registry: Dict mapping job names to job classes
        stop_event: Event to signal graceful shutdown

    Example:
        job_registry = {
            "process_band_13": ProcessBand13Job,
            "process_band_9": ProcessBand9Job,
        }
        await start_scheduler(job_registry, stop_event)
    """
    global _job_registry
    _job_registry = job_registry

    # Configure SQLite job store for persistence
    db_path = Path(config.SCHEDULER_DB_PATH)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    jobstores = {
        'default': SQLAlchemyJobStore(url=f'sqlite:///{db_path}')
    }

    scheduler = AsyncIOScheduler(
        jobstores=jobstores,
        timezone=config.TIMEZONE
    )
    schedules = config.get_job_schedules()

    logger.info("Using persistent job store at: %s", db_path)

    for job_name, job_cls in job_registry.items():
        schedule_cron = schedules.get(job_name)
        if not schedule_cron:
            logger.warning("No schedule found for job '%s'. Skipping.", job_name)
            continue

        # Add job using module-level run_job function with job_name as argument
        # This allows APScheduler to serialize the job for SQLite persistence
        scheduler.add_job(
            run_job,                  # Module-level function (serializable)
            trigger=CronTrigger.from_crontab(schedule_cron, timezone=config.TIMEZONE),
            args=[job_name],          # Pass job_name as argument
            id=job_name,
            name=job_name,
            max_instances=1,          # Prevent overlapping runs
            coalesce=True,            # Merge missed runs into single execution
            misfire_grace_time=MISFIRE_GRACE_TIME,  # Allow delayed execution
            replace_existing=True,    # Update if job already exists
        )
        logger.info("Scheduled job '%s' with cron '%s'", job_name, schedule_cron)

    logger.info("Starting scheduler with %d jobs", len(scheduler.get_jobs()))
    scheduler.start()

    try:
        await stop_event.wait()
    except asyncio.CancelledError:
        logger.info("Scheduler received cancellation signal")
        raise
    finally:
        logger.info("Shutting down scheduler...")
        scheduler.shutdown(wait=False)
        logger.info("Scheduler shutdown complete")
