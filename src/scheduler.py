import asyncio
import logging
import os
from pathlib import Path
from datetime import timezone
from typing import Dict, Type

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from config import config

logger = logging.getLogger(__name__)


def _get_directory_size(path: Path) -> int:
    """Calculate the total size of a directory in bytes."""
    total_size = 0
    if not path.exists():
        return 0
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            # skip if it is symbolic link
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)
    return total_size


async def _run_job(job_cls):
    """
    Generic runner for any job class that has a .run() coroutine.
    """
    # Check tmp dir size
    tmp_path = Path.cwd() / config.TMP_DIR
    current_size = _get_directory_size(tmp_path)
    
    if current_size > config.MAX_TMP_DIR_SIZE_BYTES:
        logger.error(
            f"Job {job_cls.__name__} cancelled. "
            f"Temporary directory {tmp_path} size ({current_size / (1024**3):.2f} GB) "
            f"exceeds limit ({config.MAX_TMP_DIR_SIZE_BYTES / (1024**3):.2f} GB)."
        )
        return

    try:
        job = job_cls()
        logger.info(f"Starting job: {job_cls.__name__}")
        await job.run()
        logger.info(f"Job finished: {job_cls.__name__}")
    except Exception:
        logger.exception("Job %s failed", job_cls.__name__)



async def _worker(queue: asyncio.Queue):
    """
    Worker coroutine that processes jobs one by one from the queue.
    """
    while True:
        job_func, job_name = await queue.get()
        try:
            # Re-check or check size limit here?
            # It's safer to check it right before execution in case the queue is long
            # But the _run_job already has the check.
            # We just need to call the function.
            remaining = queue.qsize()
            logger.info("Worker picked up job: %s. Queue size: %d pending jobs.", job_name, remaining)
            await job_func()
        except Exception:
            logger.exception("Worker failed to process job: %s", job_name)
        finally:
            queue.task_done()


async def start_scheduler(job_registry: Dict[str, Type], stop_event: asyncio.Event = None):
    """
    Start APScheduler with jobs defined in the registry.
    Schedules are pulled from config.py based on the job name.
    """
    scheduler = AsyncIOScheduler(timezone=config.TIMEZONE)
    schedules = config.get_job_schedules()
    
    # Initialize Queue
    job_queue = asyncio.Queue()

    for job_name, job_cls in job_registry.items():
        schedule_cron = schedules.get(job_name)
        if not schedule_cron:
            logger.warning(f"No schedule found for job '{job_name}' in config. Skipping.")
            continue

        # Create callback that enqueues the job instead of running it immediately
        callback = _create_enqueue_callback(job_queue, job_cls, job_name)
        
        scheduler.add_job(
            callback,
            trigger=CronTrigger.from_crontab(schedule_cron, timezone=config.TIMEZONE),
            id=job_name,
            replace_existing=True
        )
        logger.info(f"Scheduled job '{job_name}' with cron '{schedule_cron}'")

    logger.info("Starting scheduler with %d jobs", len(scheduler.get_jobs()))
    scheduler.start()

    # Start Worker
    worker_task = asyncio.create_task(_worker(job_queue))
    logger.info("Worker task started")

    # Keep the scheduler running until cancelled
    if stop_event is None:
        stop_event = asyncio.Event()

    try:
        await stop_event.wait()
    except asyncio.CancelledError:
        logger.info("Scheduler cancelled, shutting down")
    finally:
        scheduler.shutdown(wait=False)
        # Cancel worker
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass




def _create_enqueue_callback(queue: asyncio.Queue, job_cls, job_name: str):
    """
    Helper to create a callback that puts the job into the queue.
    """
    async def enqueue_job():
        # define the actual run coroutine
        async def run_specific_job():
             await _run_job(job_cls)
        
        logger.info(f"Enqueuing job: {job_name}")
        await queue.put((run_specific_job, job_name))
        
    enqueue_job.__name__ = f"enqueue_{job_name}"
    return enqueue_job

