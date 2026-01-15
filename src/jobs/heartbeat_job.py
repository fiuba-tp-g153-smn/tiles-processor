from pathlib import Path
import logging
import time

logger = logging.getLogger(__name__)


class HeartbeatJob:
    """
    Simple job to update a heartbeat file.

    MECHANISM OF ACTION:
    --------------------
    This job runs frequently (configured in config.py, default: every minute)
    and simply 'touches' a known file path (/tmp/healthy).

    Touching the file updates its modification timestamp (mtime).

    This timestamp serves as a "proof of life" for the scheduler.
    If the scheduler is running and healthy, this job will execute on schedule,
    keeping the file's timestamp fresh (e.g., < 1 minute old).

    If the scheduler hangs, crashes, or gets stuck (e.g., deadlock, resource exhaustion),
    this job will stop running. The file's timestamp will then "age".

    External health checks (like src/healthcheck.py or Docker HEALTHCHECK)
    monitor this file's age to determine if the application needs restarting.
    """

    HEALTH_FILE = Path("/app/data/tmp/healthy")

    async def run(self):
        try:
            # Update access and modification times
            self.HEALTH_FILE.touch()
            logger.info(f"Heartbeat updated: {self.HEALTH_FILE} at {time.ctime()}")
        except Exception as e:
            logger.error(f"Failed to update heartbeat: {e}")
