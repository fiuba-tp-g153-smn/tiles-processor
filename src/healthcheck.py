import sys
import time
import os
import logging
from pathlib import Path
from config import Config
from logging_config import setup_logging


"""
Application Health Check Script.

This script verifies the system's health by checking the freshness of a sentinel file.

HOW IT WORKS:
1. The Scheduler runs a `HeartbeatJob` periodically (e.g., every 1 minute).
2. The `HeartbeatJob` updates the modification time (mtime) of /tmp/healthy.
3. This script runs periodically (via Docker HEALTHCHECK) to inspect that file.

LOGIC:
- If /tmp/healthy is missing: UNHEALTHY (Scheduler never started?)
- If /tmp/healthy is older than MAX_DELAY_SECONDS: UNHEALTHY (Scheduler stuck?)
- If /tmp/healthy is fresh: HEALTHY

EXIT CODES:
- 0: Healthy
- 1: Unhealthy
"""

# Maximum age of the heartbeat file in seconds (e.g., 2 minutes)
# Logic: Scheduler runs job every 1 min. If > 2 min, something is stuck.
MAX_DELAY_SECONDS = 120
HEALTH_FILE = Path("/app/data/tmp/healthy")

EXIT_ERROR_CODE = 1
EXIT_SUCCESS_CODE = 0


def check_health():
    # Setup logging
    try:
        config = Config()
        setup_logging(config)
    except Exception:
        # Fallback if config fails
        logging.basicConfig(level=logging.INFO)

    logger = logging.getLogger("healthcheck")

    if not HEALTH_FILE.exists():
        logger.error(f"Health check failed: {HEALTH_FILE} does not exist")
        sys.exit(EXIT_ERROR_CODE)

    # Get file modification time
    mtime = HEALTH_FILE.stat().st_mtime
    current_time = time.time()
    age = current_time - mtime

    if age > MAX_DELAY_SECONDS:
        logger.error(
            f"Health check failed: last heartbeat was {age:.1f}s ago (max {MAX_DELAY_SECONDS}s)"
        )
        sys.exit(EXIT_ERROR_CODE)

    logger.info(f"Health check passed: last heartbeat was {age:.1f}s ago")
    sys.exit(EXIT_SUCCESS_CODE)


if __name__ == "__main__":
    check_health()
