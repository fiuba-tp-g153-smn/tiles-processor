"""
Application Health Check Script.

This script verifies the worker's health by checking the freshness of a sentinel file.

HOW IT WORKS:
1. The Worker updates the heartbeat file after processing each message.
2. This script runs periodically (via Docker HEALTHCHECK) to inspect that file.

LOGIC:
- If /tmp/healthy is missing: UNHEALTHY (Worker never started?)
- If /tmp/healthy is older than MAX_DELAY_SECONDS: UNHEALTHY (Worker stuck?)
- If /tmp/healthy is fresh: HEALTHY

EXIT CODES:
- 0: Healthy
- 1: Unhealthy
"""

import sys
import time
import logging
from pathlib import Path
from config import Config
from logging_config import setup_logging

# Maximum age of the heartbeat file in seconds
# Workers should update this after each processed message
# Allow 5 minutes since satellite images take time to process
MAX_DELAY_SECONDS = 300
HEALTH_FILE = Path("/app/data/tmp/healthy")

EXIT_ERROR_CODE = 1
EXIT_SUCCESS_CODE = 0


def check_health():
    """Check worker health by inspecting the heartbeat file freshness."""
    # Setup logging
    try:
        config = Config()
        setup_logging(config)
    except Exception:  # pylint: disable=broad-exception-caught
        # Fallback if config fails
        logging.basicConfig(level=logging.INFO)

    logger = logging.getLogger("healthcheck")

    if not HEALTH_FILE.exists():
        logger.error("Health check failed: %s does not exist", HEALTH_FILE)
        sys.exit(EXIT_ERROR_CODE)

    # Get file modification time
    mtime = HEALTH_FILE.stat().st_mtime
    current_time = time.time()
    age = current_time - mtime

    if age > MAX_DELAY_SECONDS:
        logger.error(
            "Health check failed: last heartbeat was %.1fs ago (max %ds)",
            age,
            MAX_DELAY_SECONDS,
        )
        sys.exit(EXIT_ERROR_CODE)

    logger.info("Health check passed: last heartbeat was %.1fs ago", age)
    sys.exit(EXIT_SUCCESS_CODE)


if __name__ == "__main__":
    check_health()
