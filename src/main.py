"""
Tiles Processor - Main Entry Point

This application processes GOES-19 satellite imagery using a RabbitMQ work queue.

Modes:
    worker    - Start a worker that consumes and processes work units
    producer  - Run the producer once to discover new images and publish work units
    dashboard - Start the backoffice performance dashboard web server
    migrate   - Apply database schema migrations (Alembic) and exit

Usage:
    python3 src/main.py worker     # Start a worker
    python3 src/main.py producer   # Run producer once (for cron)
    python3 src/main.py dashboard  # Start the metrics dashboard
    python3 src/main.py migrate    # Upgrade the SQLite databases to head

The producer is designed to be run periodically (e.g., via cron or systemd timer)
to discover new satellite images and publish work units to the queue.

Workers run continuously, consuming work units and processing them through
the satellite image processing pipeline.
"""

import sys
from logging import getLogger

from config import Config
from db.migrate import ensure_migrations
from logging_config import setup_logging
from producer.image_discovery_producer import run_producer as start_producer
from worker.worker import run_worker as start_worker

EXIT_ERROR_CODE = 1
EXIT_SUCCESS_CODE = 0


def print_usage():
    """Print usage information."""
    print("Usage: python3 src/main.py <mode>")
    print()
    print("Modes:")
    print("  worker    - Start a worker to process work units from the queue")
    print("  producer  - Run the producer to discover and publish new images")
    print("  dashboard - Start the backoffice performance dashboard web server")
    print("  migrate   - Apply database schema migrations (Alembic) and exit")
    print()
    print("Examples:")
    print("  python3 src/main.py worker     # Start a worker")
    print("  python3 src/main.py producer   # Discover and publish new images")
    print("  python3 src/main.py dashboard  # Start the metrics dashboard")
    print("  python3 src/main.py migrate    # Upgrade the SQLite databases to head")


def run_worker(config: Config) -> int:
    """Start a worker that processes work units."""
    logger = getLogger(__name__)
    logger.info("Starting worker mode...")

    try:
        start_worker(config)
        return EXIT_SUCCESS_CODE
    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.exception("Worker failed: %s", e)
        return EXIT_ERROR_CODE


def run_producer(config: Config) -> int:
    """Run the producer to discover and publish new images."""
    logger = getLogger(__name__)
    logger.info("Starting producer mode...")

    try:
        start_producer(config)
        return EXIT_SUCCESS_CODE
    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.exception("Producer failed: %s", e)
        return EXIT_ERROR_CODE


def run_dashboard(config: Config) -> int:
    """Start the backoffice performance dashboard web server."""
    logger = getLogger(__name__)
    logger.info("Starting dashboard mode...")

    try:
        # Imported lazily so worker/producer don't require FastAPI/uvicorn.
        # pylint: disable=import-outside-toplevel
        from dashboard.server import run_dashboard as start_dashboard

        start_dashboard(config)
        return EXIT_SUCCESS_CODE
    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.exception("Dashboard failed: %s", e)
        return EXIT_ERROR_CODE


def run_migrate(config: Config) -> int:
    """Apply database schema migrations and exit."""
    logger = getLogger(__name__)
    logger.info("Starting migrate mode...")

    try:
        ensure_migrations(config)
        logger.info("Database migrations applied successfully")
        return EXIT_SUCCESS_CODE
    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.exception("Migration failed: %s", e)
        return EXIT_ERROR_CODE


def main() -> int:
    """Main entry point."""
    # Parse command line
    if len(sys.argv) < 2:
        print_usage()
        return EXIT_ERROR_CODE

    mode = sys.argv[1].lower()

    # Setup config and logging
    config = Config()
    setup_logging(config)
    logger = getLogger(__name__)

    config.log_config()

    # Dispatch to appropriate mode
    match mode:
        case "worker":
            return run_worker(config)
        case "producer":
            return run_producer(config)
        case "dashboard":
            return run_dashboard(config)
        case "migrate":
            return run_migrate(config)
        case _:
            logger.error("Unknown mode: %s", mode)
            print_usage()
            return EXIT_ERROR_CODE


if __name__ == "__main__":
    try:
        EXIT_CODE = main()
        sys.exit(EXIT_CODE)
    except KeyboardInterrupt:
        getLogger(__name__).info("Application stopped by user (KeyboardInterrupt).")
        sys.exit(EXIT_SUCCESS_CODE)
