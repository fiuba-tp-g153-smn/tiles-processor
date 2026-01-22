"""
Tiles Processor - Main Entry Point

This application processes GOES-19 satellite imagery using a RabbitMQ work queue.

Modes:
    worker   - Start a worker that consumes and processes work units
    producer - Run the producer once to discover new images and publish work units

Usage:
    python3 src/main.py worker    # Start a worker
    python3 src/main.py producer  # Run producer once (for cron)

The producer is designed to be run periodically (e.g., via cron or systemd timer)
to discover new satellite images and publish work units to the queue.

Workers run continuously, consuming work units and processing them through
the satellite image processing pipeline.
"""

from logging import getLogger
from sys import argv, exit

from config import Config
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
    print("  worker   - Start a worker to process work units from the queue")
    print("  producer - Run the producer to discover and publish new images")
    print()
    print("Examples:")
    print("  python3 src/main.py worker    # Start a worker")
    print("  python3 src/main.py producer  # Discover and publish new images")


def run_worker(config: Config) -> int:
    """Start a worker that processes work units."""
    logger = getLogger(__name__)
    logger.info("Starting worker mode...")

    try:
        start_worker(config)
        return EXIT_SUCCESS_CODE
    except Exception as e:
        logger.exception(f"Worker failed: {e}")
        return EXIT_ERROR_CODE


def run_producer(config: Config) -> int:
    """Run the producer to discover and publish new images."""
    logger = getLogger(__name__)
    logger.info("Starting producer mode...")

    try:
        start_producer(config)
        return EXIT_SUCCESS_CODE
    except Exception as e:
        logger.exception(f"Producer failed: {e}")
        return EXIT_ERROR_CODE


def main() -> int:
    """Main entry point."""
    # Parse command line
    if len(argv) < 2:
        print_usage()
        return EXIT_ERROR_CODE

    mode = argv[1].lower()

    # Setup config and logging
    config = Config()
    setup_logging(config)
    logger = getLogger(__name__)

    config.log_config()

    # Dispatch to appropriate mode
    if mode == "worker":
        return run_worker(config)
    elif mode == "producer":
        return run_producer(config)
    else:
        logger.error(f"Unknown mode: {mode}")
        print_usage()
        return EXIT_ERROR_CODE


if __name__ == "__main__":
    try:
        exit_code = main()
        exit(exit_code)
    except KeyboardInterrupt:
        logger = getLogger(__name__)
        logger.info("Application stopped by user (KeyboardInterrupt).")
        exit(EXIT_SUCCESS_CODE)
