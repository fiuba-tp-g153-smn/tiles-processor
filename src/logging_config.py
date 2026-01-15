import logging
import sys
from datetime import datetime
import pytz

from config import config


class TimezoneFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, style="%", timezone_str="UTC"):
        super().__init__(fmt, datefmt, style)
        try:
            self.timezone = pytz.timezone(timezone_str)
        except pytz.UnknownTimeZoneError:
            self.timezone = pytz.UTC

    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, self.timezone)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat()


def setup_logging(log_level: str = "INFO"):
    """
    Configures the root logger with a consistent format and configured timezone.
    """
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        print(f"Invalid log level: {log_level}. Defaulting to INFO.")
        numeric_level = logging.INFO

    # Create handler
    handler = logging.StreamHandler(sys.stdout)

    # Create formatter with configured timezone
    formatter = TimezoneFormatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        timezone_str=config.TIMEZONE,
    )
    handler.setFormatter(formatter)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)

    # Remove existing handlers to avoid duplicates
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    root_logger.addHandler(handler)

    # Silence noisy libraries if necessary
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("aiobotocore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
