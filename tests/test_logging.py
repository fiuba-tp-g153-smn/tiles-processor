import logging
import pytz
from datetime import datetime
import sys
import os

# Ensure src is in python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from logging_config import TimezoneFormatter


def test_timezone_formatter_utc():
    formatter = TimezoneFormatter(
        fmt="%(asctime)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S%z",
        timezone_str="UTC",
    )
    record = logging.LogRecord("test", logging.INFO, "", 0, "test msg", (), None)
    # Mock created time to a specific timestamp
    # 1609459200 is 2021-01-01 00:00:00 UTC
    record.created = 1609459200.0

    s = formatter.format(record)
    assert "2021-01-01 00:00:00+0000" in s


def test_timezone_formatter_custom_tz():
    # New York is UTC-5 in Jan
    formatter = TimezoneFormatter(
        fmt="%(asctime)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S%z",
        timezone_str="America/New_York",
    )
    record = logging.LogRecord("test", logging.INFO, "", 0, "test msg", (), None)
    # 1609459200 is 2021-01-01 00:00:00 UTC -> 2020-12-31 19:00:00 EST
    record.created = 1609459200.0

    s = formatter.format(record)
    assert "2020-12-31 19:00:00-0500" in s
