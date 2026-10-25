"""Tests for the date-partitioned S3 key template helpers."""

from datetime import datetime, timedelta, timezone

import pytest

from models.listing_window import ListingWindow
from models.s3_key_template import expand_time_prefixes, validate_key_template


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "template",
    ["{yyyy}/{mm}/{dd}/", "*/{yyyy}{mm}{dd}/{HH}/", "{yyyy}/{jjj}/", "*/", "static/"],
)
def test_valid_templates_pass(template):
    validate_key_template(template)


@pytest.mark.parametrize(
    "template,message",
    [
        ("{yyyy}/{month}/", "unknown placeholder"),
        ("{}/", "unknown placeholder"),
        ("RMA*/{yyyy}/", "whole path segment"),
        ("{yyyy/", "'}'"),
    ],
)
def test_invalid_templates_fail(template, message):
    with pytest.raises(ValueError, match=message):
        validate_key_template(template)


def test_daily_template_lists_each_day_once():
    window = ListingWindow(utc(2026, 10, 25, 11, 30), utc(2026, 10, 25, 14, 0))
    assert expand_time_prefixes("{yyyy}/{mm}/{dd}/", window) == ["2026/10/25/"]


def test_window_across_midnight_lists_both_days():
    window = ListingWindow(utc(2026, 10, 24, 22, 30), utc(2026, 10, 25, 1, 0))
    assert expand_time_prefixes("*/{yyyy}{mm}{dd}/", window) == [
        "*/20261024/",
        "*/20261025/",
    ]


def test_hourly_template_lists_every_touched_hour():
    window = ListingWindow(utc(2026, 10, 25, 11, 30), utc(2026, 10, 25, 14, 0))
    assert expand_time_prefixes("{yyyy}{jjj}/{HH}/", window) == [
        "2026298/11/",
        "2026298/12/",
        "2026298/13/",
        "2026298/14/",
    ]


def test_non_utc_window_is_expanded_in_utc():
    """Partitions follow the UTC scan time in the filename, not local time."""
    art = timezone(timedelta(hours=-3))
    window = ListingWindow(
        datetime(2026, 10, 24, 22, 0, tzinfo=art),
        datetime(2026, 10, 24, 22, 30, tzinfo=art),
    )
    assert expand_time_prefixes("{yyyy}{mm}{dd}/{HH}/", window) == ["20261025/01/"]
