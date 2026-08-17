"""Tests for parse_zoom_levels / ZoomLevels."""

import pytest

from models.zoom_config import parse_zoom_levels


def test_parses_min_max_and_derives_spec():
    z = parse_zoom_levels("4-9", "x", default="3-7")
    assert (z.min_zoom, z.max_zoom) == (4, 9)
    assert z.spec == "4-9"


def test_uses_default_when_unset():
    z = parse_zoom_levels(None, "x", default="3-7")
    assert z.spec == "3-7"
    assert z.max_zoom == 7


def test_single_zoom_range_is_allowed():
    z = parse_zoom_levels("5-5", "x", default="3-7")
    assert z.min_zoom == z.max_zoom == 5


def test_rejects_malformed_spec():
    with pytest.raises(ValueError, match="MIN-MAX"):
        parse_zoom_levels("3..7", "sources.goes19.zoom_levels", default="3-7")


def test_rejects_non_string():
    with pytest.raises(ValueError, match="MIN-MAX"):
        parse_zoom_levels(7, "x", default="3-7")


def test_rejects_inverted_range():
    with pytest.raises(ValueError, match="must not exceed"):
        parse_zoom_levels("7-3", "sources.radar.zoom_levels", default="4-9")
