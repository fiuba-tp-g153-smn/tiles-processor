"""Tests for parse_barb_zoom_strides / BarbZoomStrides."""

import pytest

from models.barb_config import parse_barb_zoom_strides

_DEFAULT = {2: 150, 4: 38, 6: 16, 8: 9}


def test_parses_json_string_keys_into_zoom_ints():
    strides = parse_barb_zoom_strides({"2": 150, "8": 9}, "x", default=_DEFAULT)
    assert strides.items() == ((2, 150), (8, 9))
    assert strides.zooms == {2, 8}


def test_entries_are_sorted_by_zoom():
    """Settings order must not leak into the emission order."""
    strides = parse_barb_zoom_strides(
        {"8": 9, "2": 150, "6": 16}, "x", default=_DEFAULT
    )
    assert [zoom for zoom, _stride in strides.items()] == [2, 6, 8]


def test_uses_default_when_unset():
    strides = parse_barb_zoom_strides(None, "x", default=_DEFAULT)
    assert strides.zooms == {2, 4, 6, 8}
    assert strides.stride_for(8) == 9


def test_stride_for_unconfigured_zoom_raises():
    strides = parse_barb_zoom_strides({"4": 38}, "x", default=_DEFAULT)
    with pytest.raises(KeyError, match="zoom 8"):
        strides.stride_for(8)


def test_rejects_empty_mapping():
    with pytest.raises(ValueError, match="non-empty"):
        parse_barb_zoom_strides({}, "sources.gfs.barb_zoom_strides", default=_DEFAULT)


def test_rejects_non_mapping():
    with pytest.raises(ValueError, match="non-empty"):
        parse_barb_zoom_strides("2:150", "x", default=_DEFAULT)


def test_rejects_non_integer_zoom_key():
    with pytest.raises(ValueError, match="non-integer zoom key"):
        parse_barb_zoom_strides({"z8": 9}, "x", default=_DEFAULT)


@pytest.mark.parametrize("stride", [0, -1, 1.5, "9", True])
def test_rejects_degenerate_stride(stride):
    """A stride below 1 (or a non-int) would subsample nonsensically."""
    with pytest.raises(ValueError, match="stride must be an int"):
        parse_barb_zoom_strides({"8": stride}, "x", default=_DEFAULT)


def test_is_immutable():
    strides = parse_barb_zoom_strides({"8": 9}, "x", default=_DEFAULT)
    with pytest.raises(AttributeError):
        strides.entries = ()
