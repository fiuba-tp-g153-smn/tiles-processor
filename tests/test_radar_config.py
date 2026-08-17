"""Tests for RadarStationFilter parsing and predicate semantics."""

import pytest

from models.radar_config import RadarStationFilter


def test_from_settings_none_defaults_to_all():
    rf = RadarStationFilter.from_settings(None)
    assert rf.mode == "all"
    assert rf.allows("RMA1")
    assert rf.allows("RMA99")


def test_from_settings_string_all():
    rf = RadarStationFilter.from_settings("all")
    assert rf.mode == "all"
    assert rf.allows("RMA1")


def test_from_settings_string_none():
    rf = RadarStationFilter.from_settings("none")
    assert rf.mode == "none"
    assert not rf.allows("RMA1")


def test_from_settings_whitelist():
    rf = RadarStationFilter.from_settings({"whitelist": ["RMA1", "RMA2"]})
    assert rf.mode == "whitelist"
    assert rf.stations == frozenset({"RMA1", "RMA2"})
    assert rf.allows("RMA1")
    assert not rf.allows("RMA3")


def test_from_settings_blacklist_covers_new_stations():
    rf = RadarStationFilter.from_settings({"blacklist": ["RMA3"]})
    assert rf.mode == "blacklist"
    assert not rf.allows("RMA3")
    assert rf.allows("RMA1")
    # A station never listed anywhere still passes — blacklist is open by design.
    assert rf.allows("RMA99")


def test_empty_whitelist_allows_nothing():
    rf = RadarStationFilter.from_settings({"whitelist": []})
    assert not rf.allows("RMA1")


def test_empty_blacklist_allows_everything():
    rf = RadarStationFilter.from_settings({"blacklist": []})
    assert rf.allows("RMA1")


def test_rejects_object_with_both_keys():
    with pytest.raises(ValueError, match="exactly one"):
        RadarStationFilter.from_settings({"whitelist": ["RMA1"], "blacklist": ["RMA2"]})


def test_rejects_object_with_neither_key():
    with pytest.raises(ValueError, match="exactly one"):
        RadarStationFilter.from_settings({})


def test_rejects_unknown_string():
    with pytest.raises(ValueError, match="radar_stations"):
        RadarStationFilter.from_settings("everything")


def test_rejects_non_list_station_value():
    with pytest.raises(ValueError, match="list of station-ID strings"):
        RadarStationFilter.from_settings({"whitelist": "RMA1"})


def test_rejects_non_string_station_entries():
    with pytest.raises(ValueError, match="list of station-ID strings"):
        RadarStationFilter.from_settings({"blacklist": ["RMA1", 3]})
