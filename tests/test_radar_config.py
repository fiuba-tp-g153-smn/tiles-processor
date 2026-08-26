"""Tests for RadarStationFilter parsing and product resolution from filenames."""

import pytest

from models.radar_config import (
    RADAR_PRODUCT_CONFIGS,
    RadarStationFilter,
    get_radar_product_config_for_file,
    parse_radar_filename,
)


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


def test_variable_defaults_to_product_id():
    # Only products whose filename token differs from their id set file_variable.
    assert RADAR_PRODUCT_CONFIGS["DBZH"].variable == "DBZH"
    assert RADAR_PRODUCT_CONFIGS["VRAD"].variable == "VRAD"
    assert RADAR_PRODUCT_CONFIGS["DBZH_450KM"].variable == "DBZH"


def test_variable_subvolume_pairs_are_unique():
    # get_radar_product_config_for_file resolves on this pair, so a duplicate
    # would silently route a scan to whichever product is registered first.
    pairs = [(c.variable, c.subvolume) for c in RADAR_PRODUCT_CONFIGS.values()]
    assert len(pairs) == len(set(pairs))


@pytest.mark.parametrize(
    "filename,expected",
    [
        ("RMA1_0315_01_DBZH_20260114T170000Z.H5", "DBZH"),
        ("RMA1_0315_04_DBZH_20260114T170010Z.H5", "DBZH_450KM"),
        ("RMA1_0315_02_VRAD_20260114T170000Z.H5", "VRAD"),
    ],
)
def test_resolves_product_from_variable_and_subvolume(filename, expected):
    parsed = parse_radar_filename(filename)
    config = get_radar_product_config_for_file(
        parsed["variable"], parsed["subvolume"]
    )
    assert config.product_id == expected


def test_unknown_variable_subvolume_pair_raises():
    with pytest.raises(ValueError, match="No radar product"):
        get_radar_product_config_for_file("DBZH", "07")
