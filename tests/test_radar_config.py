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


def test_variable_is_the_uppercase_odim_token():
    # Only products whose filename token differs from their id set file_variable.
    assert RADAR_PRODUCT_CONFIGS["dbzh"].variable == "DBZH"
    assert RADAR_PRODUCT_CONFIGS["vrad"].variable == "VRAD"
    assert RADAR_PRODUCT_CONFIGS["dbzh-450km"].variable == "DBZH"


def test_variable_subvolume_pairs_are_unique():
    # get_radar_product_config_for_file resolves on this pair, so a duplicate
    # would silently route a scan to whichever product is registered first.
    pairs = [(c.variable, c.subvolume) for c in RADAR_PRODUCT_CONFIGS.values()]
    assert len(pairs) == len(set(pairs))


@pytest.mark.parametrize(
    "filename,expected",
    [
        ("RMA1_0315_01_DBZH_20260114T170000Z.H5", "dbzh"),
        ("RMA1_0315_04_DBZH_20260114T170010Z.H5", "dbzh-450km"),
        ("RMA1_0315_02_VRAD_20260114T170000Z.H5", "vrad"),
    ],
)
def test_resolves_product_from_variable_and_subvolume(filename, expected):
    parsed = parse_radar_filename(filename)
    config = get_radar_product_config_for_file(parsed["variable"], parsed["subvolume"])
    assert config.product_id == expected


def test_unknown_variable_subvolume_pair_raises():
    with pytest.raises(ValueError, match="No radar product"):
        get_radar_product_config_for_file("DBZH", "07")


def test_every_radar_product_config_declares_a_palette_or_stays_off():
    """A product reachable from settings must be renderable.

    `RADAR_PRODUCT_CONFIGS` is now the source of the settings id list, so every
    entry is a flippable toggle. `get_palette` is called once per file before
    the sweep loop, so a product without a palette raises for every file it ever
    sees. This records which products are knowingly unrenderable; adding a
    palette should shrink the set, never grow it.
    """
    from models.radar_palettes import RADAR_PALETTES

    unrenderable = sorted(
        pid
        for pid, cfg in RADAR_PRODUCT_CONFIGS.items()
        if cfg.variable not in RADAR_PALETTES
    )
    assert unrenderable == [], (
        f"radar products without a palette: {unrenderable}. Every product in "
        f"the registry is a flippable settings toggle, so one without a palette "
        f"is a switch that raises on every file it sees."
    )


def test_enabling_a_product_without_a_palette_fails_at_startup(tmp_path, monkeypatch):
    """The failure must land once at boot, not once per file forever.

    Every product currently has a palette, so the scenario is constructed by
    removing one: get_palette is called before the sweep loop, so without this
    check a palette-less product raises for every file it ever sees.
    """
    import json

    import models.radar_palettes as palettes

    monkeypatch.setitem(
        palettes.__dict__,
        "RADAR_PALETTES",
        {k: v for k, v in palettes.RADAR_PALETTES.items() if k != "DBZH"},
    )

    settings = {
        "timezone": "UTC",
        "bounds": {"minx": -90, "miny": -60, "maxx": -30, "maxy": -15},
        "sources": {"radar-sinarame": {"products": {"dbzh": True}}},
    }
    path = tmp_path / "settings.json"
    path.write_text(json.dumps(settings))
    for key, value in {
        "LOG_LEVEL": "ERROR",
        "DATA_DIR": "/tmp/t",
        "S3_TILES_DATA_ENDPOINT": "s:9000",
        "S3_TILES_DATA_TILES_PROCESSOR_USER": "u",
        "S3_TILES_DATA_TILES_PROCESSOR_PASSWORD": "p",
        "S3_TILES_DATA_BUCKET_NAME": "tiles-data",
        "RABBITMQ_HOST": "r",
        "RABBITMQ_PORT": "5672",
        "RABBITMQ_USER": "g",
        "RABBITMQ_PASSWORD": "g",
        "RABBITMQ_QUEUE": "q",
        "RABBITMQ_DLQ": "d",
        "RABBITMQ_DLX": "x",
        "JOB_TTL_MINUTES": "20",
    }.items():
        monkeypatch.setenv(key, value)

    from config import Config

    with pytest.raises(ValueError, match="no palette"):
        Config(settings_path=path)
