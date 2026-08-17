"""Fixes the GFS single-producer invariant (see models/gfs_config.py).

GFS deliberately diverges from the ECMWF template: ECMWF registers one producer
data source *per product* because each downloads a different parameter, while
the three GFS products all come from the same GRIB subset. Copy-pasting the
ECMWF shape would triple the request load against NOMADS and the cold-start
burst — a regression that is invisible in behaviour and only shows up as an
endpoint ban. These tests make it a build failure instead.
"""

import json

import pytest

from config import Config
from factories import create_data_source_registry, enabled_gfs_products
from models.gfs_config import (
    GFS_PRODUCER_DATA_SOURCE_ID,
    GFS_STEP_DATA_SOURCE_ID,
)

ENDPOINT = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"


def _config(tmp_path, monkeypatch, **flags) -> Config:
    """Config with only the GFS product flags under test switched on.

    Flags are still passed as ``enable_gfs_<product>=True`` (as the callers read
    naturally); they are translated to ``sources.gfs.products``.
    """
    monkeypatch.setenv("GFS_SUBSET_ENDPOINT", ENDPOINT)
    gfs_products = {key.removeprefix("enable_gfs_"): val for key, val in flags.items()}
    settings = {
        "timezone": "UTC",
        "bounds": {"minx": -110.0, "miny": -60.0, "maxx": -30.0, "maxy": -15.0},
        "sources": {
            "goes19": {
                "products": {"band_13": False, "band_9": False, "band_2": False}
            },
            "gfs": {"products": gfs_products},
        },
    }
    path = tmp_path / "settings.json"
    path.write_text(json.dumps(settings), encoding="utf-8")
    return Config(path)


def _gfs_source_ids(config: Config) -> list[str]:
    registry = create_data_source_registry(config)
    return [sid for sid in registry.get_all_ids() if sid.startswith("gfs")]


ALL_ON = {"enable_gfs_mslp": True, "enable_gfs_500": True, "enable_gfs_250": True}


class TestSingleProducerInvariant:
    def test_all_three_products_register_exactly_two_sources(
        self, tmp_path, monkeypatch
    ):
        """One producer + one step source, never one pair per product."""
        ids = _gfs_source_ids(_config(tmp_path, monkeypatch, **ALL_ON))
        assert sorted(ids) == sorted(
            [GFS_PRODUCER_DATA_SOURCE_ID, GFS_STEP_DATA_SOURCE_ID]
        )

    @pytest.mark.parametrize(
        "flags",
        [
            {"enable_gfs_mslp": True},
            {"enable_gfs_500": True},
            {"enable_gfs_250": True},
            {"enable_gfs_mslp": True, "enable_gfs_500": True},
            ALL_ON,
        ],
    )
    def test_source_count_is_independent_of_how_many_products_are_on(
        self, tmp_path, monkeypatch, flags
    ):
        assert len(_gfs_source_ids(_config(tmp_path, monkeypatch, **flags))) == 2

    def test_no_gfs_sources_when_every_product_is_off(self, tmp_path, monkeypatch):
        assert _gfs_source_ids(_config(tmp_path, monkeypatch)) == []


class TestEnabledProducts:
    """Feature flags select what gets rendered, not how much gets downloaded."""

    def test_returns_only_enabled_products(self, tmp_path, monkeypatch):
        config = _config(
            tmp_path, monkeypatch, enable_gfs_mslp=True, enable_gfs_250=True
        )
        assert [p.product_id for p in enabled_gfs_products(config)] == ["mslp", "250"]

    def test_returns_all_three_when_all_are_on(self, tmp_path, monkeypatch):
        config = _config(tmp_path, monkeypatch, **ALL_ON)
        assert len(enabled_gfs_products(config)) == 3

    def test_returns_nothing_when_all_are_off(self, tmp_path, monkeypatch):
        assert enabled_gfs_products(_config(tmp_path, monkeypatch)) == []

    def test_order_is_stable(self, tmp_path, monkeypatch):
        config = _config(tmp_path, monkeypatch, **ALL_ON)
        assert [p.product_id for p in enabled_gfs_products(config)] == [
            "mslp",
            "500",
            "250",
        ]


class TestEndpointWiring:
    def test_enabling_gfs_without_an_endpoint_fails_at_wiring_time(
        self, tmp_path, monkeypatch
    ):
        """Better a loud startup failure than a silent 33-downloads-a-cycle no-op."""
        monkeypatch.delenv("GFS_SUBSET_ENDPOINT", raising=False)
        settings = {
            "timezone": "UTC",
            "bounds": {"minx": -110.0, "miny": -60.0, "maxx": -30.0, "maxy": -15.0},
            "sources": {
                "goes19": {"products": {"band_13": False}},
                "gfs": {"products": {"mslp": True}},
            },
        }
        path = tmp_path / "settings.json"
        path.write_text(json.dumps(settings), encoding="utf-8")
        config = Config(path)
        with pytest.raises(ValueError, match="GFS_SUBSET_ENDPOINT"):
            create_data_source_registry(config)
