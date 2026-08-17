"""Tests for GFS configuration parsing and product/scheduling constants."""

import json
from pathlib import Path

import pytest

from config import Config
from models.gfs_config import (
    FORECAST_HOURS,
    GFS_250_CONFIG,
    GFS_500_CONFIG,
    GFS_MSLP_CONFIG,
    GFS_PRODUCT_CONFIGS,
    SHORT_RANGE_LIMIT_HOURS,
    STEP_HOURS_LONG,
    STEP_HOURS_SHORT,
    forecast_steps,
    get_gfs_product_config_by_band,
)

PUBLIC_ENDPOINT = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
# Stands in for the SMN's internal mirror; the real hostname is deliberately not
# committed. Any second host exercises the same behaviour.
INTERNAL_ENDPOINT = "http://gfs-mirror.internal/cgi-enabled/g2subset.pl"


def _write_settings(tmp_path, *, gfs=None):
    """Minimal settings.json with an optional sources.gfs override block."""
    settings = {
        "timezone": "UTC",
        "bounds": {"minx": -110.0, "miny": -60.0, "maxx": -30.0, "maxy": -15.0},
    }
    if gfs is not None:
        settings["sources"] = {"gfs": gfs}
    path = tmp_path / "settings.json"
    path.write_text(json.dumps(settings), encoding="utf-8")
    return path


class TestForecastSteps:
    """Tests the data cadence: 3-hourly to +48h, 6-hourly after."""

    def test_produces_thirty_three_steps(self):
        assert len(forecast_steps()) == 33

    def test_covers_analysis_through_the_full_range(self):
        steps = forecast_steps()
        assert steps[0] == 0
        assert steps[-1] == FORECAST_HOURS

    def test_short_range_is_three_hourly(self):
        steps = [s for s in forecast_steps() if s <= SHORT_RANGE_LIMIT_HOURS]
        assert all(b - a == STEP_HOURS_SHORT for a, b in zip(steps, steps[1:])), steps

    def test_long_range_is_six_hourly(self):
        steps = [s for s in forecast_steps() if s >= SHORT_RANGE_LIMIT_HOURS]
        assert all(b - a == STEP_HOURS_LONG for a, b in zip(steps, steps[1:])), steps

    def test_steps_are_unique_and_sorted(self):
        steps = forecast_steps()
        assert steps == sorted(set(steps))


class TestProductConfigs:
    def test_three_products(self):
        assert set(GFS_PRODUCT_CONFIGS) == {"mslp", "500", "250"}

    def test_band_ids_are_distinct(self):
        band_ids = {cfg.band_id for cfg in GFS_PRODUCT_CONFIGS.values()}
        assert len(band_ids) == 3

    def test_upper_level_products_share_one_processor(self):
        """One parameterized processor serves 500 and 250, like radar/WRF do."""
        assert GFS_500_CONFIG.processor_id == GFS_250_CONFIG.processor_id
        assert GFS_MSLP_CONFIG.processor_id != GFS_500_CONFIG.processor_id

    def test_only_upper_level_products_carry_a_level(self):
        assert GFS_MSLP_CONFIG.level_hpa is None
        assert GFS_500_CONFIG.level_hpa == 500
        assert GFS_250_CONFIG.level_hpa == 250

    def test_lookup_by_band_id(self):
        assert get_gfs_product_config_by_band("gfs_500") is GFS_500_CONFIG

    def test_unknown_band_id_raises(self):
        with pytest.raises(ValueError, match="Unknown GFS band_id"):
            get_gfs_product_config_by_band("gfs_850")

    def test_prefixes_do_not_collide(self):
        prefixes = [
            p
            for cfg in GFS_PRODUCT_CONFIGS.values()
            for p in (cfg.cog_prefix, cfg.tiles_prefix, cfg.geojson_prefix)
        ]
        assert len(prefixes) == len(set(prefixes))


class TestGfsAccessParsing:
    def test_endpoint_comes_from_the_environment(self, tmp_path, monkeypatch):
        """The single knob that flips a deploy between networks."""
        monkeypatch.setenv("GFS_SUBSET_ENDPOINT", INTERNAL_ENDPOINT)
        assert (
            Config(_write_settings(tmp_path)).GFS_ACCESS.subset_endpoint
            == INTERNAL_ENDPOINT
        )

    def test_settings_json_cannot_supply_the_endpoint(self, tmp_path, monkeypatch):
        """Deliberate: it is a per-environment value, and settings.json is baked
        into the image. A stray key there must not silently take effect."""
        monkeypatch.delenv("GFS_SUBSET_ENDPOINT", raising=False)
        path = _write_settings(tmp_path, gfs={"subset_endpoint": PUBLIC_ENDPOINT})
        assert not Config(path).GFS_ACCESS.is_configured

    def test_an_empty_env_var_counts_as_unset(self, tmp_path, monkeypatch):
        """Compose passes through empty strings when the .env key is blank."""
        monkeypatch.setenv("GFS_SUBSET_ENDPOINT", "")
        assert not Config(_write_settings(tmp_path)).GFS_ACCESS.is_configured

    def test_missing_gfs_block_is_tolerated(self, tmp_path, monkeypatch):
        """Settings with no GFS section must still build a usable Config."""
        monkeypatch.delenv("GFS_SUBSET_ENDPOINT", raising=False)
        config = Config(_write_settings(tmp_path))
        assert not config.GFS_ACCESS.is_configured

    def test_concurrency_from_settings(self, tmp_path, monkeypatch):
        monkeypatch.setenv("GFS_SUBSET_ENDPOINT", PUBLIC_ENDPOINT)
        path = _write_settings(tmp_path, gfs={"max_concurrent_downloads": 8})
        assert Config(path).GFS_ACCESS.max_concurrent_downloads == 8

    def test_steps_per_tick_from_settings(self, tmp_path, monkeypatch):
        monkeypatch.setenv("GFS_SUBSET_ENDPOINT", PUBLIC_ENDPOINT)
        path = _write_settings(tmp_path, gfs={"max_steps_per_tick": 4})
        assert Config(path).GFS_MAX_STEPS_PER_TICK == 4

    @pytest.mark.parametrize(
        "env_var",
        [
            "GFS_MAX_CONCURRENT_DOWNLOADS",
            "GFS_MAX_STEPS_PER_TICK",
            "GFS_HTTP_TIMEOUT_SECONDS",
            "GFS_SMOOTHING_SIGMA",
        ],
    )
    def test_settings_json_knobs_ignore_stray_env_vars(
        self, tmp_path, monkeypatch, env_var
    ):
        """Every knob has exactly one home, so no value can be set in two places.

        These four live in settings.json; the two that live in the environment
        (`GFS_SUBSET_ENDPOINT` and `GFS_TILE_SMOOTHING_RESOLUTION_DEG`) are
        covered separately below. A knob readable from both would mean the
        effective value depends on which file you happen to be looking at.
        """
        monkeypatch.setenv("GFS_SUBSET_ENDPOINT", PUBLIC_ENDPOINT)
        monkeypatch.setenv(env_var, "999")
        config = Config(
            _write_settings(
                tmp_path,
                gfs={"max_concurrent_downloads": 8, "max_steps_per_tick": 4},
            )
        )
        assert config.GFS_ACCESS.max_concurrent_downloads == 8
        assert config.GFS_MAX_STEPS_PER_TICK == 4
        assert config.GFS_ACCESS.timeout_seconds == 120
        assert config.GFS_SMOOTHING_SIGMA == 1.5

    def test_tile_smoothing_resolution_comes_from_the_environment(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setenv("GFS_SUBSET_ENDPOINT", PUBLIC_ENDPOINT)
        monkeypatch.setenv("GFS_TILE_SMOOTHING_RESOLUTION_DEG", "0.05")
        config = Config(_write_settings(tmp_path))
        assert config.GFS_TILE_SMOOTHING_RESOLUTION_DEG == 0.05

    def test_settings_json_cannot_supply_the_tile_smoothing_resolution(
        self, tmp_path, monkeypatch
    ):
        """The one thing this knob has to be is retunable without a rebuild.

        settings.json is baked into the image on the `make prod` path, so a
        value read from there would be exactly what an operator cannot change
        when tiling time or worker memory spikes. A key in settings.json must be
        ignored rather than silently honoured.
        """
        monkeypatch.setenv("GFS_SUBSET_ENDPOINT", PUBLIC_ENDPOINT)
        monkeypatch.delenv("GFS_TILE_SMOOTHING_RESOLUTION_DEG", raising=False)
        config = Config(
            _write_settings(tmp_path, gfs={"tile_smoothing_resolution_deg": 0.5})
        )
        assert config.GFS_TILE_SMOOTHING_RESOLUTION_DEG == 0.01

    def test_an_empty_tile_smoothing_env_var_falls_back_to_the_default(
        self, tmp_path, monkeypatch
    ):
        """docker-compose forwards an unset variable as "", which `os.getenv`'s
        own default would not catch — hence the `or` idiom used across Config."""
        monkeypatch.setenv("GFS_SUBSET_ENDPOINT", PUBLIC_ENDPOINT)
        monkeypatch.setenv("GFS_TILE_SMOOTHING_RESOLUTION_DEG", "")
        config = Config(_write_settings(tmp_path))
        assert config.GFS_TILE_SMOOTHING_RESOLUTION_DEG == 0.01

    def test_gfs_uses_the_global_bounds(self, tmp_path, monkeypatch):
        """GFS deliberately has no bounds of its own: same domain as every layer."""
        monkeypatch.delenv("GFS_SUBSET_ENDPOINT", raising=False)
        config = Config(_write_settings(tmp_path))
        assert config.get_bounds() == {
            "minx": -110.0,
            "miny": -60.0,
            "maxx": -30.0,
            "maxy": -15.0,
        }
        assert not hasattr(config, "GFS_BOUNDS")


class TestGfsSettingsFromRepoFile:
    """The checked-in settings.json must carry a coherent GFS block.

    Two knobs are deliberately absent from it and live in `.env` instead: the
    endpoint (per-environment, see `_parse_gfs_access`) and the tile smoothing
    resolution (must be retunable without rebuilding the image).
    """

    def test_repo_settings_do_not_pin_the_tile_smoothing_resolution(self):
        """A key here would read as tunable and do nothing — worse than absent."""
        repo_settings = Path(__file__).resolve().parent.parent / "settings.json"
        settings = json.loads(repo_settings.read_text(encoding="utf-8"))
        gfs = settings.get("sources", {}).get("gfs", {})
        assert "tile_smoothing_resolution_deg" not in gfs

    def test_repo_settings_do_not_pin_an_endpoint(self, monkeypatch):
        """settings.json is baked into the image; the endpoint is per-environment."""
        monkeypatch.delenv("GFS_SUBSET_ENDPOINT", raising=False)
        assert not Config().GFS_ACCESS.is_configured

    def test_repo_settings_configure_gfs(self, monkeypatch):
        monkeypatch.setenv("GFS_SUBSET_ENDPOINT", PUBLIC_ENDPOINT)
        config = Config()
        assert config.GFS_ACCESS.is_configured
        assert config.GFS_CYCLES_TO_MAINTAIN >= 1
        assert config.GFS_MAX_STEPS_PER_TICK >= 1
        assert (
            config.GFS_AVAILABILITY_PROBE_FROM_HOURS
            < config.GFS_AVAILABILITY_PROBE_TO_HOURS
        )
